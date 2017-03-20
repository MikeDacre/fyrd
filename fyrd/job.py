# -*- coding: utf-8 -*-
"""
Class and methods to handle Job submission.
"""
import os  as _os
import sys as _sys
from uuid import uuid4 as _uuid
from time import sleep as _sleep
from datetime import datetime as _dt
from subprocess import CalledProcessError as _CalledProcessError

import dill as _pickle
from six import reraise as _reraise

###############################################################################
#                                Our functions                                #
###############################################################################

from . import run     as _run
from . import conf    as _conf
from . import queue   as _queue
from . import logme   as _logme
from . import local   as _local
from . import batch   as _batch
from . import options as _options
from . import script_runners as _scrpts
from . import ClusterError   as _ClusterError
from .submission_scripts import Script   as _Script
from .submission_scripts import Function as _Function


__all__ = ['Job']

###############################################################################
#                                The Job Class                                #
###############################################################################


class Job(object):

    """Information about a single job on the cluster.

    Holds information about submit time, number of cores, the job script,
    and more.

    Below are the core attributes and methods required to use this class.

    Attributes:
        out (str):            The output of the function or a copy of stdout
                              for a script
        stdout (str):         Any output to STDOUT
        stderr (str):         Any output to STDERR
        exitcode (int):       The exitcode of the running processes (the script
                              runner if the Job is a function.
        start (datetime):     A datetime object containing time execution
                              started on the remote node.
        end (datetime):       Like start but when execution ended.
        runtime (timedelta):  A timedelta object containing runtime.
        files (list):         A list of script files associated with this class
        done (bool):          True if the job has completed

    Methods:
        submit(): submit the job if it is ready
        wait():   block until the job is done
        get():    block until the job is done and then return the output
                  (stdout if job is a script), by default saves all outputs to
                  self (i.e. .out, .stdout, .stderr) and deletes all
                  intermediate files before returning. If `save` argument is
                  `False`, does not delete the output files by default.
        clean():  delete any files created by this object

    Printing or reproducing the class will display detailed job information.

    Both `wait()` and `get()` will update the queue every few seconds
    (defined by the queue_update item in the config) and add queue information
    to the job as they go.

    If the job disappears from the queue with no information, it will be listed
    as 'completed'.

    All jobs have a .submission attribute, which is a Script object containing
    the submission script for the job and the file name, plus a 'written' bool
    that checks if the file exists.

    In addition, SLURM jobs have a .exec_script attribute, which is a Script
    object containing the shell command to _run. This difference is due to the
    fact that some SLURM systems execute multiple lines of the submission file
    at the same time.

    Finally, if the job command is a function, this object will also contain a
    `.function` attribute, which contains the script to run the function.

    """

    id            = None
    submitted     = False
    written       = False

    # Holds a pool object if we are in local mode
    pool_job      = None

    # Scripts
    script_files  = None
    submission    = None
    function      = None
    imports       = None
    syspaths      = None

    # Dependencies
    dependencies  = None

    # Pickled output file for functions
    poutfile      = None

    # Holds queue information in torque and slurm
    queue_info    = None

    # Output tracking
    _got_out      = False
    _got_stdout   = False
    _got_stderr   = False
    _got_exitcode = False
    _out          = None
    _stdout       = None
    _stderr       = None
    _exitcode     = None

    # Time tracking
    _got_times    = False
    start         = None
    end           = None

    # Track update status
    _updating     = False

    # Auto Cleaning
    clean_files   = _conf.get_option('jobs', 'clean_files')
    clean_outputs = _conf.get_option('jobs', 'clean_outputs')

    def __init__(self, command, args=None, kwargs=None, name=None, qtype=None,
                 profile=None, **kwds):
        """Initialization function arguments.

        Args:
            command (function/str): The command or function to execute.
            args (tuple/dict):      Optional arguments to add to command,
                                    particularly useful for functions.
            kwargs (dict):          Optional keyword arguments to pass to the
                                    command, only used for functions.
            name (str):             Optional name of the job. If not defined,
                                    guessed. If a job of the same name is
                                    already queued, an integer job number (not
                                    the queue number) will be added, ie.
                                    <name>.1
            qtype (str):            Override the default queue type
            profile (str):          The name of a profile saved in the
                                    conf

            *All other keywords are parsed into cluster keywords by the
            options system. For available keywords see `fyrd.option_help()`*
        """

        ########################
        #  Sanitize arguments  #
        ########################

        _logme.log('Args pre-check: {}'.format(kwds), 'debug')
        kwds = _options.check_arguments(kwds)
        _logme.log('Args post-check: {}'.format(kwds), 'debug')

        # Merge in profile, this includes all args from the DEFAULT profile
        # as well, ensuring that those are always set at a minumum.
        profile = profile if profile else 'DEFAULT'
        prof = _conf.get_profile(profile)
        if not prof:
            raise _ClusterError('No profile found for {}'.format(profile))
        for k,v in prof.args.items():
            if k not in kwds:
                kwds[k] = v

        # Use the default profile as a backup if any arguments missing
        default_args = _conf.DEFAULT_PROFILES['DEFAULT']
        default_args.update(_conf.get_profile('DEFAULT').args)
        for opt, arg in default_args.items():
            if opt not in kwds:
                _logme.log('{} not in kwds, adding from default: {}:{}'
                           .format(opt, opt, arg), 'debug')
                kwds[opt] = arg

        # In case cores are passed as None
        if not kwds['nodes']:
            kwds['nodes'] = default_args['nodes']
        if not kwds['cores']:
            kwds['cores'] = default_args['cores']
        self.nodes = kwds['nodes']
        self.cores = kwds['cores']

        # Make sure args are a tuple or dictionary
        kwargs = kwargs if kwargs else {}
        if args:
            if isinstance(args, dict):
                kwargs.update(args)
                args = None
            else:
                args = tuple(_run.listify(args))

        # Determine if we are a function or a script
        self.kind = 'function' if callable(command) else 'script'

        # Save command
        self.command = command
        self.args    = args
        self.kwargs  = kwargs

        ############################
        #  Name and Path Handling  #
        ############################

        # Set name
        if not name:
            if self.kind == 'function':
                strcmd = str(command).strip('<>')
                parts = strcmd.split(' ')
                if parts[0] == 'bound':
                    name = '_'.join(parts[2:3])
                else:
                    parts.remove('function')
                    try:
                        parts.remove('built-in')
                    except ValueError:
                        pass
                    name = parts[0]
            else:
                name = command.split(' ')[0].split('/')[-1]

        # Make name unique
        self.uuid = str(_uuid()).split('-')[0]
        self.name = '{}.{}'.format(self.name, self.uuid)

        # Path handling
        [kwds,
         self.runpath,
         self.outpath,
         self.scriptpath
        ] = _conf.get_job_paths(kwds)

        # Set output file names
        if 'suffix' in kwds:
            self.suffix = kwds.pop('suffix')
        else:
            self.suffix = _conf.get_option('jobs', 'suffix')

        prefix = '{}.{}'.format(self.name, self.suffix)

        self.outfile = _run.get_path(
            'outfile', '{}.{}'.format(prefix, 'out'), self.outpath, kwds
        )
        self.errfile = _run.get_path(
            'errfile', '{}.{}'.format(prefix, 'err'), self.outpath, kwds
        )
        kwds['outfile'] = self.outfile
        kwds['errfile'] = self.errfile

        if self.kind == 'function':
            self.poutfile = self.outfile + '.func.pickle'

        #################################
        #  Non-script keyword recovery  #
        #################################

        # Get imports
        self.imports = kwds.pop('imports') if 'imports' in kwds else None

        # Get syspaths
        self.syspaths = kwds.pop('syspaths') if 'syspaths' in kwds else None

        # Override autoclean state (set in config file)
        if 'clean_files' in kwds:
            self.clean_files = kwds.pop('clean_files')
        if 'clean_outputs' in kwds:
            self.clean_outputs = kwds.pop('clean_outputs')

        # Set modules
        self.modules = kwds.pop('modules') if 'modules' in kwds else None
        if self.modules:
            self.modules = _run.opt_split(self.modules, (',', ';'))

        #################################
        #  Set Initial Dependency List  #
        #################################

        if 'depends' in kwds:
            dependencies = _run.listify(kwds.pop('depends'))
            self.dependencies = []
            for dependency in dependencies:
                if isinstance(dependency, str) and dependency.isdigit():
                    dependency  = int(dependency)
                if not isinstance(dependency, (int, str, Job)):
                    raise _ClusterError(
                        'Dependencies must be an int, str, or Job'
                    )
                self.dependencies.append(dependency)

        # Save submittable keywords to self
        self.kwds = kwds

        #########################################
        #  Get Batch Environment and Set State  #
        #########################################

        if not _queue.MODE:
            _queue.MODE = _queue.get_cluster_environment()
        self.qtype = qtype if qtype else _queue.MODE
        self.queue = _queue.Queue(user='self', qtype=self.qtype)
        self.state = 'Not_Submitted'

        # Initialize the first build
        self.build_script()


    ####################
    #  Public Methods  #
    ####################

    def build_script(self):
        """Create the script used for submission of this job."""
        # Detect our batch environment
        self.batch = _batch.get_batch(self.qtype)

        # Reset script files
        self.script_files = []

        # Split out sys.paths from imports and set imports in self
        if self.imports:
            imports = []
            syspaths = self.syspaths if self.syspaths else []
            for impt in self.imports:
                if impt.startswith('sys.path.append')\
                        or impt.startswith('sys.path.insert'):
                    syspaths.append(impt)
                else:
                    imports.append(impt)
            self.imports  = imports
            self.syspaths = syspaths

        # Function specific initialization
        if self.kind == 'function':
            func_script_file = _os.path.join(
                self.scriptpath,
                '{}_func.{}.py'.format(self.name, self.suffix)
            )
            self.function = _Function(
                file_name=func_script_file,
                function=self.command,
                args=self.args,
                kwargs=self.kwargs,
                imports=self.imports,
                syspaths=syspaths,
                outfile=self.poutfile
            )
            self.script_files.append(self.function)

            # Collapse the _command into a python call to the function script
            if _conf.get_option('jobs', 'generic_python'):
                executable = '/usr/bin/env python{}'.format(
                    _sys.version_info.major
                )
            else:
                executable = _sys.executable

            command = '{} {}'.format(executable, self.function.file_name)
            args = None
        else:
            command = self.command
            args    = self.args

        # Collapse args into command
        command = command + ' '.join(args) if args else command

        # Get batch specific script
        script = self.batch.format_script(self.kwds)

        # Add modules if necessary
        modules  = ''
        if self.modules:
            for module in self.modules:
                modules += 'module load {}\n'.format(module)

        if '{modules}' not in script:
            script += '\n{modules}'
        script = script.format(modules=modules)

        # Create queue-dependent scripts
        script_file = _os.path.join(
            self.scriptpath,
            '{}.{}.{}'.format(self.name, self.suffix, self.batch.suffix)
        )

        # Create the submission Script object
        self.submission = _Script(
            script = _scrpts.CMND_RUNNER_TRACK.format(
                precmd=script,
                usedir=self.runpath,
                name=self.name,
                command=command
            ),
            file_name=script_file
        )
        self.script_files.append(self.submission)

        return self.update()

    def write(self, overwrite=True):
        """Write all scripts.

        Args:
            overwrite (bool): Overwrite existing files, defaults to True.
        """
        _logme.log('Writing files, overwrite={}'.format(overwrite), 'debug')
        for script_file in self.script_files:
            script_file.write(overwrite)
        self.written = True
        return self.update()

    def clean(self, delete_outputs=None, get_outputs=True):
        """Delete all scripts created by this module, if they were written.

        Args:
            delete_outputs (bool): also delete all output and err files,
                                   but get their contents first.
            get_outputs (bool):    if delete_outputs, save outputs before
                                   deleting.
        """
        _logme.log('Cleaning outputs, delete_outputs={}'
                   .format(delete_outputs), 'debug')
        # Sanitize arguments
        if delete_outputs is None:
            delete_outputs = self.clean_outputs
        assert isinstance(delete_outputs, bool)
        for script_file in self.script_files:
            script_file.clean()
        if delete_outputs:
            _logme.log('Deleting output files.', 'debug')
            if get_outputs:
                self.fetch_outputs(delete_files=True)
            for f in self.outfiles:
                if _os.path.isfile(f):
                    _logme.log('Deleteing {}'.format(f), 'debug')
                    _os.remove(f)
        self.written = False
        return self.update()

    def submit(self, wait_on_max_queue=True):
        """Submit this job.

        Args:
            wait_on_max_queue (bool): Block until queue limit is below the
                                      maximum before submitting.

        To disable max_queue_len, set it to 0. None will allow override by
        the default settings in the config file, and any positive integer will
        be interpretted to be the maximum queue length.

        Returns:
            self
        """
        if self.submitted:
            _logme.log('Not submitting, already submitted.', 'warn')
            return self

        if not self.written:
            self.write()

        # Check dependencies
        dependencies = []
        if self.dependencies:
            for depend in self.dependencies:
                if isinstance(depend, Job):
                    if not depend.id:
                        _logme.log(
                            'Cannot submit job as dependency {} '
                            .format(depend) + 'has not been submitted',
                            'error'
                        )
                        return self
                    dependencies.append(int(depend.id))
                else:
                    dependencies.append(int(depend))

        # Wait on the queue if necessary
        if wait_on_max_queue:
            self.update()
            self.queue.wait_to_submit()

        command = self.batch.submit_cmnd
        args    = self.batch.submit_args(kwds=self.kwds,
                                         dependencies=dependencies)
        command = '{} {}'.format(command, args)

        if self.qtype == 'local':
            # Normal mode dependency tracking uses only integer job numbers
            _logme.log('Submitting to local', 'debug')
            command = 'bash {}'.format(self.submission.file_name)
            fileargs  = dict(stdout=self.outfile,
                             stderr=self.errfile)

            # Make sure the global job pool exists
            if not _local.JQUEUE or not _local.JQUEUE.runner.is_alive():
                _local.JQUEUE = _local.JobQueue(cores=_local.THREADS)
            self.id = _local.JQUEUE.add(_run.cmd, args=(command,),
                                        kwargs=fileargs,
                                        dependencies=depends,
                                        cores=self.cores)

        else:
            _logme.log('Submitting to {}'.format(self.qtype), 'debug')

            # Try to submit job 5 times
            code, stdout, stderr = _run.cmd(args, tries=5)
            if code == 0:
                self.id = self.batch.id_from_stdout(stdout)
            else:
                _logme.log(
                    '{} failed with code {}\nstdout: {}\nstderr: {}'
                    .format(command, code, stdout, stderr), 'critical'
                )
                raise _CalledProcessError(code, command, stdout, stderr)

        self.submitted = True
        self.state = 'submitted'

        return self.update()

    def resubmit(self):
        """Attempt to auto resubmit, deletes prior files."""
        self.clean(delete_outputs=True)
        self.submitted = False
        self.state     = 'Not_Submitted'
        self.write()
        return self.submit()

    def wait(self):
        """Block until job completes.

        Returns:
            bool: True on success, False on failure
        """
        if not self.submitted:
            if _conf.get_option('jobs', 'auto_submit'):
                _logme.log('Auto-submitting as not submitted yet', 'debug')
                self.submit()
                _sleep(0.5)
            else:
                _logme.log('Cannot wait for result as job has not been ' +
                           'submitted', 'warn')
                return False
        _sleep(0.1)
        self.update(False)
        if not self.done:
            _logme.log('Waiting for self {}'.format(self.name), 'debug')
            if self.queue.wait(self) is not True:
                return False
            self.update()
        if self.get_exitcode(update=False) != 0:
            _logme.log('Job failed with exitcode {}'.format(self.exitcode),
                       'debug')
            return False
        if self.wait_for_files(caution_message=False):
            self.update()
            return True
        else:
            return False

    def wait_for_files(self, btme=None, caution_message=False):
        """Block until files appear up to 'file_block_time' in config file.

        Aborts after 2 seconds if job exit code is not 0.

        Args:
            btme (int):             Number of seconds to try for before giving
                                    up, default set in config file.
            caution_message (bool): Display a message if this is taking
                                    a while.

        Returns:
            bool: True if files found
        """
        if not self.done:
            _logme.log("Cannot wait for files if we aren't complete",
                       'warn')
            return False
        wait_time = 0.1 # seconds
        if btme:
            lvl = 'debug'
        else:
            lvl = 'warn'
            btme = _conf.get_option('jobs', 'file_block_time')
        start = _dt.now()
        dsp   = False
        _logme.log('Checking for output files', 'debug')
        while True:
            runtime = (_dt.now() - start).seconds
            if caution_message and runtime > 1:
                _logme.log('Job complete.', 'info')
                _logme.log('Waiting for output files to appear.', 'info')
                caution_message = False
            if not dsp and runtime > 20:
                _logme.log('Still waiting for output files to appear',
                           'info')
                dsp = True
            count = 0
            outfiles = self.incomplete_outfiles
            tlen = len(outfiles)
            if not outfiles:
                _logme.log('No incomplete outfiles, assuming all found in ' +
                           '{} seconds'.format(runtime), 'debug')
                break
            for i in outfiles:
                if _os.path.isfile(i):
                    count += 1
            if count == tlen:
                _logme.log('All output files found in {} seconds'
                           .format(runtime), 'debug')
                break
            _sleep(wait_time)
            if runtime > btme:
                _logme.log('Job completed but files have not appeared for ' +
                           '>{} seconds'.format(btme), lvl)
                return False
            self.update()
            if runtime > 2 and self.get_exitcode(update=False) != 0:
                _logme.log('Job failed with exit code {}.'
                           .format(self.exitcode) + ' Cannot find files.',
                           'error')
                return False
            if _queue.MODE == 'local':
                _logme.log('Job output files were not found.', 'error')
                _logme.log('Expected files: {}'.format(self.outfiles))
                return False
        return True

    def get(self, save=True, cleanup=None, delete_outfiles=None,
            del_no_save=None, raise_on_error=True):
        """Block until job completed and return output of script/function.

        By default saves all outputs to this class and deletes all intermediate
        files.

        Args:
            save (bool):            Save all outputs to the class also (advised)
            cleanup (bool):         Clean all intermediate files after job
                                    completes.
            delete_outfiles (bool): Clean output files after job completes.
            del_no_save (bool):     Delete output files even if `save` is
                                    `False`
            raise_on_error (bool): If the returned output is an Exception,
                                   raise it.

        Returns:
            str: Function output if Function, else STDOUT
        """
        _logme.log(('Getting outputs, cleanup={}, autoclean={}, '
                    'delete_outfiles={}').format(
                        cleanup, self.clean_files, delete_outfiles
                    ), 'debug')
        # Wait for queue
        if self.wait() is not True:
            _logme.log('Wait failed, cannot get outputs, aborting', 'error')
            self.update()
            if _os.path.isfile(self.errfile):
                if _logme.MIN_LEVEL in ['debug', 'verbose']:
                    _sys.stderr.write('STDERR of Job:\n')
                    _sys.stderr.write(self.get_stderr(delete_file=False,
                                                      update=False))
            if self.poutfile and _os.path.isfile(self.poutfile):
                _logme.log('Getting pickled output', 'debug')
                self.get_output(delete_file=False, update=False,
                                raise_on_error=raise_on_error)
            else:
                _logme.log('Pickled out file does not exist, cannot get error',
                           'debug')
            return
        # Get output
        _logme.log('Wait complete, fetching outputs', 'debug')
        self.fetch_outputs(save=save, delete_files=False)
        out = self.out if save else self.get_output(save=save)
        # Cleanup
        if cleanup is None:
            cleanup = self.clean_files
        else:
            assert isinstance(cleanup, bool)
        if delete_outfiles is None:
            delete_outfiles = self.clean_outputs
        if save is False:
            delete_outfiles = del_no_save if del_no_save is not None else False
        if cleanup:
            self.clean(delete_outputs=delete_outfiles)
        return out

    def get_output(self, save=True, delete_file=None, update=True,
                   raise_on_error=True):
        """Get output of function or script.

        This is the same as stdout for a script, or the function output for
        a function.

        By default, output file is kept unless delete_file is True or
        self.clean_files is True.

        Args:
            save (bool):           Save the output to self.out, default True.
                                   Would be a good idea to set to False if the
                                   output is huge.
            delete_file (bool):    Delete the output file when getting
            update (bool):         Update job info from queue first.
            raise_on_error (bool): If the returned output is an Exception,
                                   raise it.

        Returns:
            The output of the script or function. Always a string if script.
        """
        _logme.log(('Getting output, save={}, clean_files={}, '
                    'delete_file={}').format(
                        save, self.clean_files, delete_file
                    ), 'debug')
        if delete_file is None:
            delete_file = self.clean_outputs
        if self.kind == 'script':
            return self.get_stdout(save=save, delete_file=delete_file,
                                   update=update)
        if self.done and self._got_out:
            _logme.log('Getting output from _out', 'debug')
            return self._out
        if update and not self._updating and not self.done:
            self.update()
        if self.done:
            if update:
                self.wait_for_files()
        else:
            _logme.log('Cannot get pickled output before job completes',
                       'warn')
            return None
        _logme.log('Getting output from {}'.format(self.poutfile), 'debug')
        if _os.path.isfile(self.poutfile):
            with open(self.poutfile, 'rb') as fin:
                out = _pickle.load(fin)
            if delete_file is True or self.clean_files is True:
                _logme.log('Deleting {}'.format(self.poutfile),
                           'debug')
                _os.remove(self.poutfile)
            if save:
                self._out = out
                self._got_out = True
            if _run.is_exc(out):
                _logme.log('{} failed with exception {}'.format(self, out[1]),
                           'error')
                if raise_on_error:
                    _reraise(*out)
            return out
        else:
            _logme.log('No file at {} even though job has completed!'
                       .format(self.poutfile), 'critical')
            raise IOError('File not found: {}'.format(self.poutfile))

    def get_stdout(self, save=True, delete_file=None, update=True):
        """Get stdout of function or script, same for both.

        By default, output file is kept unless delete_file is True or
        self.clean_files is True.

        Args:
            save (bool):        Save the output to self.stdout, default True.
                                Would be a good idea to set to False if the
                                output is huge.
            delete_file (bool): Delete the stdout file when getting
            update (bool):      Update job info from queue first.

        Returns:
            str: The contents of STDOUT, with runtime info and trailing
                 newline removed.

        Also sets self.start and self.end from the contents of STDOUT if
        possible.
        """
        if delete_file is None:
            delete_file = self.clean_outputs
        _logme.log(('Getting stdout, save={}, clean_files={}, '
                    'delete_file={}').format(
                        save, self.clean_files, delete_file
                    ), 'debug')
        if self.done and self._got_stdout:
            _logme.log('Getting stdout from _stdout', 'debug')
            return self._stdout
        if update and not self._updating and not self.done:
            self.update()
        if self.done:
            if update:
                self.wait_for_files()
        else:
            _logme.log('Job not done, attempting to get current STDOUT ' +
                       'anyway', 'info')
        _logme.log('Getting stdout from {}'.format(self.kwargs['outfile']),
                   'debug')
        if _os.path.isfile(self.kwargs['outfile']):
            self.get_times(update=False)
            stdout = open(self.kwargs['outfile']).read()
            if stdout:
                stdouts = stdout.split('\n')
                stdout     = '\n'.join(stdouts[2:-3]) + '\n'
            if delete_file is True or self.clean_files is True:
                _logme.log('Deleting {}'.format(self.kwargs['outfile']),
                           'debug')
                _os.remove(self.kwargs['outfile'])
            if save:
                self._stdout = stdout
                if self.done:
                    self._got_stdout = True
            return stdout
        else:
            _logme.log('No file at {}, cannot get stdout'
                       .format(self.kwargs['outfile']), 'warn')
            return None

    def get_stderr(self, save=True, delete_file=None, update=True):
        """Get stderr of function or script, same for both.

        By default, output file is kept unless delete_file is True or
        self.clean_files is True.

        Args:
            save (bool):        Save the output to self.stdout, default True.
                                Would be a good idea to set to False if the
                                output is huge.
            delete_file (bool): Delete the stdout file when getting
            update (bool):      Update job info from queue first.

        Returns:
            str: The contents of STDERR, with trailing newline removed.
        """
        if delete_file is None:
            delete_file = self.clean_outputs
        _logme.log(('Getting stderr, save={}, clean_files={}, '
                    'delete_file={}').format(
                        save, self.clean_files, delete_file
                    ), 'debug')
        if self.done and self._got_stderr:
            _logme.log('Getting stderr from _stderr', 'debug')
            return self._stderr
        if update and not self._updating and not self.done:
            self.update()
        if self.done:
            if update:
                self.wait_for_files()
        else:
            _logme.log('Job not done, attempting to get current STDERR ' +
                       'anyway', 'info')
        _logme.log('Getting stderr from {}'.format(self.kwargs['errfile']),
                   'debug')
        if _os.path.isfile(self.kwargs['errfile']):
            stderr = open(self.kwargs['errfile']).read()
            if delete_file is True or self.clean_files is True:
                _logme.log('Deleting {}'.format(self.kwargs['errfile']),
                           'debug')
                _os.remove(self.kwargs['errfile'])
            if save:
                self._stderr = stderr
                if self.done:
                    self._got_stderr = True
            return stderr
        else:
            _logme.log('No file at {}, cannot get stderr'
                       .format(self.kwargs['errfile']), 'warn')
            return None

    def get_times(self, update=True):
        """Get stdout of function or script, same for both.

        Args:
            update (bool): Update job info from queue first.

        Returns:
            tuple: start, end as two datetime objects.

        Also sets self.start and self.end from the contents of STDOUT if
        possible.
        """
        _logme.log('Getting times', 'debug')
        if self.done and self._got_times:
            _logme.log('Getting times from self.start, self.end', 'debug')
            return self.start, self.end
        if update and not self._updating and not self.done:
            self.update()
        if self.done:
            if update:
                self.wait_for_files()
        else:
            _logme.log('Cannot get times until job is complete.', 'warn')
            return None, None
        _logme.log('Getting times from {}'.format(self.kwargs['outfile']),
                   'debug')
        if _os.path.isfile(self.kwargs['outfile']):
            stdout = open(self.kwargs['outfile']).read()
            if stdout:
                stdouts = stdout.split('\n')
                # Get times
                timefmt = '%y-%m-%d-%H:%M:%S'
                try:
                    self.start = _dt.strptime(stdouts[0], timefmt)
                    self.end   = _dt.strptime(stdouts[-2], timefmt)
                except ValueError as err:
                    _logme.log('Time parsing failed with value error; ' +
                               '{}. '.format(err) + 'This may be because you ' +
                               'are using the script running that does not ' +
                               'include time tracking', 'debug')
            self._got_times = True
            return self.start, self.end
        else:
            _logme.log('No file at {}, cannot get times'
                       .format(self.kwargs['outfile']), 'warn')
            return None

    def get_exitcode(self, update=True):
        """Try to get the exitcode.

        Args:
            update (bool): Update job info from queue first.

        Returns:
            int: The exitcode of the running process.
        """
        _logme.log('Getting exitcode', 'debug')
        if self.done and self._got_exitcode:
            _logme.log('Getting exitcode from _exitcode', 'debug')
            return self._exitcode
        if update and not self._updating and not self.done:
            self.update()
        if self.done:
            if update:
                self.wait_for_files()
        else:
            _logme.log('Job is not complete, no exit code yet', 'info')
            return None
        _logme.log('Getting exitcode from queue', 'debug')
        if not self.queue_info:
            self.queue_info = self.queue[self.id]
        if hasattr(self.queue_info, 'exitcode'):
            code = self.queue_info.exitcode
        else:
            code = None
            _logme.log('No exitcode even though the job is done, this ' +
                       "shouldn't happen.", 'warn')
        self._exitcode = code
        self._got_exitcode = True
        if code != 0:
            self.state = 'failed'
            _logme.log('Job {} failed with exitcode {}'
                       .format(self.name, code), 'error')
        return code

    def fetch_outputs(self, save=True, delete_files=None):
        """Save all outputs in their current state. No return value.

        This method does not wait for job completion, but merely gets the
        outputs. To wait for job completion, use `get()` instead.

        Args:
            save (bool):         Save all outputs to the class also (advised)
            delete_files (bool): Delete the output files when getting, only
                                 used if save is True
        """
        _logme.log('Saving outputs to self, delete_files={}'
                   .format(delete_files), 'debug')
        self.update()
        if delete_files is None:
            delete_files = self.clean_outputs
        if not self._got_exitcode:
            self.get_exitcode(update=False)
        if not self._got_times:
            self.get_times(update=False)
        if save:
            self.get_output(save=True, delete_file=delete_files, update=False)
            self.get_stdout(save=True, delete_file=delete_files, update=False)
            self.get_stderr(save=True, delete_file=delete_files, update=False)

    def update(self, fetch_info=True):
        """Update status from the queue.

        Args:
            fetch_info (bool): Fetch basic job info if complete.
        """
        if not self._updating:
            return self._update(fetch_info)
        else:
            _logme.log('Already updating, aborting.', 'debug')
            return self

    ######################
    #  Property Methods  #
    ######################

    @property
    def outfiles(self):
        """A list of all outfiles associated with this Job."""
        outfiles = [self.outfile, self.errfile]
        if self.poutfile:
            outfiles.append(self.poutfile)
        return outfiles

    @property
    def incomplete_outfiles(self):
        """A list of all outfiles that haven't already been fetched."""
        outfiles = []
        if self.outfile and not self._got_stdout:
            outfiles.append(self.outfile)
        if self.errfile and not self._got_stderr:
            outfiles.append(self.errfile)
        if self.poutfile and not self._got_out:
            outfiles.append(self.poutfile)
        return outfiles

    @property
    def files(self):
        """Build a list of files associated with this class."""
        return self.script_files + self.outfiles

    @property
    def runtime(self):
        """Return the runtime."""
        if not self.done:
            _logme.log('Cannot get runtime as not yet complete.' 'warn')
            return None
        if not self.start:
            self.get_times()
        return self.end-self.start

    @property
    def done(self):
        """Check if completed or not.

        Updates the Job and Queue.

        Returns:
            Bool: True if complete, False otherwise.
        """
        # We have the same statement twice to try and avoid updating.
        if self.state in _queue.DONE_STATES:
            return True
        if not self._updating:
            self.update()
            if self.state in _queue.DONE_STATES:
                return True
        return False

    ###############
    #  Internals  #
    ###############

    def _update(self, fetch_info=True):
        """Update status from the queue.

        Args:
            fetch_info (bool): Fetch basic job info if complete.
        """
        _logme.log('Updating job.', 'debug')
        self._updating = True
        if self.done or not self.submitted:
            self._updating = False
            return
        self.queue.update()
        if self.id:
            queue_info = self.queue[self.id]
            if queue_info:
                assert self.id == queue_info.id
                self.queue_info = queue_info
                self.state = self.queue_info.state
                if self.done and fetch_info:
                    if self.wait_for_files(btme=1, caution_message=False):
                        if not self._got_exitcode:
                            self.get_exitcode(update=False)
                        if not self._got_times:
                            self.get_times(update=False)
        self._updating = False
        return self

    def __getattr__(self, key):
        """Dynamically get out, stdout, stderr, and exitcode."""
        if key == 'out':
            return self.get_output()
        elif key == 'stdout':
            return self.get_stdout()
        elif key == 'stderr':
            return self.get_stderr()
        elif key == 'exitcode':
            return self.get_exitcode()
        elif key == 'err':
            return self.get_stderr()

    def __repr__(self):
        """Return simple job information."""
        outstr = "Job:{name}<{mode}:{qtype}".format(
            name=self.name, mode=self.kind, qtype=self.qtype)
        if self.submitted:
            outstr += ':{}'.format(self.id)
        outstr += "(command:{cmnd})".format(cmnd=self.command)
        if self.submitted or self.done:
            outstr += self.state.upper()
        elif self.written:
            outstr += "WRITTEN"
        else:
            outstr += "NOT_SUBMITTED"
        outstr += ">"
        return outstr

    def __str__(self):
        """Print job name and ID + status."""
        if self.done:
            state = 'completed'
            id1 = str(self.id)
        elif self.written:
            state = 'written'
            id1 = str(self.id)
        else:
            state = 'not written'
            id1 = 'NA'
        return "Job: {name} ID: {id}, state: {state}".format(
            name=self.name, id=id1, state=state)
