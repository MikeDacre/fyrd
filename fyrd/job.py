# -*- coding: utf-8 -*-
"""
Class and methods to handle Job submission.

Last modified: 2016-11-08 11:21
"""
import os  as _os
import sys as _sys
from uuid import uuid4 as _uuid
from time import sleep as _sleep
from datetime import datetime as _dt
from subprocess import CalledProcessError as _CalledProcessError

# Try to use dill, revert to pickle if not found
try:
    import dill as _pickle
except ImportError:
    try:
        import cPickle as _pickle # For python2
    except ImportError:
        import _pickle

###############################################################################
#                                Our functions                                #
###############################################################################

from . import run     as _run
from . import conf    as _conf
from . import queue   as _queue
from . import logme   as _logme
from . import local   as _local
from . import options as _options
from . import ClusterError as _ClusterError
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

    Methods:
        submit(): submit the job if it is ready
        wait():   block until the job is done
        get():    block until the job is done and then return the output
                  (stdout if job is a script), by default saves all outputs to
                  self (i.e. .out, .stdout, .stderr) and deletes all
                  intermediate files before returning. If `save` argument is
                  `False`, does not delete the output files by default.
        clean():  delete any files created by this object

    Output attributes:
        out:      The output of the function or a copy of stdout for a script
        stdout:   Any output to STDOUT
        stderr:   Any output to STDERR
        exitcode: The exitcode of the running processes (the script runner if
                  the Job is a function.
        start:    A datetime object containing time execution started on the
                  remote node.
        end:      Like start but when execution ended.
        runtime:  A timedelta object containing runtime.

    Printing the class will display detailed job information.

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
    .function attribute, which contains the script to run the function.

    """

    id            = None
    submitted     = False
    written       = False

    # Holds a pool object if we are in local mode
    pool_job      = None

    # Scripts
    submission    = None
    exec_script   = None
    function      = None

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

    # Autocleaning
    clean_files   = _conf.get_option('jobs', 'clean_files')
    clean_outputs = _conf.get_option('jobs', 'clean_outputs')

    def __init__(self, command, args=None, name=None, qtype=None,
                 profile=None, **kwds):
        """Create a job object will submission information.

        Args:
            command (function/str): The command or function to execute.
            args (tuple/dict):      Optional arguments to add to command,
                                    particularly useful for functions.
            name (str):             Optional name of the job. If not defined,
                                    guessed. If a job of the same name is
                                    already queued, an integer job number (not
                                    the queue number) will be added, ie.
                                    <name>.1
            qtype (str):            Override the default queue type
            profile (str):          The name of a profile saved in the
                                    conf
            kwargs (dict):          Keyword arguments to control job options

        There are many keyword arguments available for cluster job submission.
        These vary somewhat by queue type. For info run::
            fyrd.options.option_help()
        """

        ########################
        #  Sanitize arguments  #
        ########################

        # Make a copy of the keyword arguments, as we will delete arguments
        # as we go
        kwargs = _options.check_arguments(kwds.copy())

        # Override autoclean state (set in config file)
        if 'clean_files' in kwargs:
            self.clean_files = kwargs.pop('clean_files')
        if 'clean_outputs' in kwargs:
            self.clean_outputs = kwargs.pop('clean_outputs')

        # Save command
        self.command = command
        self.args    = args

        # Merge in profile
        if profile:
            # This is a Profile() object, the arguments are in the args dict
            prof = _conf.get_profile(profile)
            if prof:
                for k,v in prof.args.items():
                    if k not in kwargs:
                        kwargs[k] = v
            else:
                _logme.log('No profile found for {}'.format(profile), 'warn')

        # If no profile or keywords, use default profile, args is a dict
        default_args = _conf.get_profile('default').args
        if not profile and not kwargs:
            kwargs = default_args

        # Get required options
        req_options = _conf.get_option('opts')
        if req_options:
            for k,v in req_options.items():
                if k not in kwargs:
                    kwargs[k] = v

        # Get environment
        if not _queue.MODE:
            _queue.MODE = _queue.get_cluster_environment()
        self.qtype = qtype if qtype else _queue.MODE
        self.queue = _queue.Queue(user='self', qtype=self.qtype)
        self.state = 'Not_Submitted'

        # Set name
        if not name:
            if callable(command):
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

        # Make sure name not in queue
        self.uuid = str(_uuid()).split('-')[0]
        names     = [i.name.split('.')[0] for i in self.queue]
        namecnt   = len([i for i in names if i == name])
        name      = '{}.{}.{}'.format(name, namecnt, self.uuid)
        self.name = name

        # Set modules
        self.modules = kwargs.pop('modules') if 'modules' in kwargs else None
        if self.modules:
            self.modules = _run.opt_split(self.modules, (',', ';'))

        # Path handling
        runpath = _os.path.abspath(kwargs['dir'] if 'dir' in kwargs else '.')
        self.runpath = runpath

        # Set temp file path if different from runtime path
        cpath = _conf.get_option('jobs', 'filepath')
        if 'filepath' in kwargs:
            filepath = kwargs['filepath']
        elif cpath:
            filepath = cpath
        else:
            filepath = self.runpath
        filepath = _os.path.abspath(filepath)
        self.filepath = filepath

        # Make sure args are a tuple or dictionary
        if args:
            if not isinstance(args, (tuple, dict)):
                if isinstance(args, (list, set)):
                    args = tuple(args)
                else:
                    args = (args,)

        # In case cores are passed as None
        if 'nodes' not in kwargs:
            kwargs['nodes'] = default_args['nodes']
        if 'cores' not in kwargs:
            kwargs['cores'] = default_args['cores']
        self.nodes = kwargs['nodes']
        self.cores = kwargs['cores']

        # Set output files
        suffix = kwargs.pop('suffix') if 'suffix' in kwargs \
                 else _conf.get_option('jobs', 'suffix')
        if 'outfile' in kwargs:
            pth, fle = _os.path.split(kwargs['outfile'])
            if not pth:
                pth = self.filepath
            kwargs['outfile'] = _os.path.join(pth, fle)
        else:
            kwargs['outfile'] = _os.path.join(
                filepath, '.'.join([name, suffix, 'out']))
        if 'errfile' in kwargs:
            pth, fle = _os.path.split(kwargs['errfile'])
            if not pth:
                pth = self.filepath
            kwargs['errfile'] = _os.path.join(pth, fle)
        else:
            kwargs['errfile'] = _os.path.join(
                filepath, '.'.join([name, suffix, 'err']))
        self.outfile = kwargs['outfile']
        self.errfile = kwargs['errfile']

        # Check and set dependencies
        if 'depends' in kwargs:
            dependencies = kwargs.pop('depends')
            self.dependencies = []
            if isinstance(dependencies, 'str'):
                if not dependencies.isdigit():
                    raise _ClusterError('Dependencies must be number or list')
                else:
                    dependencies = [int(dependencies)]
            elif isinstance(dependencies, (int, Job)):
                dependencies = [dependencies]
            elif not isinstance(dependencies, (tuple, list)):
                raise _ClusterError('Dependencies must be number or list')
            for dependency in dependencies:
                if isinstance(dependency, str):
                    dependency  = int(dependency)
                if not isinstance(dependency, (int, Job)):
                    raise _ClusterError('Dependencies must be number or list')
                self.dependencies.append(dependency)

        ######################################
        #  Command and Function Preparation  #
        ######################################

        # Get imports
        if 'imports' in kwargs:
            self.imports = kwargs.pop('imports')
        else:
            self.imports = None

        # Function specific initialization
        if hasattr(command, '__call__'):
            self.kind = 'function'
            script_file = _os.path.join(
                filepath, '{}_func.{}.py'.format(name, suffix)
                )
            self.poutfile = self.outfile + '.func.pickle'
            self.function = _Function(
                file_name=script_file, function=command, args=args,
                outfile=self.poutfile, imports=self.imports
            )
            # Collapse the command into a python call to the function script
            command = '{} {}'.format(_sys.executable,
                                     self.function.file_name)
            args = None
        else:
            self.kind = 'script'
            self.poutfile = None

        # Collapse args into command
        command = command + ' '.join(args) if args else command

        #####################
        #  Script Creation  #
        #####################

        # Build execution wrapper with modules
        precmd  = ''
        if self.modules:
            for module in self.modules:
                precmd += 'module load {}\n'.format(module)

        # Create queue-dependent scripts
        sub_script = ''
        if self.qtype == 'slurm':
            scrpt = _os.path.join(filepath, '{}.{}.sbatch'.format(name, suffix))

            # We use a separate script and a single srun command to avoid
            # issues with multiple threads running at once
            exec_script  = _os.path.join(filepath,
                                         '{}.{}.script'.format(name, suffix))
            exe_script   = _run.CMND_RUNNER_TRACK.format(
                precmd=precmd, usedir=runpath, name=name, command=command)
            # Create the exec_script Script object
            self.exec_script = _Script(script=exe_script,
                                       file_name=exec_script)

            # Add all of the keyword arguments at once
            precmd += _options.options_to_string(kwargs, self.qtype)

            ecmnd = 'srun bash {}'.format(exec_script)
            sub_script = _run.SCRP_RUNNER.format(precmd=precmd,
                                                 script=exec_script,
                                                 command=ecmnd)

        elif self.qtype == 'torque':
            scrpt = _os.path.join(filepath, '{}.cluster.qsub'.format(name))

            # Add all of the keyword arguments at once
            precmd += _options.options_to_string(kwargs, self.qtype)

            sub_script = _run.CMND_RUNNER_TRACK.format(
                precmd=precmd, usedir=runpath, name=name, command=command)

        elif self.qtype == 'local':
            # Create the pool
            if not _local.JQUEUE or not _local.JQUEUE.runner.is_alive():
                threads = kwargs['threads'] if 'threads' in kwargs \
                        else _local.THREADS
                _local.JQUEUE = _local.JobQueue(cores=threads)

            scrpt = _os.path.join(filepath, '{}.cluster'.format(name))
            sub_script = _run.CMND_RUNNER_TRACK.format(
                precmd=precmd, usedir=runpath, name=name, command=command)

        else:
            raise _ClusterError('Invalid queue type')

        # Create the submission Script object
        self.submission = _Script(script=sub_script,
                                  file_name=scrpt)

        # Save the keyword arguments for posterity
        self.kwargs = kwargs

    ####################
    #  Public Methods  #
    ####################

    def write(self, overwrite=True):
        """Write all scripts.

        Args:
            overwrite (bool): Overwrite existing files, defaults to True.
        """
        _logme.log('Writing files, overwrite={}'.format(overwrite), 'debug')
        self.submission.write(overwrite)
        if self.exec_script:
            self.exec_script.write(overwrite)
        if self.function:
            self.function.write(overwrite)
        self.written = True

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
        if delete_outputs is None:
            delete_outputs = self.clean_outputs
        assert isinstance(delete_outputs, bool)
        for jobfile in [self.submission, self.exec_script, self.function]:
            if jobfile:
                jobfile.clean()
        if delete_outputs:
            _logme.log('Deleting output files.', 'debug')
            if get_outputs:
                self.fetch_outputs(delete_files=True)
            for f in self.outfiles:
                if _os.path.isfile(f):
                    _logme.log('Deleteing {}'.format(f), 'debug')
                    _os.remove(f)

    def submit(self, max_queue_len=None):
        """Submit this job.

        Args:
            max_queue_len: if specified (or in defaults), then this method will
                           block until the queue is open enough to allow
                           submission.

        To disable max_queue_len, set it to 0. None will allow override by
        the default settings in the config file, and any positive integer will
        be interpretted to be the maximum queue length.

        Returns:
            self
        """
        if self.submitted:
            _logme.log('Not submitting, already submitted.', 'warn')
            return

        if not isinstance(max_queue_len, (type(None), int)):
            raise ValueError('max_queue_len must be int or None, is {}'
                             .format(type(max_queue_len)))

        if not self.written:
            self.write()

        self.update()

        if max_queue_len is not 0:
            self.queue.wait_to_submit(max_queue_len)

        if self.qtype == 'local':
            # Normal mode dependency tracking uses only integer job numbers
            _logme.log('Submitting to local', 'debug')
            dependencies = []
            if self.dependencies:
                for depend in self.dependencies:
                    if isinstance(depend, Job):
                        dependencies.append(int(depend.id))
                    else:
                        dependencies.append(int(depend))

            command = 'bash {}'.format(self.submission.file_name)
            fileargs  = dict(stdout=self.outfile,
                             stderr=self.errfile)

            # Make sure the global job pool exists
            if not _local.JQUEUE or not _local.JQUEUE.runner.is_alive():
                _local.JQUEUE = _local.JobQueue(cores=_local.THREADS)
            self.id = _local.JQUEUE.add(_run.cmd, args=(command,),
                                        kwargs=fileargs,
                                        dependencies=dependencies,
                                        cores=self.cores)
            self.submitted = True
            self.state = 'submitted'

        elif self.qtype == 'slurm':
            _logme.log('Submitting to slurm', 'debug')
            if self.dependencies:
                dependencies = []
                for depend in self.dependencies:
                    if isinstance(depend, Job):
                        dependencies.append(str(depend.id))
                    else:
                        dependencies.append(str(depend))
                    depends = '--dependency=afterok:{}'.format(
                        ':'.join(dependencies))
                    args = ['sbatch', depends, self.submission.file_name]
            else:
                args = ['sbatch', self.submission.file_name]

            # Try to submit job 5 times
            code, stdout, stderr = _run.cmd(args, tries=5)
            if code == 0:
                self.id = int(stdout.split(' ')[-1])
            else:
                _logme.log('sbatch failed with code {}\n'.format(code),
                           'stdout: {}\nstderr: {}'.format(stdout, stderr),
                           'critical')
                raise _CalledProcessError(code, args, stdout, stderr)
            self.submitted = True
            self.state = 'submitted'

        elif self.qtype == 'torque':
            _logme.log('Submitting to torque', 'debug')
            if self.dependencies:
                dependencies = []
                for depend in self.dependencies:
                    if isinstance(depend, Job):
                        dependencies.append(str(depend.id))
                    else:
                        dependencies.append(str(depend))
                depends = '-W depend={}'.format(
                    ','.join(['afterok:' + d for d in dependencies]))
                args = ['qsub', depends, self.submission.file_name]
            else:
                args = ['qsub', self.submission.file_name]

            # Try to submit job 5 times
            code, stdout, stderr = _run.cmd(args, tries=5)
            if code == 0:
                self.id = int(stdout.split('.')[0])
            else:
                if stderr.startswith('qsub: submit error ('):
                    raise _ClusterError('qsub submission failed with error: ' +
                                        '{}, command: {}'.format(stderr, args))
                else:
                    _logme.log(
                        'qsub failed with code {}\nstdout: {}\nstderr: {}'
                        .format(code, stdout, stderr), 'critical'
                    )
                    raise _CalledProcessError(code, args, stdout, stderr)
            self.submitted = True
            self.state = 'submitted'
        else:
            raise _ClusterError("Invalid queue type {}".format(self.qtype))

        if not self.submitted:
            raise _ClusterError('Submission appears to have failed, this '
                                "shouldn't happen")

        return self

    def update(self):
        """Update status from the queue."""
        if not self._updating:
            self._update()
        else:
            _logme.log('Already updating, aborting.', 'debug')

    def update_queue_info(self):
        """Set queue_info from the queue even if done."""
        _logme.log('Updating queue_info', 'debug')
        queue_info1 = self.queue[self.id]
        self.queue.update()
        queue_info2 = self.queue[self.id]
        if queue_info2:
            self.queue_info = queue_info2
        elif queue_info1:
            self.queue_info = queue_info1
        elif self.queue_info is None and self.submitted:
            _logme.log('Cannot find self in the queue and queue_info is empty',
                       'warn')
        return self.queue_info

    def wait(self):
        """Block until job completes."""
        if not self.submitted:
            if _conf.get_option('jobs', 'auto_submit'):
                _logme.log('Auto-submitting as not submitted yet', 'debug')
                self.submit()
            else:
                _logme.log('Cannot wait for result as job has not been submitted',
                           'warn')
            return False
        _sleep(0.1)
        self.update()
        if self.done:
            return True
        _logme.log('Waiting for self {}'.format(self.name), 'debug')
        if self.queue.wait(self) is not True:
            return False
        # Block for up to file_block_time for output files to be copied back
        btme = _conf.get_option('jobs', 'file_block_time')
        start = _dt.now()
        while True:
            _logme.log('Checking for output files', 'debug')
            count = 0
            for i in self.outfiles:
                if _os.path.isfile(i):
                    count += 1
            if count == len(self.outfiles):
                _logme.log('All output files found', 'debug')
                break
            if (_dt.now() - start).seconds > btme:
                _logme.log('Job completed but files have not appeared for ' +
                           '>{} seconds'.format(btme))
                return False
        self.update()
        return True

    def get(self, save=True, cleanup=None, delete_outfiles=None,
            del_no_save=None):
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

        Returns:
            str: Function output if Function, else STDOUT
        """
        _logme.log(('Getting outputs, cleanup={}, autoclean={}, '
                    'delete_outfiles={}').format(
                        cleanup, self.auto_delete, delete_outfiles
                    ), 'debug')
        # Wait for queue
        if self.wait() is not True:
            _logme.log('Wait failed, cannot get outputs, aborting', 'error')
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
        if isinstance(out, Exception):
            _logme.log('Job {} ({}) failed with {}'
                       .format(self.name, self.id, out), 'critical')
            raise out
        return out

    def get_output(self, save=True, delete_file=None, update=True):
        """Get output of function or script.

        This is the same as stdout for a script, or the function output for
        a function.

        By default, output file is kept unless delete_file is True or
        self.auto_delete is True.

        Args:
            save (bool):        Save the output to self.out, default True.
                                Would be a good idea to set to False if the
                                output is huge.
            delete_file (bool): Delete the output file when getting
            update (bool):      Update job info from queue first.

        Returns:
            The output of the script or function. Always a string if script.
        """
        _logme.log(('Getting output, save={}, auto_delete={}, '
                    'delete_file={}').format(
                        save, self.auto_delete, delete_file
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
        if not self.done:
            _logme.log('Cannot get pickled output before job completes',
                       'warn')
            return None
        _logme.log('Getting output from {}'.format(self.poutfile), 'debug')
        if _os.path.isfile(self.poutfile):
            with open(self.poutfile, 'rb') as fin:
                out = _pickle.load(fin)
            if delete_file is True or self.auto_delete is True:
                _logme.log('Deleting {}'.format(self.poutfile),
                           'debug')
                _os.remove(self.poutfile)
            if save:
                self._out = out
                self._got_out = True
            return out
        else:
            _logme.log('No file at {} even though job has completed!'
                       .format(self.poutfile), 'critical')
            raise IOError('File not found: {}'.format(self.poutfile))

    def get_stdout(self, save=True, delete_file=None, update=True):
        """Get stdout of function or script, same for both.

        By default, output file is kept unless delete_file is True or
        self.auto_delete is True.

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
        _logme.log(('Getting stdout, save={}, auto_delete={}, '
                    'delete_file={}').format(
                        save, self.auto_delete, delete_file
                    ), 'debug')
        if delete_file is None:
            delete_file = self.clean_outputs
        if self.done and self._got_stdout:
            _logme.log('Getting stdout from _stdout', 'debug')
            return self._stdout
        if update and not self._updating and not self.done:
            self.update()
        if not self.done:
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
            if delete_file is True or self.auto_delete is True:
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
        self.auto_delete is True.

        Args:
            save (bool):        Save the output to self.stdout, default True.
                                Would be a good idea to set to False if the
                                output is huge.
            delete_file (bool): Delete the stdout file when getting
            update (bool):      Update job info from queue first.

        Returns:
            str: The contents of STDERR, with trailing newline removed.
        """
        _logme.log(('Getting stderr, save={}, auto_delete={}, '
                    'delete_file={}').format(
                        save, self.auto_delete, delete_file
                    ), 'debug')
        if delete_file is None:
            delete_file = self.clean_outputs
        if self.done and self._got_stderr:
            _logme.log('Getting stderr from _stderr', 'debug')
            return self._stderr
        if update and not self._updating and not self.done:
            self.update()
        if not self.done:
            _logme.log('Job not done, attempting to get current STDERR ' +
                       'anyway', 'info')
        _logme.log('Getting stderr from {}'.format(self.kwargs['errfile']),
                   'debug')
        if _os.path.isfile(self.kwargs['errfile']):
            stderr = open(self.kwargs['errfile']).read()
            if delete_file is True or self.auto_delete is True:
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
        if not self.done:
            _logme.log('Cannot get times until job is complete.', 'warn')
            return None, None
        _logme.log('Getting times from {}'.format(self.kwargs['outfile']),
                   'debug')
        if _os.path.isfile(self.kwargs['outfile']):
            stdout = open(self.kwargs['outfile']).read()
            if stdout:
                stdouts = stdout.split('\n')
                # Get times
                timefmt    = '%y-%m-%d-%H:%M:%S'
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
        if not self.done:
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

    @property
    def files(self):
        """Build a list of files associated with this class."""
        files = [self.submission]
        if self.kind == 'script':
            files.append(self.exec_script)
        if self.kind == 'function':
            files.append(self.function)
        return files

    @property
    def done(self):
        """Check if completed or not.

        Updates the Job and Queue.

        Returns:
            Bool: True if complete, False otherwise.
        """
        # We have the same statement twice to try and avoid updating.
        if self.state == 'completed':
            return True
        if not self._updating:
            self.update()
            if self.state == 'completed':
                return True
        return False

    ###############
    #  Internals  #
    ###############

    def _update(self):
        """Update status from the queue."""
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
                if self.state == 'completed':
                    if not self._got_exitcode:
                        self.get_exitcode()
                    if not self._got_times:
                        self.get_times()
        self._updating = False

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
        elif key == 'runtime':
            if not self.done:
                _logme.log('Cannot get runtime as not yet complete.' 'warn')
                return None
            if not self.start:
                self.get_times()
            return self.end-self.start
        elif key == 'outfiles':
            outfiles = [self.outfile, self.errfile]
            if self.poutfile:
                outfiles.append(self.poutfile)
            return outfiles

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
