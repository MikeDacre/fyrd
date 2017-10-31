# -*- coding: utf-8 -*-
"""
Class and methods to handle Job submission.

This module only defines a single object: the Job class.
"""
import os  as _os
import sys as _sys
from uuid import uuid4 as _uuid
from time import sleep as _sleep
from datetime import datetime as _dt
from traceback import print_tb as _tb

# Try to use dill, revert to pickle if not found
import dill as _pickle
from six import reraise as _reraise
from six import text_type as _txt
from six import string_types as _str
from six import integer_types as _int

###############################################################################
#                                Our functions                                #
###############################################################################

from . import run     as _run
from . import conf    as _conf
from . import queue   as _queue
from . import logme   as _logme
from . import script_runners as _scrpts
from . import batch_systems  as _batch
from . import ClusterError   as _ClusterError
from .submission_scripts import Function as _Function
_options = _batch.options

__all__ = ['Job']

###############################################################################
#                                The Job Class                                #
###############################################################################


class Job(object):

    """Information about a single job on the cluster.

    Holds information about submit time, number of cores, the job script,
    and more.

    Below are the core attributes and methods required to use this class,
    note that this is an incomplete list.

    Attributes
    ----------
    id : str
        The ID number for the job, only set once the job has been submitted
    name : str
        The name of the job
    command : str or callable
        The function or shell script that will be submitted
    args : list
        A list of arguments to the shell script or function in command
    kwargs : dict
        A dictionary of keyword arguments to the function (not shell script) in
        command
    state : str
        A slurm-style one word description of the state of the job, one of:
            - Not_Submitted
            - queued
            - running
            - completed
            - failed
    submitted : bool
    written : bool
    done : bool
    running : bool
    dependencies : list
        A list of dependencies associated with this job
    out : str
        The output of the function or a copy of stdout for a script
    stdout : str
        Any output to STDOUT
    stderr : str
        Any output to STDERR
    exitcode : int
        The exitcode of the running processes (the script runner if the Job is
        a function).
    submit_time : datetime
        A datetime object for the time of submission
    start : datetime
        A datetime object for time execution started on the remote node.
    end : datetime
        A datetime object for time execution ended on the remote node.
    runtime : timedelta
        A timedelta object containing runtime.
    files : list
        A list of script files associated with this job
    nodes : list
        A list of nodes associated with this job
    modules : list
        A list of modules associated with this job
    clean_files : bool
        If True, auto-delete script and function files on job completion
    clean_outputs : bool
        If True, auto-delete script outputs and error files on job completion
    kwds : dict
        Keyword arguments to the batch system (e.g. mem, cores, walltime), this
        is initialized by taking every additional keyword argument to the Job.
        e.g. Job('echo hi', profile=large, walltime='00:20:00', mem='2GB') will
        result in kwds containing {walltime: '00:20:00', mem: '2GB'}. There is
        **no need to alter this manually**.
    submit_args : list
        List of parsed submit arguments that will be passed at runtime to the
        submit function. **Generated within the Job object**, no need to set
        manually, use the `kwds` attribute instead.

    Methods
    -------
    initialize()
        Use attributes to prep job for running
    gen_scripts()
        Create script files (but do not write them)
    write(overwrite=True)
        Write scripts to files
    submit(wait_on_max_queue=True)
        Submit the job if it is ready and the queue is sufficiently open.
    resubmit(wait_on_max_queue=True)
        Clean all internal states with `scrub()` and then resubmit
    kill(confirm=True)
        Immediately kill the currently running job
    clean(delete_outputs=True, get_outputs=True)
        Delete any files created by this object
    scrub(confirm=True)
        Clean everything and reset to an unrun state.
    update(fetch_info=True)
        Update our status from the queue
    wait()
        Block until the job is done
    get()
        Block until the job is done and then return the output (stdout if job
        is a script), by default saves all outputs to self (i.e. .out, .stdout,
        .stderr) and deletes all intermediate files before returning. If `save`
        argument is `False`, does not delete the output files by default.

    Notes
    -----
    Printing or reproducing the class will display detailed job information.

    Both `wait()` and `get()` will update the queue every few seconds
    (defined by the queue_update item in the config) and add queue information
    to the job as they go.

    If the job disappears from the queue with no information, it will be listed
    as 'completed'.

    All jobs have a .submission attribute, which is a Script object containing
    the submission script for the job and the file name, plus a 'written' bool
    that checks if the file exists.

    In addition, some batch systems (e.g. SLURM) have an .exec_script
    attribute, which is a Script object containing the shell command to run.
    This difference is due to the fact that some SLURM systems execute multiple
    lines of the submission file at the same time.

    Finally, if the job command is a function, this object will also contain a
    `.function` attribute, which contains the script to run the function.
    """

    id            = None
    name          = None
    suffix        = None
    submitted     = False
    written       = False
    found         = False
    disappeared   = False
    submit_time   = None
    state         = None
    kind          = None

    # Arguments
    kwds          = None
    kwargs        = None
    submit_args   = None

    # Runtime
    nodes         = None
    cores         = None
    modules       = None

    # Files
    outfile       = None
    errfile       = None

    # Scripts
    submission    = None
    exec_script   = None
    function      = None
    imports       = None

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
    _found_files  = False
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

    # Track preparations
    initialized   = False
    scripts_ready = False
    _kwargs       = None

    # Auto Cleaning
    clean_files   = _conf.get_option('jobs', 'clean_files')
    clean_outputs = _conf.get_option('jobs', 'clean_outputs')

    def __init__(self, command, args=None, kwargs=None, name=None, qtype=None,
                 profile=None, **kwds):
        """Initialization function arguments.

        Parameters
        ----------
        command : function/str
            The command or function to execute.
        args : tuple/dict, optional
            Optional arguments to add to command, particularly useful for
            functions.
        kwargs : dict, optional
            Optional keyword arguments to pass to the command, only used for
            functions.
        name : str, optional
            Optional name of the job. If not defined, guessed. If a job of the
            same name is already queued, an integer job number (not the queue
            number) will be added, ie.  <name>.1
        qtype : str, optional
            Override the default queue type
        profile : str, optional
            The name of a profile saved in the conf
        kwds
            *All other keywords are parsed into cluster keywords by the options
            system.* For available keywords see `fyrd.option_help()`
        """

        ########################
        #  Sanitize arguments  #
        ########################
        _logme.log('Args pre-check: {}'.format(kwds), 'debug')
        kwds = _options.check_arguments(kwds)
        _logme.log('Args post-check: {}'.format(kwds), 'debug')

        # Create a unique short UUID for this job
        self.uuid = str(_uuid()).split('-')[0]

        # Path handling
        [kwds, self.runpath,
         self.outpath, self.scriptpath] = _conf.get_job_paths(kwds)

        # Save command
        self.command = command
        self.args    = args
        self.kwargs  = kwargs
        self.profile = profile

        # Get environment
        if not _batch.MODE:
            _batch.MODE = _batch.get_cluster_environment()
        self.qtype = qtype if qtype else _batch.MODE
        self.queue = _queue.Queue(user='self', qtype=self.qtype)
        self.batch = _batch.get_batch_system(self.qtype)
        self.state = 'Not_Submitted'

        # Save keywords for posterity and parsing
        self.kwds = kwds

        self.name = self._update_name(name)

        self.initialize()

    ##########################################################################
    #                             Public Methods                             #
    ##########################################################################

    ################
    #  Properties  #
    ################

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

        Returns
        -------
        done : bool
        """
        # We have the same statement twice to try and avoid updating.
        if self.state in _batch.DONE_STATES:
            return True
        if not self._updating:
            self.update()
            if self.state in _batch.DONE_STATES:
                return True
        return False

    @property
    def running(self):
        """Check if running or not.

        Updates the Job and Queue.

        Returns
        -------
        running : bool
        """
        # We have the same statement twice to try to avoid updating.
        if self.state in _batch.ACTIVE_STATES:
            return True
        if not self._updating:
            self.update()
            if self.state in _batch.ACTIVE_STATES:
                return True
        return False

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
    def exitcode(self):
        """Return exitcode."""
        return self.get_exitcode()

    @property
    def code(self):
        """Return exitcode."""
        return self.get_exitcode()

    @property
    def out(self):
        """Return output."""
        return self.get_output()

    @property
    def stdout(self):
        """Return output."""
        return self.get_stdout()

    @property
    def stderr(self):
        """Return stderr."""
        return self.get_stderr()

    @property
    def err(self):
        """Return stderr."""
        return self.get_stderr()

    ###############################
    #  Core Job Handling Methods  #
    ###############################

    def initialize(self):
        """Make self runnable using set attributes."""
        kwds = self.kwds

        # Override autoclean state (set in config file)
        if 'clean_files' in kwds:
            self.clean_files = kwds.pop('clean_files')
        if 'clean_outputs' in kwds:
            self.clean_outputs = kwds.pop('clean_outputs')

        # Set suffix
        self.suffix = kwds.pop('suffix') if 'suffix' in kwds \
                      else _conf.get_option('jobs', 'suffix')

        # Merge in profile, this includes all args from the DEFAULT profile
        # as well, ensuring that those are always set at a minumum.
        profile = self.profile if self.profile else 'DEFAULT'
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

        # Set modules
        self.modules = kwds.pop('modules') if 'modules' in kwds else None
        if self.modules:
            self.modules = _run.opt_split(self.modules, (',', ';'))

        # Make sure args are a tuple
        if self.args:
            self.args = tuple(_run.listify(self.args))

        # In case cores are passed as None
        if 'nodes' not in kwds:
            kwds['nodes'] = default_args['nodes']
        if 'cores' not in kwds:
            kwds['cores'] = default_args['cores']
        self.nodes = kwds['nodes']
        self.cores = kwds['cores']

        # Set output files
        if 'outfile' in kwds:
            pth, fle = _os.path.split(kwds['outfile'])
            if not pth:
                pth = self.outpath
            kwds['outfile'] = _os.path.join(pth, fle)
        else:
            kwds['outfile'] = _os.path.join(
                self.outpath, '.'.join([self.name, self.suffix, 'out']))
        if 'errfile' in kwds:
            pth, fle = _os.path.split(kwds['errfile'])
            if not pth:
                pth = self.outpath
            kwds['errfile'] = _os.path.join(pth, fle)
        else:
            kwds['errfile'] = _os.path.join(
                self.outpath, '.'.join([self.name, self.suffix, 'err']))
        self.outfile = kwds['outfile']
        self.errfile = kwds['errfile']

        # Check and set dependencies
        if 'depends' in kwds:
            dependencies = _run.listify(kwds.pop('depends'))
            self.dependencies = []
            errmsg = 'Dependencies must be number, numeric string or Job'
            for dependency in dependencies:
                if not isinstance(dependency, (_str, _txt, Job)):
                    raise _ClusterError(errmsg)
                self.dependencies.append(dependency)

        # Save parsed keywords as _kwargs
        self._kwargs = kwds

        self.initialized = True

        return self

    def gen_scripts(self):
        """Create the script objects from the set parameters."""
        if not self.initialized:
            self.initialize()

        ######################################
        #  Command and Function Preparation  #
        ######################################
        command = self.command
        args    = self.args
        kwargs  = self.kwargs  # Not self._kwargs
        name    = self._update_name()
        kwds    = self._kwargs

        # Get imports
        imports = kwds.pop('imports') if 'imports' in kwds else None

        # Get syspaths
        syspaths = kwds.pop('syspaths') if 'syspaths' in kwds else None

        # Split out sys.paths from imports and set imports in self
        if imports:
            self.imports = []
            syspaths = syspaths if syspaths else []
            for i in imports:
                if i.startswith('sys.path.append')\
                        or i.startswith('sys.path.insert'):
                    syspaths.append(i)
                else:
                    self.imports.append(i)

        # Function specific initialization
        if callable(command):
            self.kind = 'function'
            script_file = _os.path.join(
                self.scriptpath, '{}_func.{}.py'.format(name, self.suffix)
                )
            self.poutfile = self.outfile + '.func.pickle'
            self.function = _Function(
                file_name=script_file, function=command, args=args,
                kwargs=kwargs, imports=self.imports, syspaths=syspaths,
                outfile=self.poutfile
            )
            # Collapse the _command into a python call to the function script
            executable = '#!/usr/bin/env python{}'.format(
                _sys.version_info.major) if _conf.get_option(
                    'jobs', 'generic_python') else _sys.executable

            command = '{} {}'.format(executable, self.function.file_name)
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
        modstr  = ''
        if self.modules:
            for module in self.modules:
                modstr += 'module load {}\n'.format(module)

        # Add all of the keyword arguments at once
        opt_string, submit_args = _options.options_to_string(kwds, self.qtype)
        precmd = opt_string + '\n\n' + modstr
        self.submit_args = submit_args

        # Create queue-dependent scripts
        self.submission, self.exec_script = self.batch.gen_scripts(
            self, command, args, precmd, modstr
        )

        self.scripts_ready = True

        return self

    def write(self, overwrite=True):
        """Write all scripts.

        Parameters
        ----------
        overwrite : bool, optional
            Overwrite existing files, defaults to True.

        Returns
        -------
        self : Job
        """
        if not self.scripts_ready:
            self.gen_scripts()
        _logme.log('Writing files, overwrite={}'.format(overwrite), 'debug')
        self.submission.write(overwrite)
        if self.exec_script:
            self.exec_script.write(overwrite)
        if self.function:
            self.function.write(overwrite)
        self.written = True
        return self

    def submit(self, wait_on_max_queue=True, additional_keywords=None,
               max_jobs=None):
        """Submit this job.

        To disable max_queue_len, set it to 0. None will allow override by
        the default settings in the config file, and any positive integer will
        be interpretted to be the maximum queue length.

        Parameters
        ----------
        wait_on_max_queue : bool, optional
            Block until queue limit is below the maximum before submitting.
        additional_keywords : dict, optional
            Pass this dictionary to the batch system submission function,
            not necessary.
        max_jobs : int, optional
            Override the maximum number of jobs to wait for

        Returns
        -------
        self : Job
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
                    dependencies.append(str(depend.id))
                else:
                    dependencies.append(str(depend))

        # Wait on the queue if necessary
        if wait_on_max_queue:
            if not self._updating:
                self.update()
            self.queue.wait_to_submit(max_jobs)

        # Only include queued or running dependencies
        self.queue._update()  # Force update
        depends = []
        for depend in dependencies:
            dep_check = self.queue.check_dependencies(depend)
            if dep_check == 'absent':
                _logme.log(
                    'Cannot submit job as dependency {} '
                    .format(depend) + 'is not in the queue',
                    'error'
                )
                return self
            elif dep_check == 'good':
                _logme.log(
                    'Dependency {} is complete, skipping'
                    .format(depend), 'debug'
                )
            elif dep_check == 'bad':
                _logme.log(
                    'Cannot submit job as dependency {} '
                    .format(depend) + 'has failed',
                    'error'
                )
                return self
            elif dep_check == 'active':
                if self.queue.jobs[depend].state == 'completeing':
                    continue
                _logme.log('Dependency {} is {}, adding to deps'
                           .format(depend, self.queue.jobs[depend].state),
                           'debug')
                depends.append(depend)
            else:
                # This shouldn't happen ever
                raise _ClusterError('fyrd.queue.Queue.check_dependencies() ' +
                                    'returned an unrecognized value {0}'
                                    .format(dep_check))

        self.id = self.batch.submit(
            self.submission.file_name,
            dependencies=depends,
            job=self, args=self.submit_args,
            kwds=additional_keywords
        )

        self.submitted = True
        self.submit_time = _dt.now()
        self.state = 'submitted'

        if not self.submitted:
            raise _ClusterError('Submission appears to have failed, this '
                                "shouldn't happen")

        return self

    def resubmit(self, wait_on_max_queue=True, cancel_running=None):
        """Attempt to auto resubmit, deletes prior files.

        Parameters
        ----------
        wait_on_max_queue : bool, optional
            Block until queue limit is below the maximum before submitting.
        cancel_running : bool or None, optional
            If the job is currently running, cancel it before resubmitting.
            If None (default), will ask the user.

        To disable max_queue_len, set it to 0. None will allow override by
        the default settings in the config file, and any positive integer will
        be interpretted to be the maximum queue length.

        Returns
        -------
        self : Job
        """
        if self.running:
            if cancel_running is None:
                cancel_running = _run.get_yesno(
                    'Job currently running, cancel before resubmitting?', 'y'
                )
            if cancel_running:
                self.kill(confirm=False)
        self.scrub(confirm=False)
        # Rerun
        self.initialize()
        self.gen_scripts()
        self.write()
        return self.submit(wait_on_max_queue)

    def kill(self, confirm=True):
        """Kill the running job.

        Parameters
        ----------
        confirm : bool, optional

        Returns
        -------
        self : Job
        """
        if not self.submitted:
            _logme.log('Job not submitted, cannot kill', 'warn')
            return self
        if self.done:
            _logme.log('Job completed, cannot kill', 'warn')
            return self
        if confirm:
            if not _run.get_yesno(
                'This will terminate the running job, continue?', 'n'
            ):
                return self
        self.batch.kill(self.id)
        return self

    def clean(self, delete_outputs=None, get_outputs=True):
        """Delete all scripts created by this module, if they were written.

        Parameters
        ----------
        delete_outputs : bool, optional
            also delete all output and err files, but get their contents first.
        get_outputs : bool, optional
            if delete_outputs, save outputs before deleting.

        Returns
        -------
        self : Job
        """
        _logme.log('Cleaning outputs, delete_outputs={}'
                   .format(delete_outputs), 'debug')
        if not isinstance(delete_outputs, bool):
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
        return self

    def scrub(self, confirm=True):
        """Clean everything and reset to an unrun state.

        Parameters
        ----------
        confirm : bool, optional
            Get user input before proceeding

        Returns
        -------
        self : Job
        """
        msg = ("This will delete all outputs stored in this job, as well "
               "as all output files, job files, and scripts. Are you sure "
               "you want to do this?")
        if confirm:
            _run.get_yesno(msg, default='n')
        # Clean old set
        self.clean(delete_outputs=True)
        # Reset runtime attributes
        self.initialized   = False
        self.scripts_ready = False
        self.written       = False
        self.submitted     = False
        self.id            = None
        self.found         = False
        self.queue_info    = None
        self.state         = 'Not_Submitted'
        self._got_out      = False
        self._got_stdout   = False
        self._got_stderr   = False
        self._got_exitcode = False
        self._out          = None
        self._stdout       = None
        self._stderr       = None
        self._exitcode     = None
        self._got_times    = False
        self._updating     = False
        self._found_files  = False
        self.start         = None
        self.end           = None
        return self.update()

    ######################
    #  Queue Management  #
    ######################

    def update(self, fetch_info=True):
        """Update status from the queue.

        Parameters
        ----------
        fetch_info : bool, optional
            Fetch basic job info if complete.

        Returns
        -------
        self : Job
        """
        if not self._updating:
            self._update(fetch_info)
        else:
            _logme.log('Already updating, aborting.', 'debug')
        return self

    def update_queue_info(self):
        """Set (and return) queue_info from the queue even if done."""
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

    #################################
    #  Output Handling and Waiting  #
    #################################

    def wait(self):
        """Block until job completes.

        Returns
        -------
        success : bool or str
            True if exitcode == 0, False if not, 'disappeared' if job lost from
            queue.
        """
        if not self.submitted:
            if _conf.get_option('jobs', 'auto_submit'):
                _logme.log('Auto-submitting as not submitted yet', 'debug')
                self.submit()
            else:
                _logme.log('Cannot wait for result as job has not been ' +
                           'submitted', 'warn')
                return False
        self.update(fetch_info=False)
        if not self.done:
            _logme.log('Waiting for self {}'.format(self.name), 'debug')
            status = self.queue.wait(self.id, return_disp=True)
            if status == 'disappeared':
                self.state = status
            elif status is not True:
                return False
            else:
                if not self._updating:
                    self.update()
                if self.get_exitcode(update=False) != 0:
                    _logme.log('Job failed with exitcode {}'
                               .format(self.exitcode), 'debug')
                    return False
        if self._wait_for_files(caution_message=False):
            if not self._updating:
                self.update()
            if self.state == 'disappeared':
                _logme.log('Job files found for disappered job, assuming '
                           'success', 'info')
                return 'disappeared'
            return True
        else:
            if self.state == 'disappeared':
                _logme.log('Disappeared job has no output files, assuming '
                           'failure', 'error')
            return False

    def get(self, save=True, cleanup=None, delete_outfiles=None,
            del_no_save=None, raise_on_error=True):
        """Block until job completed and return output of script/function.

        By default saves all outputs to this class and deletes all intermediate
        files.

        Parameters
        ----------
        save : bool, optional
            Save all outputs to the class also (advised)
        cleanup : bool, optional
            Clean all intermediate files after job completes.
        delete_outfiles : bool, optional
            Clean output files after job completes.
        del_no_save : bool, optional
            Delete output files even if `save` is `False`
        raise_on_error : bool, optional
            If the returned output is an Exception, raise it.

        Returns
        -------
        str
            Function output if Function, else STDOUT
        """
        _logme.log(('Getting outputs, cleanup={}, autoclean={}, '
                    'delete_outfiles={}').format(
                        cleanup, self.clean_files, delete_outfiles
                    ), 'debug')
        # Wait for queue
        status = self.wait()
        if status is not True:
            if status == 'disappeared':
                msg = 'Job disappeared from queue'
                _logme.log(msg + ', attempting to get '
                           'outputs', 'debug')
            else:
                msg = 'Wait failed'
                _logme.log(msg + ', attempting to get outputs anyway',
                           'debug')
            try:
                self.fetch_outputs(save=save, delete_files=False,
                                   get_stats=False)
            except IOError:
                _logme.log(msg + ' and files could not be found, job must '
                           'have failed', 'error')
                if raise_on_error:
                    raise
                return
            if status != 'disappeared':
                return
        else:
            # Get output
            _logme.log('Wait complete, fetching outputs', 'debug')
            self.fetch_outputs(save=save, delete_files=False)
        out = self.out if save else self.get_output(save=save, update=False)
        if isinstance(out, tuple) and issubclass(out[0], Exception):
            if raise_on_error:
                _reraise(*out)
            else:
                _logme.log('Job failed with exception {}'.format(out))
                print(_tb(out[2]))
                return out
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

        Parameters
        ----------
        save : bool, optional
            Save the output to self.out, default True.  Would be a good idea to
            set to False if the output is huge.
        delete_file : bool, optional
            Delete the output file when getting
        update : bool, optional
            Update job info from queue first.
        raise_on_error : bool, optional
            If the returned output is an Exception, raise it.

        Returns
        -------
        output : anything
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
                self._wait_for_files()
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

        Also sets self.start and self.end from the contents of STDOUT if
        possible.

        Returns
        -------
        save : bool, optional
            Save the output to self.stdout, default True.  Would be a good idea
            to set to False if the output is huge.
        delete_file : bool, optional
            Delete the stdout file when getting
        update : bool, optional
            Update job info from queue first.

        Returns
        -------
        str
            The contents of STDOUT, with runtime info and trailing newline
            removed.
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
                self._wait_for_files()
        else:
            _logme.log('Job not done, attempting to get current STDOUT ' +
                       'anyway', 'info')
        _logme.log('Getting stdout from {}'.format(self._kwargs['outfile']),
                   'debug')
        if _os.path.isfile(self._kwargs['outfile']):
            with open(self._kwargs['outfile']) as fin:
                stdout = fin.read()
            if stdout:
                stdouts = stdout.strip().split('\n')
                if len(stdouts) < 3 or stdouts[-3] != 'Done':
                    _logme.log('STDOUT incomplete, returning as is', 'info')
                    return stdout
                if self.done:
                    self.get_times(update=False, stdout=stdout)
                    self.get_exitcode(update=False, stdout=stdout)
                stdout  = '\n'.join(stdouts[2:-3]) + '\n'
            if delete_file is True or self.clean_files is True:
                _logme.log('Deleting {}'.format(self._kwargs['outfile']),
                           'debug')
                _os.remove(self._kwargs['outfile'])
            if save:
                self._stdout = stdout
                if self.done:
                    self._got_stdout = True
            return stdout
        else:
            _logme.log('No file at {}, cannot get stdout'
                       .format(self._kwargs['outfile']), 'warn')
            return None

    def get_stderr(self, save=True, delete_file=None, update=True):
        """Get stderr of function or script, same for both.

        By default, output file is kept unless delete_file is True or
        self.clean_files is True.

        Parameters
        ----------
        save : bool, optional
            Save the output to self.stdout, default True.  Would be a good idea
            to set to False if the output is huge.
        delete_file : bool, optional
            Delete the stdout file when getting
        update : bool, optional
            Update job info from queue first.

        Returns
        -------
        str
            The contents of STDERR, with trailing newline removed.
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
                self._wait_for_files()
        else:
            _logme.log('Job not done, attempting to get current STDERR ' +
                       'anyway', 'info')
        _logme.log('Getting stderr from {}'.format(self._kwargs['errfile']),
                   'debug')
        if _os.path.isfile(self._kwargs['errfile']):
            with open(self._kwargs['errfile']) as fin:
                stderr = fin.read()
            if delete_file is True or self.clean_files is True:
                _logme.log('Deleting {}'.format(self._kwargs['errfile']),
                           'debug')
                _os.remove(self._kwargs['errfile'])
            if save:
                self._stderr = stderr
                if self.done:
                    self._got_stderr = True
            return stderr
        else:
            _logme.log('No file at {}, cannot get stderr'
                       .format(self._kwargs['errfile']), 'warn')
            return None

    def get_times(self, update=True, stdout=None):
        """Get stdout of function or script, same for both.

        Sets self.start and self.end from the contents of STDOUT if
        possible.

        Parameters
        ----------
        update : bool, optional
            Update job info from queue first.
        stdout : str, optional
            Pass existing stdout for use

        Returns
        -------
        start : datetime.datetime
        end : datetime.datetime
        """
        _logme.log('Getting times', 'debug')
        if self.done and self._got_times:
            _logme.log('Getting times from self.start, self.end', 'debug')
            return self.start, self.end
        if update and not self._updating and not self.done:
            self.update()
        if self.done:
            if update:
                self._wait_for_files()
        else:
            _logme.log('Cannot get times until job is complete.', 'warn')
            return None, None
        _logme.log('Getting times from {}'.format(self._kwargs['outfile']),
                   'debug')
        if not stdout:
            if _os.path.isfile(self._kwargs['outfile']):
                with open(self._kwargs['outfile']) as fin:
                    stdout = fin.read()
            else:
                _logme.log('No file at {}, cannot get times'
                           .format(self._kwargs['outfile']), 'warn')
                return None
        stdouts = stdout.strip().split('\n')
        if len(stdouts) < 3 or stdouts[-3] != 'Done':
            _logme.log('STDOUT incomplete, cannot get times', 'warn')
            return None

        # Get times
        timefmt = '%y-%m-%d-%H:%M:%S'
        try:
            self.start = _dt.strptime(stdouts[0], timefmt)
            self.end   = _dt.strptime(stdouts[-1], timefmt)
        except ValueError as err:
            _logme.log('Time parsing failed with value error; ' +
                       '{}. '.format(err) + 'This may be because you ' +
                       'are using the script running that does not ' +
                       'include time tracking', 'debug')
        self._got_times = True
        return self.start, self.end

    def get_exitcode(self, update=True, stdout=None):
        """Try to get the exitcode.

        Parameters
        ----------
        update : bool, optional
            Update job info from queue first.
        stdout : str, optional
            Pass existing stdout for use

        Returns
        -------
        exitcode : int
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
        if self.state == 'disappeared':
            _logme.log('Cannot get exitcode for disappeared job', 'debug')
            return 0

        code = None

        if not stdout and _os.path.isfile(self._kwargs['outfile']):
            with open(self._kwargs['outfile']) as fin:
                stdout = fin.read()

        if stdout:
            stdouts = stdout.strip().split('\n')
            if len(stdouts) > 3 and stdouts[-3] == 'Done':
                if stdouts[-2].startswith('Code: '):
                    code = int(stdouts[-2].split(':')[-1].strip())

        if code is None:
            _logme.log('Getting exitcode from queue', 'debug')
            if not self.queue_info:
                self.queue_info = self.queue[self.id]
            if hasattr(self.queue_info, 'exitcode'):
                code = self.queue_info.exitcode

        if code is None:
            _logme.log('Failed to get exitcode for job', 'warn')
            return None

        self._exitcode     = code
        self._got_exitcode = True

        if code != 0:
            self.state = 'failed'
            _logme.log('Job {} failed with exitcode {}'
                       .format(self.name, code), 'error')

        return code

    def fetch_outputs(self, save=True, delete_files=None, get_stats=True):
        """Save all outputs in their current state. No return value.

        This method does not wait for job completion, but merely gets the
        outputs. To wait for job completion, use `get()` instead.

        Parameters
        ----------
        save : bool, optional
            Save all outputs to the class also (advised)
        delete_files : bool, optional
            Delete the output files when getting, only used if save is True
        get_stats : bool, optional
            Try to get exitcode.
        """
        _logme.log('Saving outputs to self, delete_files={}'
                   .format(delete_files), 'debug')
        if not self._updating:
            self.update()
        if delete_files is None:
            delete_files = self.clean_outputs
        if not self._got_exitcode and get_stats:
            self.get_exitcode(update=False)
        if not self._got_times:
            self.get_times(update=False)
        if save:
            self.get_output(save=True, delete_file=delete_files, update=False)
            self.get_stdout(save=True, delete_file=delete_files, update=False)
            self.get_stderr(save=True, delete_file=delete_files, update=False)

    ##############################
    #  Minor management methods  #
    ##############################

    def get_keywords(self):
        """Return a list of the keyword arguments used to make the job."""
        return self.kwds

    def set_keywords(self, kwds, replace=False):
        """Set the job keywords, just updates self.kwds.

        Parameters
        ----------
        kwds : dict
            Set of valid arguments.
        replace : bool, optional
            Overwrite the keword arguments instead of updating.
        """
        kwds = _options.check_arguments(kwds)
        if replace:
            self.kwds = kwds
        else:
            for key, value in kwds.items():
                self.kwds[key] = value

    ###############
    #  Internals  #
    ###############

    def _update(self, fetch_info=True):
        """Update status from the queue.

        Parameters
        ----------
        fetch_info : bool, optional
            Fetch basic job info if complete.
        """
        _logme.log('Updating job.', 'debug')
        self._updating = True
        if self.done or not self.submitted:
            self._updating = False
            return
        self.queue.update()
        if self.submitted and self.id:
            queue_info = self.queue[self.id]
            if queue_info:
                assert self.id == queue_info.id
                self.found = True
                self.queue_info = queue_info
                self.state = self.queue_info.state
            elif self.found:
                _logme.log('Job appears to have disappeared, waiting for '
                           'reappearance, this may take a while', 'warn')
                status = self.wait()
                if status == 'disappeared':
                    _logme.log('Job disappeared, but the output files are '
                               'present assuming completion', 'info')
                    self.state = 'completed'
                    self.disappeared = True
                elif not status:
                    _logme.log('Job appears to have failed and disappeared',
                               'error')
            # If job not found after 30 seconds, assume trouble, check for
            # completion
            elif self.submitted and (_dt.now()-self.submit_time).seconds > 360:
                if self._wait_for_files(btme=4, caution_message=False):
                    self.state = 'completed'
                    self.disappeared = True
                    _logme.log('Job never appeared in the queue, but '
                               'outfiles still exist, assuming completion.',
                               'warn')
                else:
                    self.state = 'failed'
                    self.disappeared = True
                    s = (_dt.now()-self.submit_time).seconds
                    _logme.log('Job not in queue after {} seconds '.format(s) +
                               'of searching and no outputs found, assuming '
                               'failure.', 'error')
            elif self.submitted and (_dt.now()-self.submit_time).seconds > 30:
                if self._wait_for_files(btme=1, caution_message=False):
                    self.state = 'completed'
                    self.disappeared = True
                    _logme.log('Job never appeared in the queue, but '
                               'outfiles still exist, assuming completion.',
                               'warn')
        if self.done and fetch_info:
            if self._wait_for_files(btme=1, caution_message=False):
                if not self._got_exitcode:
                    self.get_exitcode(update=False)
                if not self._got_times:
                    self.get_times(update=False)
        self._updating = False

    def _wait_for_files(self, btme=None, caution_message=False):
        """Block until files appear up to 'file_block_time' in config file.

        Aborts after 2 seconds if job exit code is not 0.

        Parameters
        ----------
        btme : int, optional
            Number of seconds to try for before giving up, default set in
            config file.
        caution_message : bool, optional
            Display a message if this is taking a while.

        Returns
        -------
        bool
            True if files found
        """
        if self._found_files:
            _logme.log('Already found files, not waiting again', 'debug')
            return True
        wait_time = 0.1 # seconds
        if btme:
            lvl = 'debug'
        else:
            lvl = 'warn'
            btme = _conf.get_option('jobs', 'file_block_time', 30)
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
                _logme.log('Job files have not appeared for ' +
                           '>{} seconds'.format(btme), lvl)
                return False
            if not self._updating:
                self.update()
            if runtime > 2 and self.get_exitcode(update=False) != 0:
                _logme.log('Job failed with exit code {}.'
                           .format(self.exitcode) + ' Cannot find files.',
                           'error')
                return False
        self._found_files = True
        return True

    def _update_name(self, name=None):
        """Make sure the job name is unique.

        Sets
        ----
        self.name

        Parameters
        ----------
        name : str, optional
            A name override, if no provided self.name used

        Returns
        -------
        name : str
        """
        # Set name
        name = name if name else self.name
        if not name:
            if callable(self.command):
                strcmd = str(self.command).strip('<>')
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
                name = self.command.split(' ')[0].split('/')[-1]

        # Make sure name not in queue
        if '.' not in name or not name.split('.')[-1] == self.uuid:
            name = '{}.{}'.format(name, self.uuid)

        self.name = name

        return name

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
        if not self._updating:
            self.update()
        return "Job: {name} ID: {id}, state: {state}".format(
            name=self.name, id=self.id, state=self.state)

    def __int__(self):
        """Return integer of ID."""
        if self.id:
            if str(self.id.isdigit()):
                return int(id)
        _logme.log('No ID yet.', 'error')
        return 0
