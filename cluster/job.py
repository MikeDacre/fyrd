"""
Submit jobs to slurm or torque, or with multiprocessing.

===============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2016-04-20 23:03
 Last modified: 2016-06-15 13:31

===============================================================================
"""
import os
import sys
import inspect
from time import sleep
from types import ModuleType
from subprocess import CalledProcessError

# Try to use dill, revert to pickle if not found
try:
    import dill as pickle
except ImportError:
    try:
        import cPickle as pickle # For python2
    except ImportError:
        import pickle

###############################################################################
#                                Our functions                                #
###############################################################################

from . import run
from . import logme
from . import queue
from . import options
from . import config_file
from . import ClusterError

##########################################################
#  The multiprocessing pool, only used in 'local' mode  #
##########################################################

from . import jobqueue
from . import THREADS

__all__ = ['Job', 'submit', 'make_job_file', 'clean', 'submit_file',
           'clean_dir']

###############################################################################
#                                The Job Class                                #
###############################################################################


class Job(object):

    """Information about a single job on the cluster.

    Holds information about submit time, number of cores, the job script,
    and more.

    submit() will submit the job if it is ready
    wait()   will block until the job is done
    get()    will block until the job is done and then unpickle a stored
             output (if defined) and return the contents
    clean()  will delete any files created by this object

    Printing the class will display detailed job information.

    Both wait() and get() will update the queue every two seconds and add
    queue information to the job as they go.

    If the job disappears from the queue with no information, it will be listed
    as 'complete'.

    All jobs have a .submission attribute, which is a Script object containing
    the submission script for the job and the file name, plus a 'written' bool
    that checks if the file exists.

    In addition, SLURM jobs have a .exec_script attribute, which is a Script
    object containing the shell command to run. This difference is due to the
    fact that some SLURM systems execute multiple lines of the submission file
    at the same time.

    Finally, if the job command is a function, this object will also contain a
    .function attribute, which contains the script to run the function.

    """

    id           = None
    submitted    = False
    written      = False
    done         = False

    # Holds a pool object if we are in local mode
    pool_job     = None

    # Scripts
    submission   = None
    exec_script  = None
    function     = None

    # Dependencies
    dependencies = None

    # Holds queue information in torque and slurm
    queue_info   = None

    stdout = stderr = None

    def __init__(self, command, args=None, name=None, path=None,
                 qtype=None, profile=None, **kwds):
        """Create a job object will submission information.

        :command: The command or function to execute.
        :args:    Optional arguments to add to command, particularly
                  useful for functions.
        :name:    Optional name of the job. If not defined, guessed. If a job
                  of the same name is already queued, an integer job number
                  (not the queue number) will be added, ie. <name>.1
        :path:    Where to create the script, if None, current dir used.
        :qtype:   Override the default queue type
        :profile: An optional profile to define options

        Available Keyword Arguments
        ---------------------------

        There are many keyword arguments available for cluster job submission.
        These vary somewhat by queue type. For info run:
            cluster.options.option_help()
        """

        ########################
        #  Sanitize arguments  #
        ########################

        # Make a copy of the keyword arguments, as we will delete arguments
        # as we go
        kwargs = options.check_arguments(kwds.copy())

        # Save command
        self.command = command
        self.args    = args

        # Merge in profile
        if profile:
            # This is a Profile() object, the arguments are in the args dict
            prof = config_file.get_profile(profile)
            if prof:
                for k,v in prof.args.items():
                    if k not in kwargs:
                        kwargs[k] = v
            else:
                logme.log('No profile found for {}'.format(profile), 'warn')

        # If no profile or keywords, use default profile, args is a dict
        default_args = config_file.get_profile('default').args
        if not profile and not kwargs:
            kwargs = default_args

        # Get required options
        req_options = config_file.get('opts')
        for k,v in req_options.items():
            if k not in kwargs:
                kwargs[k] = v

        # Get environment
        self.qtype = qtype if qtype else queue.MODE
        self.queue = queue.Queue(user='self', queue=self.qtype)
        self.state = 'Not Submitted'

        # Set name
        if not name:
            if hasattr(command, '__call__'):
                parts = str(command).strip('<>').split(' ')
                parts.remove('function')
                try:
                    parts.remove('built-in')
                except ValueError:
                    pass
                name = parts[0]
            else:
                name = command.split(' ')[0].split('/')[-1]

        # Make sure name not in queue
        self.queue.update()
        names   = [i.name.split('.')[0] for i in self.queue]
        namecnt = len([i for i in names if i == name])
        name = '{}.{}'.format(name, namecnt)
        self.name = str(name)

        # Set modules
        self.modules = kwargs.pop('modules') if 'modules' in kwargs else None
        if self.modules:
            self.modules = run.opt_split(self.modules, (',', ';'))

        # Path handling
        if path:
            usedir = os.path.abspath(path)
        elif 'dir' in kwargs:
            usedir = os.path.abspath(kwargs['dir'])
        else:
            usedir = os.path.abspath('.')

        # Set runtime dir in arguments
        if 'dir' not in kwargs:
            kwargs['dir'] = usedir

        # Set temp file path if different from runtime path
        filedir = os.path.abspath(kwargs['filedir']) \
            if 'filedir' in kwargs else usedir

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
        suffix = kwargs.pop('suffix') if 'suffix' in kwargs else 'cluster'
        if 'outfile' not in kwargs:
            kwargs['outfile'] = os.path.join(filedir,
                                             '.'.join([name, suffix, 'out']))
        if 'errfile' not in kwargs:
            kwargs['errfile'] = os.path.join(filedir,
                                             '.'.join([name, suffix, 'err']))
        self.outfile = kwargs['outfile']
        self.errfile = kwargs['errfile']

        # Check and set dependencies
        if 'depends' in kwargs:
            dependencies = kwargs.pop('depends')
            self.dependencies = []
            if isinstance(dependencies, 'str'):
                if not dependencies.isdigit():
                    raise ClusterError('Dependencies must be number or list')
                else:
                    dependencies = [int(dependencies)]
            elif isinstance(dependencies, (int, Job)):
                dependencies = [dependencies]
            elif not isinstance(dependencies, (tuple, list)):
                raise ClusterError('Dependencies must be number or list')
            for dependency in dependencies:
                if isinstance(dependency, str):
                    dependency  = int(dependency)
                if not isinstance(dependency, (int, Job)):
                    raise ClusterError('Dependencies must be number or list')
                self.dependencies.append(dependency)

        ######################################
        #  Command and Function Preparation  #
        ######################################

        # Make functions run remotely
        if hasattr(command, '__call__'):
            self.function = Function(
                file_name=os.path.join(filedir, name + '_func.py'),
                function=command, args=args)
            # Collapse the command into a python call to the function script
            command = 'python{} {}'.format(sys.version[0],
                                           self.function.file_name)
            args = None

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
            scrpt = os.path.join(filedir, '{}.cluster.sbatch'.format(name))

            # We use a separate script and a single srun command to avoid
            # issues with multiple threads running at once
            exe_scrpt  = os.path.join(filedir, name + '.script')
            exe_script = run.CMND_RUNNER_TRACK.format(
                precmd=precmd, usedir=usedir, name=name, command=command)

            # Add all of the keyword arguments at once
            precmd += options.options_to_string(kwargs, self.qtype)

            ecmnd = 'srun bash {}.script'.format(exe_scrpt)
            sub_script = run.SCRP_RUNNER.format(precmd=precmd,
                                                script=exe_script,
                                                command=ecmnd)

        elif self.qtype == 'torque':
            scrpt = os.path.join(filedir, '{}.cluster.qsub'.format(name))

            # Add all of the keyword arguments at once
            precmd += options.options_to_string(kwargs, self.qtype)

            sub_script = run.CMND_RUNNER_TRACK.format(
                precmd=precmd, usedir=usedir, name=name, command=command)

        elif self.qtype == 'local':
            # Create the pool
            if not jobqueue.JQUEUE or not jobqueue.JQUEUE.runner.is_alive():
                threads = kwargs['threads'] if 'threads' in kwargs else THREADS
                jobqueue.JQUEUE = jobqueue.JobQueue(cores=threads)

            # Add all of the keyword arguments at once
            precmd += options.options_to_string(kwargs, self.qtype)

            scrpt = os.path.join(filedir, '{}.cluster'.format(name))
            sub_script = run.CMND_RUNNER_TRACK.format(
                precmd=precmd, usedir=usedir, name=name, command=command)

        # Create the Script objects
        self.submission = Script(script=sub_script,
                                 file_name=scrpt)
        if hasattr(self, 'exe_scrpt'):
            self.exec_script = Script(script='\n'.join(exe_script),
                                      file_name=exe_scrpt)

        self.kwargs = kwargs

    ####################
    #  Public Methods  #
    ####################

    def write(self, overwrite=True):
        """Write all scripts."""
        self.submission.write(overwrite)
        if self.exec_script:
            self.exec_script.write(overwrite)
        if self.function:
            self.function.write(overwrite)
        self.written = True

    def clean(self):
        """Delete all scripts created by this module, if they were written."""
        self.get_stdout()
        self.get_stderr()
        for jobfile in [self.submission, self.exec_script, self.function]:
            if jobfile:
                jobfile.clean()

    def submit(self, max_queue_len=None):
        """Submit this job.

        If max_queue_len is specified (or in defaults), then this method will
        block until the queue is open enough to allow submission.

        To disable max_queue_len, set it to 0. None will allow override by
        the default settings in the config file, and any positive integer will
        be interpretted to be the maximum queue length.

        NOTE: In local mode, dependencies will result in this function blocking
              until the dependencies are satisfied, not idea behavior.

        Returns self.
        """
        if self.submitted:
            sys.stderr.write('Already submitted.')
            return

        if not isinstance(max_queue_len, (type(None), int)):
            raise Exception('max_queue_len must be int or None, is {}'
                            .format(type(max_queue_len)))

        if not self.written:
            self.write()

        self.update()

        if max_queue_len is not 0:
            self.queue.wait_to_submit(max_queue_len)

        if self.qtype == 'local':
            # Normal mode dependency tracking uses only integer job numbers
            dependencies = []
            if self.dependencies:
                for depend in self.dependencies:
                    if isinstance(depend, Job):
                        dependencies.append(int(depend.id))
                    else:
                        dependencies.append(int(depend))

            command = 'bash {}'.format(self.submission.file_name)
            #  kwargs  = dict(stdout=self.stdout,
                           #  stderr=self.stderr)

            # Make sure the global job pool exists
            if not jobqueue.JQUEUE or not jobqueue.JQUEUE.runner.is_alive():
                jobqueue.JQUEUE = jobqueue.JobQueue(cores=self.cores)
            self.id = jobqueue.JQUEUE.add(run.cmd, args=(command,),
                                          #  kwargs=kwargs,
                                          dependencies=dependencies)
            self.submitted = True
            return self

        elif self.qtype == 'slurm':
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
            code, stdout, stderr = run.cmd(args, tries=5)
            if code == 0:
                self.id = int(stdout.split(' ')[-1])
            else:
                logme.log('sbatch failed with code {}\n'.format(code),
                          'stdout: {}\nstderr: {}'.format(stdout, stderr),
                          'critical')
                raise CalledProcessError(code, args, stdout, stderr)
            self.submitted = True
            return self

        elif self.qtype == 'torque':
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
            code, stdout, stderr = run.cmd(args, tries=5)
            if code == 0:
                self.id = int(stdout.split('.')[0])
            else:
                if stderr.startswith('qsub: submit error ('):
                    raise ClusterError('qsub submission failed with error: ' +
                                       '{}, command: {}'.format(stderr, args))
                else:
                    logme.log(('qsub failed with code {}\n'
                               'stdout: {}\nstderr: {}')
                              .format(code, stdout, stderr),
                              'critical')
                    raise CalledProcessError(code, args, stdout, stderr)
                    logme.log('qsub failed with err {}. Resubmitting.'
                              .format(stderr), 'debug')
            self.submitted = True
            return self

    def update(self):
        """Update status from the queue."""
        if not self.submitted:
            return
        self.queue.update()
        if self.id:
            queue_info = self.queue[self.id]
            if queue_info:
                assert self.id == queue_info.id
                self.queue_info = queue_info
                self.state = self.queue_info.state
                if self.state == 'complete':
                    self.done = True
        return self

    def wait(self):
        """Block until job completes."""
        self.update()
        if self.done:
            return
        self.queue.wait(self)
        self.done = True

    def get(self):
        """Block until job completed and return exit_code, stdout, stderr."""
        self.wait()
        if self.qtype == 'local':
            return jobqueue.JQUEUE.get(self.id)
        else:
            return self.queue_info.exitcode, self.stdout, self.stderr

    def get_stdout(self):
        """Read stdout file if exists and set self.stdout, return it."""
        if not self.done:
            self._update()
        if self.done and hasattr(self, '_stdout'):
            return self._stdout
        if os.path.isfile(self.kwargs['outfile']):
            stdout = open(self.kwargs['outfile']).read()
            if stdout:
                stdout = '\n'.join(stdout.split('\n')[2:-3]) + '\n'
            if self.done:
                self._stdout = stdout
                self.stdout  = self._stdout
            return stdout

    def get_stderr(self):
        """Read stdout file if exists and set self.stdout, return it."""
        if not self.done:
            self._update()
        if self.done and hasattr(self, '_stderr'):
            return self._stderr
        if os.path.isfile(self.kwargs['errfile']):
            stderr = open(self.kwargs['errfile']).read()
            if self.done:
                self._stderr = stderr
                self.stderr  = self._stderr
            return stderr

    ###############
    #  Internals  #
    ###############

    def __getattribute__(self, key):
        """Make some attributes fully dynamic."""
        if key == 'done':
            if not object.__getattribute__(self, 'done'):
                self.update()
        elif key == 'files':
            files = [self.submission]
            if self.exec_script:
                files.append(self.exec_script)
            if self.function:
                files.append(self.function)
            return files
        elif key == 'stdout':
            return self.get_stdout()
        elif key == 'stderr':
            return self.get_stderr()
        return object.__getattribute__(self, key)

    def __repr__(self):
        """Return simple job information."""
        self.update()
        outstr = "Job:{name}<{mode}".format(name=self.name, mode=self.qtype)
        if self.submitted:
            outstr += ':{}'.format(self.id)
        outstr += "(command:{cmnd};args:{args})".format(
            cmnd=self.command, args=self.args)
        if self.done:
            outstr += "COMPLETED"
        elif self.written:
            outstr += "WRITTEN"
        else:
            outstr += "NOT_SUBMITTED"
        outstr += ">"
        return outstr

    def __str__(self):
        """Print job name and ID + status."""
        self.update()
        if self.done:
            state = 'complete'
            id1 = self.id
        elif self.written:
            state = 'written'
            id1 = self.id
        else:
            state = 'not written'
            id1 = 'NA'
        return "{name} ID: {id}, state: {state}".format(
            name=self.name, id=id1, state=state)


class Script(object):

    """A script string plus a file name."""

    written = False

    def __init__(self, file_name, script):
        """Initialize the script and file name."""
        self.script    = script
        self.file_name = os.path.abspath(file_name)

    def write(self, overwrite=True):
        """Write the script file."""
        if overwrite or not os.path.exists(self.file_name):
            with open(self.file_name, 'w') as fout:
                fout.write(self.script + '\n')
            self.written = True
            return self.file_name
        else:
            return None

    def clean(self):
        """Delete any files made by us."""
        if self.written and self.exists:
            os.remove(self.file_name)

    def __getattr__(self, attr):
        """Make sure boolean is up to date."""
        if attr == 'exists':
            return os.path.exists(self.file_name)

    def __repr__(self):
        """Display simple info."""
        return "Script<{}(exists: {}; written: {})>".format(
            self.file_name, self.exists, self.written)

    def __str__(self):
        """Print the script."""
        return repr(self) + '::\n\n' + self.script + '\n'


class Function(Script):

    """A special Script used to run a function."""

    def __init__(self, file_name, function, args=None, imports=None,
                 pickle_file=None, outfile=None):
        """Create a function wrapper.

        :function:    Function handle.
        :args:        Arguments to the function as a tuple.
        :imports:     A list of imports, if not provided, defaults to all current
                      imports, which may not work if you use complex imports.
                      The list can include the import call, or just be a name, e.g
                      ['from os import path', 'sys']
        :pickle_file: The file to hold the function.
        :outfile:     The file to hold the output.
        :path:        The path to the calling script, used for importing self.
        """
        self.function = function
        self.parent   = inspect.getmodule(self.function).__name__
        self.args     = args

        # Get the module path
        rootmod  = inspect.getmodule(function)
        if hasattr(rootmod, '__file__'):
            imppath = os.path.split(rootmod.__file__)[0]
        else:
            imppath = '.'
        rootname = rootmod.__name__

        # Try to set a sane import string to make the function work
        modstr = 'import {}'.format(rootname)
        if rootname != self.function.__name__:
            modstr  = 'try:\n    import {}.{}\n'.format(
                rootname, self.function.__name__)
            modstr += 'except ImportError:\n    pass\n'
            modstr += 'from {} import {}'.format(
                rootname, self.function.__name__)

        # Take care of imports, either use manual or all current imports
        if imports:
            if not isinstance(imports, (list, tuple)):
                imports = [imports]
        else:
            imports = []
            for module in globals().values():
                if isinstance(module, ModuleType):
                    imports.append(module.__name__)
            imports = list(set(imports))

        # Create a sane set of imports
        filtered_imports = []
        for imp in imports:
            if imp.startswith('import') or imp.startswith('from'):
                filtered_imports.append(imp.rstrip())
            else:
                if '.' in imp:
                    rootimp = imp.split('.')[0]
                    if not rootimp == 'pickle' and not rootimp == 'sys':
                        filtered_imports.append('import {}'.format(rootimp))
                if imp == 'pickle' or imp == 'sys':
                    continue
                filtered_imports.append(('try:\n    import {}\n'
                                         'except ImportError:\n    pass\n')
                                        .format(imp))
        # Get rid of duplicates and sort imports
        impts = '\n'.join(sorted(set(filtered_imports)))

        # Set file names
        self.pickle_file = pickle_file if pickle_file else file_name + '.pickle.in'
        self.outfile     = outfile if outfile else file_name + '.pickle.out'

        # Create script text
        script = '#!/usr/bin/env python{}\n'.format(sys.version[0])
        script += run.FUNC_RUNNER.format(path=imppath,
                                         modimpstr=modstr,
                                         imports=impts,
                                         pickle_file=self.pickle_file,
                                         out_file=self.outfile)

        super(Function, self).__init__(file_name, script)

    def write(self, overwrite=True):
        """Write the pickle file and call the parent Script write function."""
        with open(self.pickle_file, 'wb') as fout:
            pickle.dump((self.function, self.args), fout)
        super(Function, self).write(overwrite)

    def clean(self):
        """Delete any files made by us."""
        if self.written:
            if os.path.isfile(self.pickle_file):
                os.remove(self.pickle_file)
            if os.path.isfile(self.outfile):
                os.remove(self.outfile)
        super(Function, self).clean()


###############################################################################
#                            Submission Functions                             #
###############################################################################


def submit(command, args=None, name=None, path=None, qtype=None,
           profile=None, **kwargs):
    """Submit a script to the cluster.

    Used in all modes::
    :command:   The command or function to execute.
    :args:      Optional arguments to add to command, particularly
                useful for functions.
    :name:      The name of the job.
    :path:      Where to create the script, if None, current dir used.

    Available Keyword Arguments
    ---------------------------

    There are many keyword arguments available for cluster job submission.
    These vary somewhat by queue type. For info run:
        cluster.options.option_help()

    Returns:
        Job object
    """

    queue.check_queue()  # Make sure the queue.MODE is usable

    job = Job(command=command, args=args, name=name, path=path, qtype=qtype,
              profile=profile, **kwargs)

    job.write()
    job.submit()

    return job


#########################
#  Job file generation  #
#########################


def make_job(command, args=None, name=None, path=None, qtype=None,
             profile=None, **kwargs):
    """Make a job file compatible with the chosen cluster.

    If mode is local, this is just a simple shell script.

     Used in all modes::
    :command:   The command or function to execute.
    :args:      Optional arguments to add to command, particularly
                useful for functions.
    :name:      The name of the job.
    :path:      Where to create the script, if None, current dir used.

    Available Keyword Arguments
    ---------------------------

    There are many keyword arguments available for cluster job submission.
    These vary somewhat by queue type. For info run:
        cluster.options.option_help()

    Returns:
        A job object
    """

    queue.check_queue()  # Make sure the queue.MODE is usable

    job = Job(command=command, args=args, name=name, path=path, qtype=qtype,
              profile=profile, **kwargs)

    # Return the path to the script
    return job


def make_job_file(command, args=None, name=None, path=None, qtype=None,
                  profile=None, **kwargs):
    """Make a job file compatible with the chosen cluster.

    If mode is local, this is just a simple shell script.

     Used in all modes::
    :command:   The command or function to execute.
    :args:      Optional arguments to add to command, particularly
                useful for functions.
    :name:      The name of the job.
    :path:      Where to create the script, if None, current dir used.

    Available Keyword Arguments
    ---------------------------

    There are many keyword arguments available for cluster job submission.
    These vary somewhat by queue type. For info run:
        cluster.options.option_help()

    Returns:
        Path to job script
    """

    queue.check_queue()  # Make sure the queue.MODE is usable

    job = Job(command=command, args=args, name=name, path=path, qtype=qtype,
              profile=profile, **kwargs)

    job = job.write()

    # Return the path to the script
    return job.submission


##############
#  Cleaning  #
##############


def clean(jobs):
    """Delete all files in jobs list or single Job object."""
    if isinstance(jobs, Job):
        jobs = [jobs]
    if not isinstance(jobs, (list, tuple)):
        raise ClusterError('Job list must be a Job, list, or tuple')
    for job in jobs:
        job.clean()


###############################################################################
#                      Job Object Independent Functions                       #
###############################################################################


def submit_file(script_file, dependencies=None, threads=None, qtype=None):
    """Submit a job file to the cluster.

    If qtype or queue.MODE is torque, qsub is used; if it is slurm, sbatch
    is used; if it is local, the file is executed with subprocess.

    This function is independent of the Job object and just submits a file.

    :dependencies: A job number or list of job numbers.
                   In slurm: `--dependency=afterok:` is used
                   For torque: `-W depend=afterok:` is used

    :threads:      Total number of threads to use at a time, defaults to all.
                   ONLY USED IN LOCAL MODE

    :returns:      job number for torque or slurm
                   multiprocessing job object for local mode
    """
    queue.check_queue()  # Make sure the queue.MODE is usable

    # Sanitize arguments
    name = str(name)

    if not qtype:
        qtype = queue.get_cluster_environment()

    # Check dependencies
    if dependencies:
        if isinstance(dependencies, (str, int)):
            dependencies = [dependencies]
        if not isinstance(dependencies, (list, tuple)):
            raise Exception('dependencies must be a list, int, or string.')
        dependencies = [str(i) for i in dependencies]

    if qtype == 'slurm':
        if dependencies:
            dependencies = '--dependency=afterok:{}'.format(
                ':'.join([str(d) for d in dependencies]))
            args = ['sbatch', dependencies, script_file]
        else:
            args = ['sbatch', script_file]
        # Try to submit job 5 times
        count = 0
        while True:
            code, stdout, stderr = run.cmd(args)
            if code == 0:
                job = int(stdout.split(' ')[-1])
                break
            else:
                if count == 5:
                    logme.log('sbatch failed with code {}\n'.format(code),
                              'stdout: {}\nstderr: {}'.format(stdout, stderr),
                              'critical')
                    raise CalledProcessError(code, args, stdout, stderr)
                logme.log('sbatch failed with err {}. Resubmitting.'.format(
                    stderr), 'debug')
                count += 1
                sleep(1)
                continue
            break
        return job

    elif qtype == 'torque':
        if dependencies:
            dependencies = '-W depend={}'.format(
                ','.join(['afterok:' + d for d in dependencies]))
            args = ['qsub', dependencies, script_file]
        else:
            args = ['qsub', script_file]
        # Try to submit job 5 times
        count = 0
        while True:
            code, stdout, stderr = run.cmd(args)
            if code == 0:
                job = int(stdout.split('.')[0])
                break
            else:
                if count == 5:
                    logme.log('qsub failed with code {}\n'.format(code),
                              'stdout: {}\nstderr: {}'.format(stdout, stderr),
                              'critical')
                    raise CalledProcessError(code, args, stdout, stderr)
                logme.log('qsub failed with err {}. Resubmitting.'.format(
                    stderr), 'debug')
                count += 1
                sleep(1)
                continue
            break
        return job

    elif qtype == 'local':
        # Normal mode dependency tracking uses only integer job numbers
        depends = []
        if dependencies:
            for depend in dependencies:
                if isinstance(depend, Job):
                    depends.append(int(depend.id))
                else:
                    depends.append(int(depend))
        command = 'bash {}'.format(script_file)
        # Make sure the global job pool exists
        if not jobqueue.JQUEUE or not jobqueue.JQUEUE.runner.is_alive():
            jobqueue.JQUEUE = jobqueue.JobQueue(cores=threads)
        return jobqueue.JQUEUE.add(run.cmd, (command,), dependencies=depends)


def clean_dir(directory='.', suffix='cluster'):
    """Delete all files made by this module in directory.

    CAUTION: The clean() function will delete **EVERY** file with
             extensions matching those these::
                 .<suffix>.err
                 .<suffix>.out
                 .<suffix>.sbatch & .cluster.script for slurm mode
                 .<suffix>.qsub for torque mode
                 .<suffix> for local mode

    :directory: The directory to run in, defaults to the current directory.
    :returns:   A set of deleted files
    """
    queue.check_queue()  # Make sure the queue.MODE is usable

    extensions = ['.' + suffix + '.err', '.' + suffix + '.out']
    if queue.MODE == 'local':
        extensions.append('.' + suffix)
    elif queue.MODE == 'slurm':
        extensions = extensions + ['.' + suffix + '.sbatch',
                                   '.' + suffix + '.script']
    elif queue.MODE == 'torque':
        extensions.append('.' + suffix + '.qsub')

    files = [i for i in os.listdir(os.path.abspath(directory))
             if os.path.isfile(i)]

    if not files:
        logme.log('No files found.', 'debug')
        return []

    deleted = []
    for f in files:
        for extension in extensions:
            if f.endswith(extension):
                os.remove(f)
                deleted.append(f)

    return deleted
