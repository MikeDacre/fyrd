"""
Class and methods to handle Job submission.

Last modified: 2016-10-27 13:32
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

__all__ = ['Job', 'submit', 'make_job', 'make_job_file', 'submit_file',
           'clean', 'clean_dir']

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
    as 'completed'.

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

    stdout = stderr = exitcode = None

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
        :profile: The name of a profile saved in the config_file
        :kwargs:  Keyword arguments to control job options

        There are many keyword arguments available for cluster job submission.
        These vary somewhat by queue type. For info run:
            fyrd.options.option_help()
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
        req_options = config_file.get_option('opts')
        if req_options:
            for k,v in req_options.items():
                if k not in kwargs:
                    kwargs[k] = v

        # Get environment
        if not queue.MODE:
            queue.MODE = queue.get_cluster_environment()
        self.qtype = qtype if qtype else queue.MODE
        self.queue = queue.Queue(user='self', qtype=self.qtype)
        self.state = 'Not_Submitted'

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
        names     = [i.name.split('.')[0] for i in self.queue]
        namecnt   = len([i for i in names if i == name])
        self.name = '{}.{}'.format(name, namecnt)

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
                file_name=os.path.join(filedir, '{}_func.{}.py'.format(
                    name, suffix)
                ),
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
            scrpt = os.path.join(filedir, '{}.{}.sbatch'.format(name, suffix))

            # We use a separate script and a single srun command to avoid
            # issues with multiple threads running at once
            exec_script  = os.path.join(filedir, '{}.{}.script'.format(name,
                                                                       suffix))
            exe_script   = run.CMND_RUNNER_TRACK.format(
                precmd=precmd, usedir=usedir, name=name, command=command)
            # Create the exec_script Script object
            self.exec_script = Script(script=exe_script, file_name=exec_script)

            # Add all of the keyword arguments at once
            precmd += options.options_to_string(kwargs, self.qtype)

            ecmnd = 'srun bash {}'.format(exec_script)
            sub_script = run.SCRP_RUNNER.format(precmd=precmd,
                                                script=exec_script,
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
                threads = kwargs['threads'] if 'threads' in kwargs \
                        else jobqueue.THREADS
                jobqueue.JQUEUE = jobqueue.JobQueue(cores=threads)

            scrpt = os.path.join(filedir, '{}.cluster'.format(name))
            sub_script = run.CMND_RUNNER_TRACK.format(
                precmd=precmd, usedir=usedir, name=name, command=command)

        else:
            raise ClusterError('Invalid queue type')

        # Create the submission Script object
        self.submission = Script(script=sub_script,
                                 file_name=scrpt)

        # Save the keyword arguments for posterity
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

    def clean(self, delete_outputs=False):
        """Delete all scripts created by this module, if they were written.

        If delete_outputs is True, also delete the stdout and stderr files,
        but get their contents first.
        """
        for jobfile in [self.submission, self.exec_script, self.function]:
            if jobfile:
                jobfile.clean()
        if delete_outputs:
            self.get_stdout()
            self.get_stderr()
            for f in [self.outfile, self.errfile]:
                if os.path.isfile(f):
                    os.remove(f)

    def submit(self, max_queue_len=None):
        """Submit this job.

        :max_queue_len: if specified (or in defaults), then this method will
                        block until the queue is open enough to allow
                        submission.

        To disable max_queue_len, set it to 0. None will allow override by
        the default settings in the config file, and any positive integer will
        be interpretted to be the maximum queue length.

        :returns: self
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
            fileargs  = dict(stdout=self.outfile,
                             stderr=self.errfile)

            # Make sure the global job pool exists
            if not jobqueue.JQUEUE or not jobqueue.JQUEUE.runner.is_alive():
                jobqueue.JQUEUE = jobqueue.JobQueue(cores=jobqueue.THREADS)
            self.id = jobqueue.JQUEUE.add(run.cmd, args=(command,),
                                          kwargs=fileargs,
                                          dependencies=dependencies,
                                          cores=self.cores)
            self.submitted = True
            self.state = 'submitted'

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
            self.state = 'submitted'

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
            self.submitted = True
            self.state = 'submitted'
        else:
            raise ClusterError("Invalid queue type {}".format(self.qtype))

        if not self.submitted:
            raise ClusterError('Submission appears to have failed, this '
                               "shouldn't happen")

        sleep(0.5)  # Give submission a chance
        return self

    def update(self):
        """Update status from the queue."""
        if object.__getattribute__(self, 'done') or not self.submitted:
            return
        self.queue.update()
        if self.id:
            queue_info = self.queue[self.id]
            if queue_info:
                assert self.id == queue_info.id
                self.queue_info = queue_info
                self.state = self.queue_info.state
                if self.state == 'completed':
                    self.done = True
                    self.get_stdout(False)
                    self.get_stderr(False)
                    self.get_exitcode(False)
        return self

    def wait(self):
        """Block until job completes."""
        self.update()
        if self.done:
            return
        sleep(1)
        self.queue.wait(self)
        self.done = True

    def get(self):
        """Block until job completed and return exit_code, stdout, stderr."""
        if not self.submitted:
            logme.log('Cannot wait for result as job has not been submitted',
                      'warn')
            return
        sleep(0.2)
        self.wait()
        return self.get_exitcode(), self.get_stdout(), self.get_stderr()

    def get_stdout(self, update=True):
        """Read stdout file if exists and set self.stdout, return it."""
        if update and not object.__getattribute__(self, 'done'):
            self.update()
        if object.__getattribute__(self, 'done') and hasattr(self, '_stdout'):
            return self._stdout
        if os.path.isfile(self.kwargs['outfile']):
            stdout = open(self.kwargs['outfile']).read()
            if stdout:
                stdout = '\n'.join(stdout.split('\n')[2:-3]) + '\n'
            if object.__getattribute__(self, 'done'):
                self._stdout = stdout
                self.stdout  = self._stdout
            return stdout
        else:
            logme.log('No file at {}, cannot get stdout'
                      .format(self.kwargs['outfile']), 'debug')
            return -1

    def get_stderr(self, update=True):
        """Read stdout file if exists and set self.stdout, return it."""
        if update and not object.__getattribute__(self, 'done'):
            self.update()
        if object.__getattribute__(self, 'done') and hasattr(self, '_stderr'):
            return self._stderr
        if os.path.isfile(self.kwargs['errfile']):
            stderr = open(self.kwargs['errfile']).read()
            if object.__getattribute__(self, 'done'):
                self._stderr = stderr
                self.stderr  = self._stderr
            return stderr
        else:
            logme.log('No file at {}, cannot get stderr'
                      .format(self.kwargs['errfile']), 'debug')

    def get_exitcode(self, update=True):
        """Try to get the exitcode."""
        if update and not object.__getattribute__(self, 'done'):
            self.update()
        if not object.__getattribute__(self, 'done'):
            logme.log('Job is not complete, no exit code yet', 'info')
            return None
        if self.done and hasattr(self, '_exitcode'):
            return self._exitcode
        if not self.queue_info:
            self.queue_info = self.queue[self.id]
        if hasattr(self.queue_info, 'exitcode'):
            code = self.queue_info.exitcode
        else:
            code = None
            logme.log('No exitcode even though the job is done, this ' +
                      "shouldn't happen.", 'warn')
        self._exitcode = code
        self.exitcode  = self._exitcode
        return code

    def update_queue_info(self):
        """Set queue_info from the queue even if done."""
        queue_info1 = self.queue[self.id]
        self.queue.update()
        queue_info2 = self.queue[self.id]
        if queue_info2:
            self.queue_info = queue_info2
        elif queue_info1:
            self.queue_info = queue_info1
        elif self.queue_info is None and self.submitted:
            logme.log('Cannot find self in the queue and queue_info is empty',
                      'warn')
        return self.queue_info

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
        elif key == 'exitcode':
            return self.get_exitcode()
        return object.__getattribute__(self, key)

    def __repr__(self):
        """Return simple job information."""
        self.update()
        outstr = "Job:{name}<{mode}".format(name=self.name, mode=self.qtype)
        if self.submitted:
            outstr += ':{}'.format(self.id)
        outstr += "(command:{cmnd};args:{args})".format(
            cmnd=self.command, args=self.args)
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
        self.update()
        if self.done:
            state = 'completed'
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

        NOTE: Function submission will fail if the parent file's code is not
        wrapped in an if __main__ wrapper.

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
        rootmod       = inspect.getmodule(self.function)
        self.parent   = rootmod.__name__
        self.args     = args

        # Get the module path
        rootname = self.parent
        if hasattr(rootmod, '__file__'):
            imppath, impt = os.path.split(rootmod.__file__)
            impt = os.path.splitext(impt)[0]
        else:
            imppath = '.'
            impt = None

        if impt and self.function.__module__ == '__main__':
            self.function.__module__ = impt

        # Try to set a sane import string to make the function work
        if impt:
            #  modstr  = 'try:\n    import {}\n'.format(impt)
            #  modstr += 'except ImportError:\n    pass\n'
            modstr  = 'try:\n    from {} import {}\n'.format(
                impt, self.function.__name__)
            modstr += 'except ImportError:\n    pass\n'
        elif rootname != self.function.__name__ and rootname != '__main__':
            modstr  = 'try:\n    from {} import {}\n'.format(
                rootname, self.function.__name__)
            modstr += 'except ImportError:\n    pass\n'
        else:
            modstr  = 'try:\n    import {}\n'.format(self.function.__name__)
            modstr += 'except ImportError:\n    pass\n'

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

        # Write a list of modules to ~/.python_module_list.txt
        # This will work on standard linux systems only, and will silently fail
        # on other systems.
        ver = sys.version_info.major
        run.cmd("pip{} freeze --local | ".format(ver) +
                "grep -v '^\-e' | cut -d = -f 1 > " +
                "~/python_module_list")
        # Create a sane set of imports
        filtered_imports = []
        for imp in imports:
            if imp.startswith('import') or imp.startswith('from'):
                filtered_imports.append('try:\n    ' + imp.rstrip() +
                                        'except ImportError:\n    pass')
            else:
                if '.' in imp:
                    rootimp = imp.split('.')[0]
                    if not rootimp == 'pickle' and not rootimp == 'sys' \
                            and not rootimp == 'dill':
                        filtered_imports.append('try:\n    import {}\n'
                                                .format(rootimp) +
                                                'except ImportError:\n    ' +
                                                'pass\n')

                if imp == 'pickle' or imp == 'sys' or imp == 'dill':
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
        script = '#!/usr/bin/env python{}\n'.format(ver)
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

    :command:   The command or function to execute.
    :args:      Optional arguments to add to command, particularly
                useful for functions.
    :name:      The name of the job.
    :path:      Where to create the script, if None, current dir used.
    :qtype:     'torque', 'slurm', or 'normal'
    :profile:   The name of a profile saved in the config_file
    :kwargs:    Keyword arguments to control job options

    There are many keyword arguments available for cluster job submission.
    These vary somewhat by queue type. For info run::

        fyrd.options.option_help()

    :returns: Job object
    """

    queue.check_queue()  # Make sure the queue.MODE is usable

    job = Job(command=command, args=args, name=name, path=path, qtype=qtype,
              profile=profile, **kwargs)

    job.write()
    job.submit()
    job.update()

    return job


#########################
#  Job file generation  #
#########################


def make_job(command, args=None, name=None, path=None, qtype=None,
             profile=None, **kwargs):
    """Make a job file compatible with the chosen cluster.

    If mode is local, this is just a simple shell script.

    :command:   The command or function to execute.
    :args:      Optional arguments to add to command, particularly
                useful for functions.
    :name:      The name of the job.
    :path:      Where to create the script, if None, current dir used.
    :qtype:     'torque', 'slurm', or 'normal'
    :profile:   The name of a profile saved in the config_file

    There are many keyword arguments available for cluster job submission.
    These vary somewhat by queue type. For info run::

        fyrd.options.option_help()

    :returns: A Job object
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

    :command:   The command or function to execute.
    :args:      Optional arguments to add to command, particularly
                useful for functions.
    :name:      The name of the job.
    :path:      Where to create the script, if None, current dir used.
    :qtype:     'torque', 'slurm', or 'normal'
    :profile:   The name of a profile saved in the config_file
    :kwargs:    Keyword arguments to control job options

    There are many keyword arguments available for cluster job submission.
    These vary somewhat by queue type. For info run::

        fyrd.options.option_help()

    :returns:   Path to job script
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


def clean_dir(directory='.', suffix='cluster', qtype=None, confirm=False):
    """Delete all files made by this module in directory.

    CAUTION: The clean() function will delete **EVERY** file with
             extensions matching those these::
                 .<suffix>.err
                 .<suffix>.out
                 .<suffix>.sbatch & .<suffix>.script for slurm mode
                 .<suffix>.qsub for torque mode
                 .<suffix> for local mode
                 _func.<suffix>.py
                 _func.<suffix>.py.pickle.in
                 _func.<suffix>.py.pickle.out

    :directory: The directory to run in, defaults to the current directory.
    :qtype:     Only run on files of this qtype
    :confirm:   Ask the user before deleting the files
    :returns:   A set of deleted files
    """
    queue.check_queue(qtype)  # Make sure the queue.MODE is usable

    # Sanitize arguments
    if not directory:
        directory = '.'
    if not suffix:
        suffix = 'cluster'

    # Extension patterns to delete
    extensions = ['.' + suffix + '.err', '.' + suffix + '.out',
                  '_func.' + suffix + '.py',
                  '_func.' + suffix + '.py.pickle.in',
                  '_func.' + suffix + '.py.pickle.out']

    if qtype:
        if qtype == 'local':
            extensions.append('.' + suffix)
        elif qtype == 'slurm':
            extensions += ['.' + suffix + '.sbatch', '.' + suffix + '.script']
        elif qtype== 'torque':
            extensions.append('.' + suffix + '.qsub')
    else:
        extensions.append('.' + suffix)
        extensions += ['.' + suffix + '.sbatch', '.' + suffix + '.script']
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
                deleted.append(f)

    deleted = sorted(deleted)
    delete  = False

    if confirm:
        if deleted:
            sys.stdout.write('Files to delete::\n\t')
            sys.stdout.write('\n\t'.join(deleted) + '\n')
            msg = "Do you want to delete these files? (y/n) "
            while True:
                if sys.version_info.major == 2:
                    answer = raw_input(msg)
                else:
                    answer = input(msg)
                if answer == 'y' or answer == 'n':
                    break
                else:
                    sys.stdout.write('Invalid response {}, please try again\n'
                                     .format(answer))
            if answer == 'y':
                delete  = True
                sys.stdout.write('Deleting...\n')
            else:
                sys.stdout.write('Aborting\n')
                delete  = False
                deleted = []
        else:
            sys.stdout.write('No files to delete.\n')
    else:
        delete = True

    if delete and deleted:
        for f in deleted:
            os.remove(f)
        if confirm:
            sys.stdout.write('Done\n')

    return deleted
