"""
Manage job dependency tracking with multiprocessing.

Last modified: 2016-10-27 13:08

Runs jobs with a multiprocessing.Pool, but manages dependency using an
additional Process that loops through all submitted jobs and checks
dependencies before running.

The JobQueue class works as the queue and functions in a similar, but much more
basic, way as torque or slurm. It manages jobs by forking an instance of the
job_runner function and keeping it alive. The multiprocessing.Queue class is
then used to pass job information contained in the Job class back and forth
between the JobQueue class running in the main process and the job_runner()
fork running as a separate thread.

The actual job management is done by job_runner() and uses
multiprocessing.Process objects and not the Pool object.  This allows for more
careful management and it also allows exit codes to be captured.
"""
import os
import sys
import atexit
import signal
import multiprocessing as mp
from subprocess import check_output, CalledProcessError
from time import sleep

from . import run

# Get defaults
from . import config_file

# Get an Exception object to use
from . import ClusterError

# Get logging function
from . import logme

# A global placeholder for a single JobQueue instance
JQUEUE = None

__all__ = ['JobQueue']

################################
#  Normal Mode Multithreading  #
################################

from multiprocessing import cpu_count as _cnt
THREADS  = _cnt()

# Reset broken multithreading
# Some of the numpy C libraries can break multithreading, this command
# fixes the issue.
try:
    check_output("taskset -p 0xff %d >/dev/null 2>/dev/null" % os.getpid(),
                 shell=True)
except CalledProcessError:
    pass  # This doesn't work on Macs or Windows


###############################################################################
#                      The JobQueue Class to Manage Jobs                      #
###############################################################################


class JobQueue(object):

    """Monitor and submit multiprocessing.Pool jobs with dependencies."""

    def __init__(self, cores=None):
        """Spawn a job_runner process to interact with."""
        self._jobqueue = mp.Queue()
        self._outputs  = mp.Queue()
        self.jobno     = int(config_file.get_option('jobqueue', 'jobno',
                                                    str(1)))
        self.cores     = int(cores) if cores else THREADS
        self.runner    = mp.Process(target=job_runner,
                                    args=(self._jobqueue,
                                          self._outputs,
                                          self.cores,
                                          self.jobnumber),
                                    name='Runner')
        self.runner.start()
        self.pid = self.runner.pid
        assert self.runner.is_alive()
        self.jobs = {}

        def terminate():
            """Kill the queue runner."""
            try:
                self.runner.terminate()
                self._jobqueue.close()
                self._outputs.close()
            except AttributeError:
                pass
            if run.check_pid(self.runner.pid):
                os.kill(self.runner.pid, signal.SIGKILL)

        # Call terminate when we exit
        atexit.register(terminate)

    def update(self):
        """Get fresh job info from the runner."""
        sleep(0.5)  # This allows the queue time to flush
        if self.runner.is_alive() is not True:
            self.restart(True)
        if self.runner.is_alive() is not True:
            raise ClusterError('JobRunner has crashed')
        while not self._outputs.empty():
            # We loop through the whole queue stack, updating the dictionary
            # every time so that we get the latest info
            self.jobs.update(self._outputs.get_nowait())
        if self.jobs:
            self.jobno = max(self.jobs.keys())
            config_file.set_option('jobqueue', 'jobno', str(self.jobno))

    def add(self, function, args=None, kwargs=None, dependencies=None,
            cores=1):
        """Add function to local job queue.

        :function:     A function object. To run a command, use the run.cmd
                       function here.
        :args:         A tuple of args to submit to the function.
        :kwargs:       A dict of keyword arguments to submit to the function.
        :dependencies: A list of job IDs that this job will depend on.
        :cores:        The number of threads required by this job.
        :returns:      A job ID
        """
        self.update()
        assert self.runner.is_alive()
        oldjob = self.jobno
        cores = int(cores)
        if cores > self.cores:
            logme.log('Job core request exceeds resources, limiting to max: ' +
                      '{}'.format(self.cores), 'warn')
            cores = self.cores
        self._jobqueue.put(Job(function, args, kwargs, dependencies,
                               cores))
        sleep(0.5)
        self.update()
        newjob = self.jobno
        # Sometimes the queue can freeze for reasons I don't understand, this
        # is an attempted workaround.
        if not newjob == oldjob + 1:
            self.restart(True)
            self._jobqueue.put(Job(function, args, kwargs, dependencies,
                                   cores))
            self.update()
            newjob = self.jobno
        if not newjob == oldjob + 1:
            raise ClusterError('Job numbers are not updating correctly, the '
                               'local queue has probably crashed. Please '
                               'report this issue.')
        return self.jobno

    def wait(self, jobs=None):
        """Wait for a list of jobs, all jobs are the default."""
        self.update()
        if not isinstance(jobs, (list, tuple)):
            jobs = [jobs]
        while jobs:
            for job in jobs:
                if job not in self.jobs:
                    raise ClusterError('Job {} has not been submitted.'.format(job))
                if self.jobs[job].state == 'done':
                    jobs.remove(job)
                sleep(0.5)

    def get(self, job):
        """Return the output of a single job"""
        if job not in self.jobs:
            raise ClusterError('Job {} has not been submitted.'.format(job))
        self.wait(job)
        return self.jobs[job].out

    def restart(self, force=False):
        """Kill the job queue and restart it."""
        if not force:
            self.update()
            if len(self.done) != len(self.jobs):
                logme.log('Cannot restart, incomplete jobs', 'error')
                return
        self.runner.terminate()
        self.runner  = mp.Process(target=job_runner,
                                  args=(self._jobqueue, self._outputs,
                                        self.cores, self.jobnumber),
                                  name='Runner')
        self.runner.start()
        self.pid = self.runner.pid
        assert self.runner.is_alive()

    def __getattr__(self, attr):
        """Dynamic dictionary filtering."""
        if attr == 'done' or attr == 'queued' or attr == 'waiting' \
                or attr == 'running':
            newdict = {}
            for jobid, job_info in self.jobs.items():
                if job_info.state == attr:
                    newdict[jobid] = job_info
            return newdict

    def __getitem__(self, key):
        """Allow direct accessing of jobs by job id."""
        self.update()
        key = int(key)
        try:
            return self.jobs[key]
        except KeyError:
            return None

    def __iter__(self):
        """Allow us to be iterable"""
        self.update()
        for jobno, job in self.jobs.items():
            yield jobno, job

    def __len__(self):
        """Length is the total job count."""
        self.update()
        return len(self.jobs)

    def __repr__(self):
        """Class information."""
        self.update()
        return ("JobQueue<({})jobs:{};completed:{};running:{};queued:{}>"
                .format(self.cores, len(self.jobs), len(self.done),
                        len(self.running),
                        len(self.waiting) + len(self.queued)))

    def __str__(self):
        """Print jobs."""
        self.update()
        return str(self.jobs)

class Job(object):

    """An object to pass arguments to the runner."""

    def __init__(self, function, args=None, kwargs=None, depends=None,
                    cores=1):
        """Parse and save arguments."""
        if args and not isinstance(args, tuple):
            args = (args,)
        if kwargs and not isinstance(kwargs, dict):
            raise TypeError('kwargs must be a dict')
        if depends:
            if not isinstance(depends, (tuple, list)):
                depends = [depends]
            try:
                depends = [int(i) for i in depends]
            except ValueError:
                raise ValueError('dependencies must be integer job ids')
        self.function = function
        self.args     = args
        self.kwargs   = kwargs
        self.depends  = depends
        self.cores    = int(cores)

        # Assigned later
        self.id       = None
        self.pid      = None
        self.exitcode = None
        self.out      = None
        self.state    = 'Not Submitted'

    def __repr__(self):
        """Job Info."""
        return ("Job<{} (function:{},args:{}," +
                "kwargs:{};cores:{}) {}>").format(
                    self.id, self.function.__name__, self.args,
                    self.kwargs, self.cores, self.state)

    def __str__(self):
        """Print Info and Output."""
        outstr  = "Job #{}; Cores: {}\n".format(
            self.id if self.id else 'NA', self.cores)
        outstr += "\tFunction: {}\n\targs: {}\n\tkwargs: {}\n\t".format(
            self.function.__name__, self.args, self.kwargs)
        outstr += "State: {}\n\tOutput: {}\n".format(self.state, self.out)
        return outstr


###############################################################################
#               The Job Runner that will fork and run all jobs                #
###############################################################################


def job_runner(jobqueue, outputs, cores=None, jobno=None):
    """Run jobs with dependency tracking.

    Must be run as a separate multiprocessing.Process to function correctly.

    :jobqueue: A multiprocessing.Queue object into which Job objects
               must be added. The function continually searches this Queue for
               new jobs. Note, function must be a function call, it cannot be
               anything else.
               function is the only required argument, the rest are optional.
               tuples are required.
    :outputs:  A multiprocessing.Queue object that will take outputs. A
               dictionary of job objects will be output here with the format::
               {job_no => Job}
               **NOTE**: function return must be picklable otherwise this will
               raise an exception when it is put into the Queue object.
    :cores:    Number of cores to use in the multiprocessing pool. Defaults to
               all.
    :jobno:    What number to start counting jobs from, default 1.
    """
    def output(out):
        """Explicitly clear the dictionary before sending the output."""
        # Don't send output if it is the same as last time.
        lastout = outputs.get() if not outputs.empty() else ''
        if out == lastout:
            return
        while not outputs.empty():
            # Clear the output object
            outputs.get()
        outputs.put(out)

    # Make sure we have Queue objects
    if not isinstance(jobqueue, mp.queues.Queue) \
            or not isinstance(outputs, mp.queues.Queue):
        raise ClusterError('jobqueue and outputs must be multiprocessing ' +
                           'Queue objects')

    # Initialize job objects
    jobno   = int(jobno) if jobno \
              else int(config_file.get_option('jobqueue', 'jobno', str(1)))
    jobs    = {} # This will hold job numbers
    started = [] # A list of started jobs to check against
    cores   = cores if cores else THREADS
    queue   = [] # This will hold Processes that haven't started yet
    running = [] # This will hold actively running jobs to manage core count
    done    = [] # A list of completed jobs to check against

    # Actually loop through the jobs
    while True:
        if not jobqueue.empty():
            oldjob = jobno
            jobno += 1
            newjob = jobno
            # Sometimes the thread stalls if it has been left a while and
            # ends up reusing the same job number. I don't know why this
            # happens, however explicitly getting the jobno seems to fix the
            # issue. Just to be sure I also want to test that the job number is
            # incremented, but this is a little redundant at this point.
            assert newjob == oldjob + 1
            job = jobqueue.get_nowait()
            if not isinstance(job, Job):
                logme.log('job information must be a job object, was {}'.format(
                    type(job)), 'error')
                continue

            # The arguments look good, so lets add this to the stack.
            job.state   = 'submitted'
            job.id      = jobno
            jobs[jobno] = job

            # Send the job dictionary
            output(jobs)

        # If there are jobs, try and run them
        if jobs:
            for jobno, job_info in jobs.items():
                # Skip completed jobs
                if job_info.state == 'done':
                    continue

                # Check dependencies
                ready = True
                if job_info.depends:
                    for depend in job_info.depends:
                        if int(depend) not in done:
                            ready = False
                            job_info.state = 'waiting'
                            output(jobs)

                # Start jobs if dependencies are met and they aren't started.
                # We use daemon mode so that child jobs are killed on exit.
                if ready and not jobno in started:
                    ver = sys.version_info.major
                    # Python 2 doesn't support daemon, even though the docs
                    # say that it does.
                    gen_args = dict(name=str(jobno)) if ver == 2 \
                        else dict(name=str(jobno), daemon=True)
                    if job_info.args and job_info.kwargs:
                        queue.append((mp.Process(target=job_info.function,
                                                 args=job_info.args,
                                                 kwargs=job_info.kwargs,
                                                 **gen_args),
                                      job_info.cores))
                    elif job_info.args:
                        queue.append((mp.Process(target=job_info.function,
                                                 args=job_info.args,
                                                 **gen_args),
                                      job_info.cores))
                    elif job_info.kwargs:
                        queue.append((mp.Process(target=job_info.function,
                                                 kwargs=job_info.kwargs,
                                                 **gen_args),
                                      job_info.cores))
                    else:
                        queue.append((mp.Process(target=job_info.function,
                                                 **gen_args),
                                      job_info.cores))
                    job_info.state = 'queued'
                    started.append(jobno)
                    output(jobs)

        # Actually run jobs
        if queue:
            # Get currently used cores, ignore ourself
            running_cores = 0
            for i in [i[1] for i in running]:
                running_cores += i
            # Look for a job to run
            for j in queue:
                if j[1] + running_cores <= cores:
                    j[0].start()
                    jobs[int(j[0].name)].state = 'running'
                    jobs[int(j[0].name)].pid = j[0].pid
                    running.append(queue.pop(queue.index(j)))
                    output(jobs)
                    sleep(0.5)  # Wait for a second to allow job to start
                    break

        # Clean out running jobs
        if running:
            for i in running:
                j = i[0]
                if not j.is_alive():
                    jobs[int(j.name)].out = j.join()
                    jobs[int(j.name)].state = 'done'
                    jobs[int(j.name)].exitcode = j.exitcode
                    done.append(int(j.name))
                    running.pop(running.index(i))
                    output(jobs)

        # Wait for half a second before looping again
        sleep(0.5)
