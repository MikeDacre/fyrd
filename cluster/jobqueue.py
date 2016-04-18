"""
Manage job dependency tracking with multiprocessing.

============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2016-56-14 14:04
 Last modified: 2016-04-15 17:40

   DESCRIPTION: Runs jobs with a multiprocessing.Pool, but manages dependency
                using an additional Process that loops through all submitted
                jobs and checks dependencies before running.

============================================================================
"""
import atexit
import multiprocessing as mp
from time import sleep

# Get threads from root
from . import THREADS

# Get defaults
from . import config_file

# Get an Exception object to use
from . import ClusterError

# Get logging function
from . import logme

# A global placeholder for a single JobQueue instance
JQUEUE = None

###############################################################################
#                      The JobQueue Class to Manage Jobs                      #
###############################################################################


class JobQueue(object):

    """Monitor and submit multiprocessing.Pool jobs with dependencies."""

    def __init__(self, cores=None):
        """Spawn a job_runner process to interact with."""
        self._jobqueue = mp.Queue()
        self._outputs  = mp.Queue()
        self.jobno     = int(config_file.get('jobqueue', 'jobno', str(1)))
        self.runner    = mp.Process(target=job_runner,
                                    args=(self._jobqueue, self._outputs,
                                          cores, self.jobnumber))
        self.runner.start()
        assert self.runner.is_alive()
        self.jobs = {}

        def terminate():
            """Kill the queue runner."""
            try:
                self.update()
                self.runner.terminate()
            except AssertionError:
                pass
            self._jobqueue.close()
            self._outputs.close()

        # Call terminate when we exit
        atexit.register(terminate)

    def update(self):
        """Get fresh job info from the runner."""
        sleep(0.5)  # This allows the queue time to flush
        assert self.runner.is_alive() is True
        while not self._outputs.empty():
            # We loop through the whole queue stack, updating the dictionary
            # every time so that we get the latest info
            self.jobs.update(self._outputs.get_nowait())
        if self.jobs:
            self.jobno = max(self.jobs.keys())
            config_file.write('jobqueue', 'jobno', str(self.jobno))

    def add(self, function, args=None, kwargs=None, dependencies=None):
        """Add function to local job queue.

        :function:     A function object. To run a command, use the run.cmd
                       function here.
        :args:         A tuple of args to submit to the function.
        :kwargs:       A dict of keyword arguments to submit to the function.
        :dependencies: A list of job IDs that this job will depend on.
        :returns:      A job ID
        """
        self.update()
        assert self.runner.is_alive()
        self._jobqueue.put(self.Job(function, args, kwargs, dependencies))
        sleep(0.5)
        self.update()
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


    def __getattr__(self, attr):
        """Dynamic dictionary filtering."""
        if attr == 'done' or attr == 'started' or attr == 'waiting':
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
        for id, job in self.jobs.items():
            yield id, job

    def __len__(self):
        """Length is the total job count."""
        self.update()
        return len(self.jobs)

    def __repr__(self):
        """Class information."""
        return "JobQueue<jobs:{};completed:{};waiting:{};started:{}>".format(
            len(self.jobs), len(self.done), len(self.waiting),
            len(self.started))

    def __str__(self):
        """Print jobs."""
        return str(self.jobs)

    class Job(object):

        """An object to pass arguments to the runner."""

        def __init__(self, function, args=None, kwargs=None, depends=None):
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

            # Assigned later
            self.id       = None
            self.out      = None
            self.state    = 'Not Submitted'

        def __repr__(self):
            """Job Info."""
            return "JobQueue.Job<{}(function:{},args:{},kwargs:{}) {}>".format(
                self.id, self.function.__name__, self.args, self.kwargs,
                self.state)

        def __str__(self):
            """Print Info and Output."""
            outstr  = "Job #{}\n".format(self.id if self.id else 'NA')
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

    :jobqueue: A multiprocessing.Queue object into which JobQueue.Job objects
               must be added. The function continually searches this Queue for
               new jobs. Note, function must be a function call, it cannot be
               anything else.
               function is the only required argument, the rest are optional.
               tuples are required.
    :outputs:  A multiprocessing.Queue object that will take outputs. A
               dictionary of job objects will be output here with the format::
                   {job_no => JobQueue.Job}
               **NOTE**: function return must be picklable otherwise this will
                         raise an exception when it is put into the Queue
                         object.
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
              else int(config_file.get('jobqueue', 'jobno', str(1)))
    jobno   = jobno-1 if jobno is not 0 else 0  # We increment first
    jobs    = {} # This will hold job numbers
    runners = {} # This will hold the multiprocessing objects
    done    = [] # A list of completed jobs to check against
    started = [] # A list of started jobs to check against
    cores   = cores if cores else THREADS
    pool    = mp.Pool(cores)

    # Actually loop through the jobs
    while True:
        if not jobqueue.empty():
            jobno += 1
            job = jobqueue.get_nowait()
            if not isinstance(job, JobQueue.Job):
                logme.log('job information must be a job object, was {}'.format(
                    type(job)), 'error')
                continue

            # The arguments look good, so lets add this to the stack.
            job.state   = 'queued'
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
                if ready and not jobno in started:
                    if job_info.args and job_info.kwargs:
                        runners[jobno] = pool.apply_async(job_info.function,
                                                          job_info.args,
                                                          job_info.kwargs)
                    elif job_info.args:
                        runners[jobno] = pool.apply_async(job_info.function,
                                                          job_info.args)
                    elif job_info.kwargs:
                        runners[jobno] = pool.apply_async(job_info.function,
                                                          kwds=job_info.kwargs)
                    else:
                        runners[jobno] = pool.apply_async(job_info.function)
                    job_info.state = 'started'
                    started.append(jobno)
                    output(jobs)
                    sleep(0.5)  # Wait for a second to allow job to start

                # Check running jobs for completion
                if job_info.state == 'started' and not jobno in done \
                        and runners[jobno].ready():
                    job_info.out   = runners[jobno].get()
                    job_info.state = 'done'
                    done.append(jobno)
                    output(jobs)

        # Wait for half a second before looping again
        sleep(0.5)
