"""
Manage job dependency tracking with multiprocessing.

============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2016-56-14 14:04
 Last modified: 2016-04-14 19:47

   DESCRIPTION: Runs jobs with a multiprocessing.Pool, but manages dependency
                using an additional Process that loops through all submitted
                jobs and checks dependencies before running.

============================================================================
"""
import multiprocessing as mp
from time import sleep

# Get threads from root
from . import THREADS

# Get an Exception object to use
from . import ClusterError

# Get logging function
from . import logme

# A global placeholder for a single JobQueue instance
QUEUE = None

###############################################################################
#                      The JobQueue Class to Manage Jobs                      #
###############################################################################


class JobQueue(object):

    """Monitor and submit multiprocessing.Pool jobs with dependencies."""

    def __init__(self, cores=None):
        """Spawn a job_runner process to interact with."""
        self._jobqueue = mp.Queue()
        self._outputs  = mp.Queue()
        self.runner    = mp.Process(target=job_runner,
                                    args=(self._jobqueue, self._outputs,
                                          cores))
        self.runner.start()
        assert self.runner.is_alive()
        self.jobs = {}

    def update(self):
        """Get fresh job info from the runner."""
        sleep(2)  # This allows the queue time to flush
        while not self._outputs.empty():
            # We loop through the whole queue stack, updating the dictionary
            # every time so that we get the latest info
            self.jobs.update(self._outputs.get_nowait())

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
        lastjob = max(self.jobs.keys()) if self.jobs.keys() else 0
        assert self.runner.is_alive()
        self._jobqueue.put(((function, args, kwargs), dependencies))
        sleep(0.5)
        self.update()
        newjob = max(self.jobs.keys())
        assert newjob != lastjob
        return newjob

    def __len__(self):
        """Length is the total job count."""
        self.update()
        return len(self.jobs)

    def __getattr__(self, attr):
        """Dynamic dictionary filtering."""
        if attr == 'completed' or attr == 'started' or attr == 'waiting':
            newdict = {}
            for jobid, job_info in self.jobs.items():
                if job_info['state'] == attr:
                    newdict[jobid] = job_info
            return newdict

    def __getitem__(self, key):
        """Allow direct job lookup by jobid."""
        key = int(key)
        if key in self.jobs:
            return self.jobs[key]
        else:
            return None

    def __repr__(self):
        """Class information."""
        return "JobQueue<jobs:{};completed{};waiting:{};started{}>".format(
            len(self.jobs), len(self.completed), len(self.waiting),
            len(self.started))

    def __str__(self):
        """Print jobs."""
        return str(self.jobs)


###############################################################################
#               The Job Runner that will fork and run all jobs                #
###############################################################################


def job_runner(jobqueue, outputs, cores=None):
    """Run jobs with dependency tracking.

    Must be run as a separate multiprocessing.Process to function correctly.

    :jobqueue: A multiprocessing.Queue object which jobs in the format::
                    ((function, args, kwargs), depends)
               must be added. The function continually searches this Queue for
               new jobs. Note, function must be a function call, it cannot be
               anything else.
               function is the only required argument, the rest are optional.
               tuples are required.
    :outputs:  A multiprocessing.Queue object that will take outputs. A
               dictionary of job objects will be output here with the format::
                   {job_no => {func:function, args:args,
                               state:waiting,started,done,
                               out:returned object}}
               **NOTE**: function return must be picklable otherwise this will
                         raise an exception when it is put into the Queue
                         object.
    :cores:    Number of cores to use in the multiprocessing pool. Defaults to
               all.
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
    if not isinstance(jobqueue, mp.Queue) or not isinstance(outputs, mp.Queue):
        raise ClusterError('jobqueue and outputs must be multiprocessing ' +
                           'Queue objects')

    # Initialize job objects
    jobno   = 1  # Start job numbers at 1
    jobs    = {} # This will hold job numbers
    runners = {} # This will hold the multiprocessing objects
    done    = [] # A list of completed jobs to check against
    started = [] # A list of started jobs to check against
    cores   = cores if cores else THREADS
    pool    = mp.Pool(cores)

    # Actually loop through the jobs
    while True:
        if not jobqueue.empty():
            info = jobqueue.get_nowait()
            if not isinstance(info, tuple):
                logme.log('job information must be tuple, was {}'.format(
                    type(info)), 'error')
                continue
            if isinstance(info[0], tuple):
                if len(info) == 1:
                    fun_args = info[0]
                    depends  = None
                elif len(info) == 2:
                    fun_args = info[0]
                    depends  = info[1]
                else:
                    logme.log('job information tuple must be in the format ' +
                              '((function, args, kwargs), depends). Your ' +
                              'tuple was in the format:: ' +
                              '((something), depends, ??)', 'error')
                    continue
            else:
                fun_args = info
                depends  = None
            if len(fun_args) == 1:
                function = fun_args[0]
                args     = None
                kwargs   = None
            elif len(fun_args) == 2:
                function, args = fun_args
                if not isinstance(args, tuple):
                    args = (args,)
                kwargs = None
            elif len(fun_args) == 3:
                function, args, kwargs = fun_args
                if not isinstance(args, tuple):
                    args = (args,)
                if not isinstance(kwargs, dict):
                    logme.log('kwargs must be a dict', 'error')
                    continue
            else:
                logme.log('Too many function arguments', 'error')
                continue

            # The arguments look good, so lets add this to the stack.
            jobs[jobno] = {'func': function, 'args': args,
                           'kwargs': kwargs, 'depends': depends,
                           'out': None, 'state': None}
            output(jobs)
            jobno += 1

        # If there are jobs, try and run them
        if jobs:
            for jobno, job_info in jobs.items():
                # Skip completed jobs
                if job_info['done']:
                    continue

                # Check dependencies
                ready = True
                if job_info['depends']:
                    for depend in job_info['depends']:
                        if not depend in done:
                            ready = False
                            job_info['state'] = 'waiting'

                # Start jobs if dependencies are met and they aren't started.
                if ready and not jobno in started:
                    if job_info['args'] or job_info['kwargs']:
                        if job_info['kwargs']:
                            if job_info['args']:
                                runners[jobno] = pool.apply_async(
                                    job_info['func'], job_info['args'],
                                    job_info['kwargs'])
                            else:
                                runners[jobno] = pool.apply_async(
                                    job_info['func'],
                                    kwds=job_info['kwargs'])
                        else:
                            runners[jobno] = pool.apply_async(
                                job_info['func'], job_info['args'])
                    else:
                        runners[jobno] = pool.apply_async(job_info['func'])
                    job_info['state'] = 'started'
                    started.append(jobno)
                    output(jobs)
                    sleep(0.5)  # Wait for a second to allow job to start

                # Check running jobs for completion
                if job_info['state'] == 'started' and runners[jobno].ready():
                    job_info['out']   = runners[jobno].get()
                    job_info['state'] = 'done'
                    done.append(jobno)
                    output(jobs)

        # Wait for half a second before looping again
        sleep(0.5)
