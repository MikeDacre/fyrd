"""
Manage job dependency tracking with multiprocessing.

============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2016-56-14 14:04
 Last modified: 2016-04-14 15:38

   DESCRIPTION: Runs jobs with a multiprocessing.Pool, but manages dependency
                using an additional Process that loops through all submitted
                jobs and checks dependencies before running.

============================================================================
"""
import sys
import multiprocessing as mp
from time import sleep
from queue import Empty

# Get threads from root
from . import THREADS

# Get an Exception object to use
from . import ClusterError

# Get logging function
from . import logme


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

    def update_jobs(self):
        """Get fresh job info from the runner."""
        sleep(2)  # This allows the queue time to flush
        while not outqueue.empty():
            # We loop through the whole queue stack, updating the dictionary
            # every time so that we get the latest info
            jobdict.update(outqueue.get_nowait())
        return jobdict


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
                   {job_no => {func:function, args:args, started:True/False,
                               done:True/False, out:returned object}}
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
                    func_args = info[0]
                    depends   = None
                elif len(info) == 2:
                    func_args = info[0]
                    depends   = info[1]
                else:
                    logme.log('job information tuple must be in the format ' +
                              '((function, args, kwargs), depends). Your ' +
                              'tuple was in the format:: ' +
                              '((something), depends, ??)', 'error')
                    continue
            else:
                func_args = info
                depends   = None
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
                    logme.log('kwargs must be a dict'. 'error')
                    continue
            else:
                logme.log('Too many function arguments', 'error')
                continue

            # The arguments look good, so lets add this to the stack.
            jobs[jobno] = {'func': function, 'args': args,
                           'kwargs': kwargs, 'depends': depends,
                           'out': None, 'started': False, 'done': False}
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

                # Start jobs if dependencies are met and they aren't started.
                if ready and not job_info['started']:
                    if job_info['args']:
                        if job_info['kwargs']:
                            runners[jobno] = pool.apply_async(
                                job_info['func'], job_info['args'],
                                job_info['kwargs'])
                        else:
                            runners[jobno] = pool.apply_async(
                                job_info['func'], job_info['args'])
                    else:
                        runners[jobno] = pool.apply_async(job_info['func'])
                    job_info['started'] = True
                    output(jobs)
                    sleep(0.5)  # Wait for a second to allow job to start

                # Check running jobs for completion
                if job_info['started'] and not job_info['done'] and runners[jobno].ready():
                    job_info['out'] = runners[jobno].get()
                    job_info['done'] = True
                    done.append(jobno)
                    output(jobs)

        # Wait for half a second before looping again
        sleep(0.5)
