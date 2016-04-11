"""
Monitor the queue for torque or slurm.
============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2015-12-11
 Last modified: 2016-04-11 08:56

============================================================================
"""
import os
import re
import sys
from os import environ
from sys import stderr
from time import time
from time import sleep
from pwd import getpwnam
from subprocess import check_output, CalledProcessError
from multiprocessing import Pool, pool

###############################################################################
#                                Our functions                                #
###############################################################################

from . import run
from . import logme
from . import ClusterError

#########################
#  Which system to use  #
#########################

# Default is normal, change to 'slurm' or 'torque' as needed.
from . import QUEUE
from . import ALLOWED_QUEUES

#########################################################
#  The multiprocessing pool, only used in 'local' mode  #
#########################################################

from . import POOL

# Reset broken multithreading
# Some of the numpy C libraries can break multithreading, this command
# fixes the issue.
check_output("taskset -p 0xff %d &>/dev/null" % os.getpid(), shell=True)

# Our imports
from . import DEFAULTS

# We only need the queue defaults
_defaults = DEFAULTS['queue']

# Funtions to import if requested
__all__ = ['get_cluster_environment', 'Queue']

###########################################################
#  Set the global cluster type: slurm, torque, or normal  #
###########################################################

def get_cluster_environment():
    """Detect the local cluster environment and set QUEUE globally.

    Uses which to search for sbatch first, then qsub. If neither is found,
    QUEUE is set to local.

    :returns: QUEUE variable ('torque', 'slurm', or 'local')
    """
    global QUEUE
    if run.which('sbatch'):
        QUEUE = 'slurm'
    elif run.which('qsub'):
        QUEUE = 'torque'
    else:
        QUEUE = 'local'
    if QUEUE == 'slurm' or QUEUE == 'torque':
        logme.log('{} detected, using for cluster submissions'.format(QUEUE),
                  'debug')
    else:
        logme.log('No cluster environment detected, using multiprocessing',
                  'debug')
    return QUEUE


# Actually run the above function on every import
get_cluster_environment()

##############################
#  Check if queue is usable  #
##############################

def check_queue():
    """Raise exception if QUEUE is incorrect."""
    if QUEUE not in ALLOWED_QUEUES:
        raise ClusterError('QUEUE value {} is not recognized, '.format(QUEUE) +
                           'should be: normal, torque, or slurm')


#####################################
#  Wait for cluster jobs to finish  #
#####################################

def wait(jobs):
    """Wait for jobs to finish.

    :jobs:    A single job or list of jobs to wait for. With torque or slurm,
              these should be job IDs, with normal mode, these are
              multiprocessing job objects (returned by submit())
    """
    check_queue()  # Make sure the QUEUE is usable

    # Sanitize argument
    if not isinstance(jobs, (list, tuple)):
        jobs = [jobs]
    for job in jobs:
        if not isinstance(job, (str, int, pool.ApplyResult)):
            raise ClusterError('job must be int, string, or ApplyResult, ' +
                               'is {}'.format(type(job)))

    if QUEUE == 'normal':
        for job in jobs:
            if not isinstance(job, pool.ApplyResult):
                raise ClusterError('jobs must be ApplyResult objects')
            job.wait()
    elif QUEUE == 'torque':
        # Wait for 5 seconds before checking, as jobs take a while to be queued
        # sometimes
        sleep(5)

        s = re.compile(r' +')  # For splitting qstat output
        # Jobs must be strings for comparison operations
        jobs = [str(j) for j in jobs]
        while True:
            c = 0
            try:
                q = check_output(['qstat', '-a']).decode().rstrip().split('\n')
            except CalledProcessError:
                if c == 5:
                    raise
                c += 1
                sleep(2)
                continue
            # Check header
            if not re.split(r' {2,100}', q[3])[9] == 'S':
                raise ClusterError('Unrecognized torque qstat format')
            # Build a list of completed jobs
            complete = []
            for j in q[5:]:
                i = s.split(j)
                if i[9] == 'C':
                    complete.append(i[0].split('.')[0])
            # Build a list of all jobs
            all  = [s.split(j)[0].split('.')[0] for j in q[5:]]
            # Trim down job list
            jobs = [j for j in jobs if j in all]
            jobs = [j for j in jobs if j not in complete]
            if len(jobs) == 0:
                return
            sleep(2)
    elif QUEUE == 'slurm':
        # Wait for 2 seconds before checking, as jobs take a while to be queued
        # sometimes
        sleep(2)

        # Jobs must be strings for comparison operations
        jobs = [str(j) for j in jobs]
        while True:
            # Slurm allows us to get a custom output for faster parsing
            q = check_output(
                ['squeue', '-h', '-o', "'%A,%t'"]).decode().rstrip().split(',')
            # Build a list of jobs
            complete = [i[0] for i in q if i[1] == 'CD']
            failed   = [i[0] for i in q if i[1] == 'F']
            all      = [i[0] for i in q]
            # Trim down job list, ignore failures
            jobs = [i for i in jobs if i not in all]
            jobs = [i for i in jobs if i not in complete]
            jobs = [i for i in jobs if i not in failed]
            if len(jobs) == 0:
                return
            sleep(2)


class Queue(object):
    """ Functions that need to access the slurm queue """

    def wait(self, job_list):
        """ Block until all jobs in job_list are complete.

        Note: update time is dependant upon the queue_update parameter in
              your ~/.cluster file.

              In addition, wait() will not return until between 1 and 3
              seconds after a job has completed, irrespective of queue_update
              time. This allows time for any copy operations to complete after
              the job exits.

        :job_list: int, string, list, or tuple of job ids.
        :returns:  True on success False or nothing on failure.
        """
        if not isinstance(job_list, (list, tuple)):
            job_list = [job_list]
        job_list = [int(i) for i in job_list]
        for jb in job_list:
            while True:
                self.load()
                not_found = 0
                # Allow two seconds to elapse before job is found in queue,
                # if it is not in the queue by then, raise exception.
                if jb not in self:
                    sleep(1)
                    not_found += 1
                    if not_found == 3:
                        raise self.QueueError('{} not in queue'.format(jb))
                    continue
                # Actually look for job in running/queued queues
                if jb in self.running.keys() or jb in self.queued.keys():
                    sleep(2)
                else:
                    break
        sleep(1)  # Sleep an extra second to allow post-run scripts to run.
        return True

    def get_job_count(self):
        """ If job count not updated recently, update it """
        self.load()
        return self.job_count

    def __init__(self):
        """ Create self. """
        self.uid = getpwnam(environ['USER']).pw_uid
        self.full_queue = job()
        self._load()

    ######################
    # Internal Functions #
    ######################
    def load(self):
        if int(time()) - self.full_queue.lastUpdate() > int(_defaults['queue_update']):
            self._load()

    def _get_running_jobs(self):
        """ Return only queued jobs """
        self.load()
        j = 'RUNNING'
        running = {}
        for k, v in self.queue.items():
            if isinstance(v['job_state'], bytes):
                v['job_state'] = v['job_state'].decode()
            if v['job_state'] == j:
                running[k] = v
        return running

    def _get_queued_jobs(self):
        """ Return only queued jobs """
        self.load()
        j = 'PENDING'
        queued = {}
        for k, v in self.queue.items():
            if isinstance(v['job_state'], bytes):
                v['job_state'] = v['job_state'].decode()
            if v['job_state'] == j:
                queued[k] = v
        return queued

    def _load(self):
        """ Refresh the list of jobs from the server. """
        try:
            self.current_job_ids = self.full_queue.find('user_id', self.uid)
        except ValueError:
            sleep(5)
            self._load()
        self.job_count = len(self.current_job_ids)
        self.queue = {}
        try:
            for k, v in self.full_queue.get().items():
                if k in self.current_job_ids:
                    self.queue[k] = v
        except ValueError:
            sleep(5)
            self._load()

    def __iter__(self):
        """ Loop through jobs. """
        for jb in self.queue.keys():
            yield jb

    def __getattr__(self, key):
        """ Make running and queued attributes dynamic. """
        if key == 'running':
            return self._get_running_jobs()
        if key == 'queued':
            return self._get_queued_jobs()

    def __getitem__(self, key):
        """ Allow direct accessing of jobs by key. """
        self.load()
        try:
            return self.queue[key]
        except KeyError:
            return None

    def __len__(self):
        """ Length is the total job count. """
        self.load()
        return self.get_job_count()

    def __repr__(self):
        """ For debugging. """
        self.load()
        return 'queue<{}>'.format(self.queue.keys())

    def __str__(self):
        """ A list of keys. """
        self.load()
        return str(self.queue.keys())

    ################
    #  Exceptions  #
    ################
    class QueueError(Exception):

        """ Simple Exception wrapper. """

        pass
