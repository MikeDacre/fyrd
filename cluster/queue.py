"""
Monitor the queue for torque or slurm.
============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2015-12-11
 Last modified: 2016-04-11 20:11

============================================================================
"""
import re
import pwd               # Used to get usernames for queue
import os
from os import environ   # Used to check current username
from time import time
from time import sleep
from subprocess import check_output, CalledProcessError
from multiprocessing import Pool, pool

# For parsing torque queues
import xml.etree.ElementTree as ET

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
from . import THREADS

# Reset broken multithreading
# Some of the numpy C libraries can break multithreading, this command
# fixes the issue.
try:
    check_output("taskset -p 0xff %d &>/dev/null" % os.getpid(), shell=True)
except CalledProcessError:
    pass  # This doesn't work on Macs or Windows

# Our imports
from . import DEFAULTS

# We only need the queue defaults
_defaults = DEFAULTS['queue']

# Funtions to import if requested
__all__ = ['Queue', 'check_queue', 'get_cluster_environment']

###########################################################
#  Set the global cluster type: slurm, torque, or normal  #
###########################################################


def get_cluster_environment():
    """Detect the local cluster environment and set QUEUE globally.

    Uses which to search for sbatch first, then qsub. If neither is found,
    QUEUE is set to normal.

    :returns: QUEUE variable ('torque', 'slurm', or 'local')
    """
    global QUEUE
    if run.which('sbatch'):
        QUEUE = 'slurm'
    elif run.which('qsub'):
        QUEUE = 'torque'
    else:
        QUEUE = 'normal'
    if QUEUE == 'slurm' or QUEUE == 'torque':
        logme.log('{} detected, using for cluster submissions'.format(QUEUE),
                  'debug')
    else:
        logme.log('No cluster environment detected, using multiprocessing',
                  'debug')
    return QUEUE


##############################
#  Check if queue is usable  #
##############################


def check_queue():
    """Raise exception if QUEUE is incorrect."""
    if QUEUE not in ALLOWED_QUEUES:
        raise ClusterError('QUEUE value {} is not recognized, '.format(QUEUE) +
                           'should be: normal, torque, or slurm')


class Queue(object):

    """Handle torque, slurm, or multiprocessing objects.

    All methods are transparent and work the same regardless of queue type.

    Queue.queue is a list of jobs in the queue. For torque and slurm, this is
    all jobs in the queue for the specified user. In normal mode, it is all jobs
    added to the pool, Queue must be notified of these by adding the job object
    to the queue directly with add().

    """

    def __init__(self, user=None, queue=None):
        """Create a queue object specific to a single queue and user.

        :queue: 'torque', 'slurm', or 'normal', defaults to auto-detect.
        :user:  An optional username, if provided queue will only contain the
                jobs of that user. Not required.
                If user='self' or 'current', the current user will be used.
        """
        # Get user ID as an int UID
        if user:
            if user == 'self' or user == 'current':
                self.uid = pwd.getpwnam(environ['USER']).pw_uid
            else:
                if isinstance(user, int) or isinstance(user, str) and user.isdigit:
                    self.uid = pwd.getpwuid(int(user))
                else:
                    self.uid = pwd.getpwnam(str(user)).pw_uid
        else:
            self.uid = None
        self.user = pwd.getpwuid(self.uid).pw_name if self.uid else None

        # Support python2, which hates reciprocal import
        from .job import Job
        self._Job = Job

        # Set type
        self.qtype = queue if queue else QUEUE

        # Will contain a dict of QueueJob objects indexed by ID
        self.jobs = {}

        # Load the queue, also sets the last update time
        self._load()

    ######################################
    #  Public functions: load(), wait()  #
    ######################################

    def wait(self, jobs):
        """ Block until all jobs in jobs are complete.

        Note: update time is dependant upon the queue_update parameter in
              your ~/.cluster file.

              In addition, wait() will not return until between 1 and 3
              seconds after a job has completed, irrespective of queue_update
              time. This allows time for any copy operations to complete after
              the job exits.

        :jobs: A job or list of jobs to check. Can be one of:
                    Job or multiprocessing.pool.ApplyResulti objects, job ID
                    (int/str), or a object or a list/tuple of multiple Jobs or
                    job IDs.
        :returns:  True on success False or nothing on failure.
        """
        self.load()

        # Sanitize arguments
        if not isinstance(jobs, (list, tuple)):
            jobs = [jobs]
        for job in jobs:
            if not isinstance(job, (str, int, pool.ApplyResult, self._Job)):
                raise ClusterError('job must be int, string, or ApplyResult, ' +
                                'is {}'.format(type(job)))

        if QUEUE == 'normal':
            for job in jobs:
                if isinstance(job, self._Job):
                    job = job.pool_job
                if not isinstance(job, pool.ApplyResult):
                    raise ClusterError('jobs must be ApplyResult objects')
                job.wait()
        else:
            # Wait for 5 seconds before checking, as jobs take a while to be
            # queued sometimes
            sleep(5)

            while True:
                for jb in jobs:
                    if isinstance(jb, self._Job):
                        jb = jb.id
                    while True:
                        self.load()
                        not_found = 0
                        # Allow two seconds to elapse before job is found in queue,
                        # if it is not in the queue by then, raise exception.
                        if jb not in self.jobs:
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

    def load(self):
        """Refresh the list of jobs from the server, limit queries."""
        if int(time()) - self.last_update > int(_defaults['queue_update']):
            self._load()

    ######################
    # Internal Functions #
    ######################

    def _load(self):
        """Refresh the list of jobs from the server.

        This is the core queue interaction function of this class.
        """

        # Set the update time I don't care about microseconds
        self.last_update = int(time())

        jobs = [] # list of jobs created this session

        # Mode specific initialization
        if self.qtype == 'normal':
            # We don't need to do anything for normal mode, just make sure
            # the pool is set, jobs need to be added to the queue manually
            global POOL
            if not POOL or POOL._state != 0:
                POOL = Pool(THREADS)
            self.pool = POOL

        elif self.qtype == 'torque':
            try_count = 0
            # Get an XML queue from torque
            while True:
                try:
                    sleep(1)
                    qargs = ['qstat', '-x']
                    if self.user:
                        qargs += ['-u', self.user]
                    xmlqueue = ET.fromstring(check_output(qargs))
                except CalledProcessError:
                    print(try_count)
                    if try_count == 5:
                        raise
                    else:
                        try_count += 1
                else:
                    break

            # Create QueueJob objects for all entries that match user
            for xmljob in xmlqueue:
                job_id = int(xmljob.find('Job_Id').text.split('.')[0])
                if job_id not in self.jobs:
                    job = self.QueueJob()
                else:
                    job = self.jobs[job_id]
                jobs.append(job_id)
                job.id    = job_id
                job.name  = xmljob.find('Job_Name').text
                job.owner = xmljob.find('Job_Owner').text.split('@')[0]
                job.queue = xmljob.find('queue').text
                job_state = xmljob.find('job_state').text
                if job_state == 'Q':
                    job.state = 'pending'
                elif job_state == 'R' or job_state == 'E':
                    job.state = 'running'
                elif job_state == 'C':
                    job.state = 'complete'
                if job.state == 'pending':
                    continue
                nodes = xmljob.find('exec_host').text.split('+')
                if nodes:
                    job.nodes = []
                    for node in nodes:
                        node = node.split('/')[0]
                        if node not in job.nodes:
                            job.nodes.append(node)
                    job.nodes = ','.join(job.nodes)  # Maintain slurm consistency
                # I assume that every 'node' is a core, as that is the default
                # for torque
                job.threads  = len(nodes)
                exitcode     = xmljob.find('exit_status')
                if hasattr(exitcode, 'text'):
                    job.exitcode = exitcode.text

                # Assign the job to self.
                self.jobs[job_id] = job

        elif self.qtype == 'slurm':
            try_count = 0
            while True:
                try:
                    sleep(1)
                    qargs = ['squeue', '-h', '-O',
                             'jobid:40,name:400,userid:40,partition:40,state,' +
                             'nodelist:100,numnodes,ntpernode,exit_code']
                    if self.user:
                        qargs += ['-u', self.user]
                    squeue = [tuple(re.split(r' +', i.rstrip())) for i in \
                              check_output(qargs).decode().rstrip().split('\n')]
                except CalledProcessError:
                    if try_count == 5:
                        raise
                    else:
                        try_count += 1
                else:
                    break
            # SLURM sometimes clears the queue extremely fast, so we use sacct
            # to get old jobs by the current user
            try_count = 0
            while True:
                try:
                    sleep(1)
                    qargs = ['sacct',
                             '--format=jobid,jobname,user,partition,state,' +
                             'exitcode,ncpus,nodelist']
                    sacct = [tuple(re.split(r' +', i.rstrip())) for i in \
                             check_output(qargs).decode().rstrip().split('\n')]
                    sacct = sacct[2:]
                except CalledProcessError:
                    if try_count == 5:
                        raise
                    else:
                        try_count += 1
                else:
                    break

            # Loop through the queues
            for sjob in squeue:
                job_id = int(sjob[0])
                if job_id not in self.jobs:
                    job = self.QueueJob()
                else:
                    job = self.jobs[job_id]
                jobs.append(job_id)
                job.id = job_id
                job.name, job.owner, job.queue = sjob[1:4]
                job.state = sjob[5].lower()
                if job.state == 'pending':
                    continue
                job.nodes = sjob[6]

                # Threads is number of nodes * jobs per node
                job.threads = int(sjob[7]) * int(sjob[8])
                if job.state == 'completed' or job.state == 'failed':
                    job.exitcode = int(sjob[9])

                # Assign the job to self.
                self.jobs[job_id] = job

            # Add job info from sacct that isn't in the main queue
            for sjob in sacct:
                # Skip job steps, only index whole jobs
                if '.' in sjob[0]:
                    continue
                job_id = int(sjob[0])
                if job_id in jobs:
                    continue
                if job_id not in self.job:
                    job = self.QueueJob()
                else:
                    job = self.jobs[job_id]
                jobs.append(job_id)
                job.id = job_id
                if not job.name:
                    job.name = sjob[1]
                if not job.user:
                    job.user = sjob[2]
                if not job.queue:
                    job.queue = sjob[3]
                job.state = sjob[4].lower()
                if not job.exitcode:
                    job.exitcode = sjob[5].split(':')[-1]
                if not job.threads:
                    job.threads = int(sjob[6])
                if not job.nodes:
                    job.nodes = sjob[7]

                # Assign the job to self.
                self.jobs[job_id] = job

        # We assume that if a job just disappeared it completed
        for qjob in self.jobs.values():
            if qjob.id not in jobs:
                qjob.state = 'completed'
                qjob.disappeared = True

    def _get_jobs(self, key):
        """Return a dict of jobs where state matches key."""
        self.load()
        retjobs = {}
        for jobid, job in self.jobs.items():
            if job.state == key.lower():
                retjobs[jobid] = job
        return retjobs

    def __iter__(self):
        """Allow us to be iterable"""
        self.load()
        for jb in self.jobs.values():
            yield jb

    def __getattr__(self, key):
        """Make running and queued attributes dynamic."""
        if key == 'running' or key == 'queued' or key == 'completed':
            return self._get_jobs(key)

    def __getitem__(self, key):
        """Allow direct accessing of jobs by job id."""
        self.load()
        if isinstance(key, self._Job):
            key = key.jobid
        key = int(key)
        try:
            return self.jobs[key]
        except KeyError:
            return None

    def __len__(self):
        """Length is the total job count."""
        self.load()
        return len(self.jobs)

    def __repr__(self):
        """ For debugging. """
        self.load()
        if self.user:
            outstr = 'Queue<jobs:{};user={}>'.format(len(self), self.user)
        else:
            outstr = 'Queue<jobs:{};user=ALL>'.format(len(self))
        return outstr

    def __str__(self):
        """A list of keys."""
        self.load()
        return str(self.queue.keys())

    ##############################################
    #  A simple class to hold jobs in the queue  #
    ##############################################

    class QueueJob(object):

        """Only used for torque/slurm jobs in the queue."""

        id          = None
        name        = None
        owner       = None
        queue       = None
        state       = None
        nodes       = None
        exitcode    = None
        threads     = None
        disappeared = False

        def __init__(self):
            """No initialization needed."""
            pass

        def __repr__(self):
            """Show all info."""
            outstr = ("Queue.QueueJob<{id}:{state}({name},owner:{owner}," +
                      "queue:{queue},nodes:{nodes},threads:{threads}," +
                      "exitcode:{code})").format(
                          id=self.id, name=self.name, owner=self.owner,
                          queue=self.queue, nodes=self.nodes, code=self.exitcode,
                          threads=self.threads, state=self.state)
            if self.disappeared:
                outstr += 'DISAPPEARED>'
            else:
                outstr += '>'
            return outstr

        def __str__(self):
            """Print job ID."""
            return str(self.id)

    ################
    #  Exceptions  #
    ################

    class QueueError(Exception):

        """ Simple Exception wrapper. """

        pass


###############################################################################
#                  Expose Queue Methods as Simple Functions                   #
###############################################################################


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


