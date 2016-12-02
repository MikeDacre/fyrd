#job -*- coding: utf-8 -*-
"""
Monitor the queue for torque or slurm.

Provides a class to monitor the torque, slurm, or local queues with identical
syntax.

At its simplest, you can use it like::

    q = queue.Queue()
    q.jobs
    q.running
    q.queued
    q.complete

All of the above commands return a dictionary of:
    {job_no: Queue.QueueJob}

Queue.QueueJob classes include information on job state, owner, queue, nodes,
threads, exitcode, etc.

Queue also defines a wait() method that takes a list of job numbers, job.Job()
objects, or JobQueue.Job objects and blocks until those jobs to complete

The default cluster environment is also defined in this file as MODE, it can be
set directly or with the get_cluster_environment() function definied here.
"""
import re
import sys
import pwd      # Used to get usernames for queue
import socket   # Used to get the hostname
import getpass  # Used to get usernames for queue
from datetime import datetime as _dt
from time import time, sleep
from subprocess import check_output, CalledProcessError

# For parsing torque queues
import xml.etree.ElementTree as ET

###############################################################################
#                                Our functions                                #
###############################################################################

from . import run
from . import logme
from . import conf
from . import ClusterError

#########################
#  Which system to use  #
#########################

from . import ALLOWED_MODES

#########################################################
#  The multiprocessing pool, only used in 'local' mode  #
#########################################################

from . import local

# Funtions to import if requested
__all__ = ['Queue', 'wait', 'check_queue', 'get_cluster_environment']

# We only need the queue defaults
_defaults = conf.get_option('queue')

# This is set in the get_cluster_environment() function.
MODE = ''

# Define torque-to-slurm mappings
TORQUE_SLURM_STATES = {
    'C': 'completed',
    'E': 'completing',
    'H': 'held',  # Not a SLURM state
    'Q': 'pending',
    'R': 'running',
    'T': 'suspended',
    'W': 'running',
    'S': 'suspended',
}

# Define job states
GOOD_STATES      = ['complete', 'completed', 'special_exit']
ACTIVE_STATES    = ['configuring', 'completing', 'pending',
                    'running']
BAD_STATES       = ['boot_fail', 'cancelled', 'failed',
                    'node_fail', 'timeout']
UNCERTAIN_STATES = ['hold', 'preempted', 'stopped',
                    'suspended']
ALL_STATES = GOOD_STATES + ACTIVE_STATES + BAD_STATES + UNCERTAIN_STATES
DONE_STATES = GOOD_STATES + BAD_STATES

###############################################################################
#                               The Queue Class                               #
###############################################################################


class Queue(object):

    """A wrapper for torque, slurm, or local queues.

    Attributes:
        jobs (dict)             A dictionary of all jobs in this queue
                                {jobid: Queue.QueueJob}
        max_jobs (int):         The maximum number of jobs allowed in the queue
        job_states (int):       A list of the different states of jobs in this
                                queue
        active_job_count (int): A count of all jobs that are either queued or
                                running in the current queue
        can_submit (bool):      True if total active jobs is less than max_jobs
    """

    def __init__(self, user=None, partition=None, qtype=None,):
        """Can filter by user, queue type or partition on initialization.

        Args:
            user (str):      Optional usernameto filter the queue with.
                             If user='self' or 'current', the current user will
                             be used.
            partition (str): Optional partition to filter the queue with.
            qtype (str):     'torque', 'slurm', or 'local', defaults to auto-detect.
        """
        # Get user ID as an int UID
        if user:
            if user == 'self' or user == 'current':
                self.user = getpass.getuser()
                """The username if defined."""
                self.uid  = pwd.getpwnam(self.user).pw_uid
            elif user == 'ALL':
                self.user = None
            else:
                if isinstance(user, int) \
                        or (isinstance(user, str) and user.isdigit()):
                    self.uid  = int(user)
                else:
                    self.uid = pwd.getpwnam(str(user)).pw_uid
        else:
            self.uid = None
        self.user = pwd.getpwuid(self.uid).pw_name if self.uid else None
        self.partition = partition
        """The partition if defined."""

        # Set queue length
        self.max_jobs = conf.get_option('queue', 'max_jobs')
        """The maximum number of jobs that can run in this queue."""

        # Support python2, which hates reciprocal import
        from .job import Job
        from .local import Job as QJob
        self._Job      = Job
        self._JobQueue = QJob

        # Get sleep time and update time
        self.queue_update_time = conf.get_option('queue', 'queue_update', 2)
        self.sleep_len         = conf.get_option('queue', 'sleep_len', 2)

        # Set type
        if qtype:
            check_queue(qtype)
        else:
            check_queue()
        self.qtype = qtype if qtype else MODE

        # Allow tracking of updates to prevent too many updates
        self._updating = False

        # Will contain a dict of QueueJob objects indexed by ID
        self.jobs = {}
        """All jobs currently in this queue."""

        self._update()

    ########################################
    #  Public functions: update(), wait()  #
    ########################################

    def wait(self, jobs):
        """Block until all jobs in jobs are complete.

        Update time is dependant upon the queue_update parameter in
        your ~/.fyrd file.

        In addition, wait() will not return until between 1 and 3
        seconds after a job has completed, irrespective of queue_update
        time. This allows time for any copy operations to complete after
        the job exits.

        Args:
            jobs:  A job or list of jobs to check. Can be one of: Job or
                   multiprocessing.pool.ApplyResult objects, job ID (int/str),
                   or a object or a list/tuple of multiple Jobs or job IDs.

        Returns:
            True on success False or None on failure.
        """
        self.update()
        logme.log('Queue waiting.', 'debug')

        # Sanitize arguments
        if not isinstance(jobs, (list, tuple)):
            jobs = [jobs]
        for job in jobs:
            if not isinstance(job, (str, int, self.QueueJob, self._Job,
                                    self._JobQueue)):
                raise ClusterError('job must be int, string, or Job, ' +
                                   'is {}'.format(type(job)))

        # Wait for 0.1 second before checking, as jobs take a while to be
        # queued sometimes
        sleep(0.1)
        for job in jobs:
            logme.log('Checking {}'.format(job), 'debug')
            qtype = job.qtype if isinstance(job, self._Job) else self.qtype
            if isinstance(job, (self._Job, self._JobQueue, self.QueueJob)):
                job = job.id
            if qtype == 'local':
                logme.log('Job is in local queue', 'debug')
                try:
                    job = int(job)
                except TypeError:
                    raise TypeError('Job must be a Job object or job #.')
                if not local.JQUEUE \
                        or not local.JQUEUE.runner.is_alive():
                    raise ClusterError('Cannot wait on job ' + str(job) +
                                       'JobQueue does not exist')
                local.JQUEUE.wait(job)
            else:
                logme.log('Job is in remote queue', 'debug')
                if isinstance(job, self._Job):
                    job = job.id
                job = int(job)
                not_found = 0
                lgd = False
                while True:
                    self._update()
                    # Allow 12 seconds to elapse before job is found in queue,
                    # if it is not in the queue by then, raise exception.
                    if job not in self.jobs:
                        if lgd:
                            logme.log('Attempt #{}/12'.format(not_found),
                                      'debug')
                        else:
                            logme.log('{} not in queue, waiting up to 12s '
                                      .format(job) +
                                      'for it to appear', 'info')
                            lgd = True
                        sleep(1)
                        not_found += 1
                        if not_found == 12:
                            raise QueueError(
                                '{} not in queue, tried 12 times over 12s'
                                .format(job)
                            )
                        continue
                    ## Actually look for job in running/queued queues
                    lgd      = False
                    lgd2     = False
                    start    = _dt.now()
                    res_time = conf.get_option('queue', 'res_time')
                    count    = 0
                    # Get job state
                    job_state = self.jobs[job].state
                    # Check the state
                    if job_state in GOOD_STATES:
                        logme.log('Queue wait for {} complete'
                                  .format(job), 'debug')
                        sleep(0.1)
                        break
                    elif job_state in ACTIVE_STATES:
                        if lgd:
                            logme.log('{} not complete yet, waiting'
                                      .format(job), 'debug')
                            lgd = True
                        else:
                            logme.log('{} still not complete, waiting'
                                      .format(job), 'verbose')
                        sleep(self.sleep_len)
                    elif job_state in BAD_STATES:
                        logme.log('Job {} failed with state {}'
                                  .format(job, job_state), 'error')
                        return False
                    elif job_state in UNCERTAIN_STATES:
                        if not lgd2:
                            logme.log('Job {} in state {}, waiting {} '
                                      .format(job, job_state, res_time) +
                                      'seconds for resolution', 'warn')
                            lgd2 = True
                        if (_dt.now() - start).seconds > res_time:
                            logme.log('Job {} still in state {}, aborting'
                                      .format(job, job_state), 'error')
                            return False
                        sleep(self.sleep_len)
                    else:
                        if count == 5:
                            logme.log('Job {} in unknown state {} '
                                      .format(job, job_state) +
                                      'cannot continue', 'critical')
                            raise QueueError('Unknown job state {}'
                                             .format(job_state))
                        logme.log('Job {} in unknown state {} '
                                  .format(job, job_state) +
                                  'trying to resolve', 'debug')
                        count += 1
                        sleep(self.sleep_len)

        # Sleep an extra half second to allow post-run scripts to run
        sleep(0.5)
        return True

    def wait_to_submit(self, max_jobs=None):
        """Block until fewer running/queued jobs in queue than max_jobs.

        Args:
            max_jobs (int): Override self.max_jobs
        """
        count   = 50
        written = False
        if max_jobs:
            self.max_jobs = int(max_jobs)
        while True:
            if self.can_submit:
                return
            if not written:
                logme.log(('The queue is full, there are {} jobs running and '
                           '{} jobs queued. Will wait to submit, retrying '
                           'every {} seconds.')
                          .format(len(self.running), len(self.queued),
                                  self.sleep_len),
                          'info')
                written = True
            if count == 0:
                logme.log('Still waiting to submit.', 'info')
                count = 50
            count -= 1
            sleep(self.sleep_len)

    def update(self):
        """Refresh the list of jobs from the server, limit queries."""
        if int(time()) - self.last_update > self.queue_update_time:
            self._update()
        else:
            logme.log('Skipping update as last update too recent', 'debug')
        return self

    def get_jobs(self, key):
        """Return a dict of jobs where state matches key."""
        retjobs = {}
        for jobid, job in self.jobs.items():
            if job.state == key.lower():
                retjobs[jobid] = job
        return retjobs

    def get_user_jobs(self, users):
        """Filter jobs by user.

        Args:
            users (list): A list of users/owners

        Returns:
            dict: A filtered job dictionary of {job_id: QueueJob} for all jobs
                  owned by the queried users.
        """
        try:
            if isinstance(users, (str, int)):
                users = [users]
            else:
                users = list(users)
        except TypeError:
            users = [users]
        return {k: v for k, v in self.jobs.items() if v.owner in users}

    @property
    def users(self):
        """Return a list of users with jobs running."""
        return [job.owner for job in self.jobs.values()]

    @property
    def job_states(self):
        """Return a list of job states for all jobs in the queue."""
        return [job.state for job in self.jobs]

    @property
    def finished(self):
        """Return a list of jobs that are neither queued nor running."""
        return {i: j for i, j in self.jobs.items() \
                if j.state not in ACTIVE_STATES}

    @property
    def bad(self):
        """Return a list of jobs that have bad or uncertain states."""
        return {i: j for i, j in self.jobs.items() \
                if j.state in BAD_STATES or j.state in UNCERTAIN_STATES}

    @property
    def active_job_count(self):
        """Return a count of all queued or running jobs."""
        return sum([
            len(j) for j in [
                self.get_jobs(i) for i in ACTIVE_STATES
            ]
        ])

    @property
    def can_submit(self):
        """Return True if R/Q jobs are less than max_jobs.

        If max_jobs is None, default from config is used.
        """
        self.update()
        return self.active_job_count < self.max_jobs

    ######################
    # Internal Functions #
    ######################

    def _update(self):
        """Refresh the list of jobs from the server.

        This is the core queue interaction function of this class.
        """
        if self._updating:
            return
        logme.log('Queue updating', 'debug')
        # Set the update time I don't care about microseconds
        self.last_update = int(time())

        jobs = []  # list of jobs created this session

        # Mode specific initialization
        if self.qtype == 'local':
            if not local.JQUEUE or not local.JQUEUE.runner.is_alive():
                local.JQUEUE = local.JobQueue(cores=local.THREADS)
            for job_id, job_info in local.JQUEUE:
                if job_id in self.jobs:
                    job = self.jobs[job_id]
                else:
                    job = self.QueueJob()
                job.id     = job_id
                job.name   = job_info.function.__name__
                job.owner  = self.user
                self.nodes = socket.gethostname()
                if job_info.state == 'Not Submitted':
                    job.state = 'pending'
                elif job_info.state == 'waiting' \
                        or job_info.state == 'submitted':
                    job.state = 'pending'
                elif job_info.state == 'started' \
                        or job_info.state == 'running':
                    job.state = 'running'
                elif job_info.state == 'done':
                    job.state = 'completed'
                    job.exitcode = int(job_info.exitcode)
                else:
                    raise Exception('Unrecognized state')

                # Assign the job to self.
                self.jobs[job_id] = job
                jobs.append(job_id)

        else:
            for [job_id, job_name, job_user, job_partition,
                 job_state, job_nodelist, job_nodecount,
                 job_cpus, job_exitcode] in queue_parser(self.qtype,
                                                         self.user,
                                                         self.partition):
                if job_id not in self.jobs:
                    job = self.QueueJob()
                else:
                    job = self.jobs[job_id]
                jobs.append(job_id)
                job.id    = job_id
                job.name  = job_name
                job.owner = job_user
                job.queue = job_partition
                job.state = job_state.lower()
                job.nodes = job_nodelist

                # Threads is number of nodes * jobs per node
                if job_nodecount and job_cpus:
                    job.threads = int(job_nodecount) * int(job_cpus)
                else:
                    job.threads = None
                if job.state == 'completed' or job.state == 'failed':
                    job.exitcode = job_exitcode

                # Assign the job to self.
                self.jobs[job_id] = job
                jobs.append(job_id)

        # We assume that if a job just disappeared it completed
        if self.jobs:
            for qjob in self.jobs.values():
                if qjob.id not in jobs:
                    qjob.state = 'completed'
                    qjob.disappeared = True

    def __getattr__(self, key):
        """Make running and queued attributes dynamic."""
        key = key.lower()
        if key in TORQUE_SLURM_STATES:
            key = TORQUE_SLURM_STATES[key]
        if key == 'complete':
            key = 'completed'
        elif key == 'queued':
            key = 'pending'
        if key in ALL_STATES:
            return self.get_jobs(key)

    def __getitem__(self, key):
        """Allow direct accessing of jobs by job id."""
        if isinstance(key, self._Job):
            key = key.jobid
        key = int(key)
        try:
            return self.jobs[key]
        except KeyError:
            return None

    def __iter__(self):
        """Allow us to be iterable."""
        for jb in self.jobs.values():
            yield jb

    def __len__(self):
        """Length is the total job count."""
        return len(self.jobs)

    def __repr__(self):
        """For debugging."""
        self._updating = True
        if self.user:
            outstr = 'Queue<jobs:{};completed:{};queued:{};user={}>'.format(
                len(self), len(self.complete), len(self.queued), self.user)
        else:
            outstr = 'Queue<jobs:{};completed:{};queued:{};user=ALL>'.format(
                len(self), len(self.complete), len(self.queued))
        self._updating = False
        return outstr

    def __str__(self):
        """A list of keys."""
        return str(self.jobs.keys())

    ##############################################
    #  A simple class to hold jobs in the queue  #
    ##############################################

    class QueueJob(object):

        """A very simple class to store info about jobs in the queue.

        Only used for torque and slurm queues.

        Attributes:
            id (int):           Job ID
            name (str):         Job name
            owner (str):        User who owns the job
            threads (int):      Number of cores used by the job
            queue (str):        The queue/partition the job is running in
            state (str):        Current state of the job, normalized to slurm
                                states
            nodes (list):       List of nodes job is running on
            exitcode (int):     Exit code of completed job
            disappeared (bool): Job cannot be found in the queue anymore
        """

        id          = None
        name        = None
        owner       = None
        threads     = None
        queue       = None
        state       = None
        nodes       = None
        exitcode    = None
        disappeared = False

        def __init__(self):
            """No initialization needed all attributes are set elsewhere."""
            pass

        def __repr__(self):
            """Show all info."""
            outstr = ("Queue.QueueJob<{id}:{state}({name},owner:{owner}," +
                      "queue:{queue},nodes:{nodes},threads:{threads}," +
                      "exitcode:{code})").format(
                          id=self.id, name=self.name, owner=self.owner,
                          queue=self.queue, nodes=self.nodes,
                          code=self.exitcode, threads=self.threads,
                          state=self.state)
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

    """Simple Exception wrapper."""

    pass


###############################################################################
#                             Non-Class Functions                             #
###############################################################################


###################
#  Queue Parsers  #
###################

def queue_parser(qtype=None, user=None, partition=None):
    """Call either torque or slurm qtype parsers depending on qtype.

    Args:
        qtype: Either 'torque' or 'slurm', defaults to current MODE
        user:  optional user name to pass to queue to filter queue with

    Yields:
        tuple: job_id, name, userid, partition, state, nodelist, numnodes,
               ntpernode, exit_code
    """
    if not qtype:
        qtype = get_cluster_environment()
    if qtype == 'torque':
        return torque_queue_parser(user, partition)
    elif qtype == 'slurm':
        return slurm_queue_parser(user, partition)
    else:
        raise ClusterError("Invalid qtype type {}, must be 'torque' or 'slurm'"
                           .format(qtype))


def torque_queue_parser(user=None, partition=None):
    """Iterator for torque queues.

    Use the `qstat -x` command to get an XML queue for compatibility.

    Args:
        user:     optional user name to pass to qstat to filter queue with
        partiton: optional partition to filter the queue with

    Yields:
        tuple: job_id, name, userid, partition, state, nodelist, numnodes,
               ntpernode, exit_code

    numcpus is currently always 1 as most torque queues treat every core as a
    node.
    """
    # I am not using run.cmd because I want to catch XML errors also
    try_count = 0
    qargs = ['qstat', '-x']
    while True:
        try:
            xmlqueue = ET.fromstring(check_output(qargs))
        except CalledProcessError:
            sleep(1)
            if try_count == 5:
                raise
            else:
                try_count += 1
        except ET.ParseError:
            # ElementTree throws error when string is empty
            sleep(1)
            if try_count == 1:
                xmlqueue = None
                break
            else:
                try_count += 1
        else:
            break

    # Create QueueJob objects for all entries that match user
    if xmlqueue is not None:
        for xmljob in xmlqueue:
            job_id    = int(xmljob.find('Job_Id').text.split('.')[0])
            job_owner = xmljob.find('Job_Owner').text.split('@')[0]
            if user and job_owner != user:
                continue
            job_name  = xmljob.find('Job_Name').text
            job_queue = xmljob.find('queue').text
            job_state = xmljob.find('job_state').text
            job_state = TORQUE_SLURM_STATES[job_state]
            logme.log('Job {} state: {}'.format(job_id, job_state),
                      'debug')
            ndsx = xmljob.find('exec_host')
            if ndsx:
                nds = ndsx.text.split('+')
            else:
                nds = []
            nodes = []
            for node in nds:
                if '-' in node:
                    nm, num = node.split('/')
                    for i in range(*[int(i) for i in num.split('-')]):
                        nodes.append(nm + '/' + str(i).zfill(2))
                else:
                    nodes.append(node)
            # I assume that every 'node' is a core, as that is the
            # default for torque, but it isn't always true
            job_threads  = len(nodes)
            exitcode     = xmljob.find('exit_status')
            if hasattr(exitcode, 'text'):
                exitcode = int(exitcode.text)

            if partition and job_queue != partition:
                continue
            yield (job_id, job_name, job_owner, job_queue, job_state,
                   nodes, job_threads, 1, exitcode)


def slurm_queue_parser(user=None, partition=None):
    """Iterator for slurm queues.

    Use the `squeue -O` command to get standard data across implementation,
    supplement this data with the results of `sacct`. sacct returns data only
    for the current user but retains a much longer job history. Only jobs not
    returned by squeue are added with sacct, and they are added to *the end* of
    the returned queue, i.e. *out of order with respect to the actual queue*.

    Args:
        user:      optional user name to filter queue with
        partition: optional partition to filter queue with

    Yields:
        tuple: job_id, name, userid, partition, state, nodelist, numnodes,
               ntpernode, exit_code
    """
    nodequery = re.compile(r'([^\[,]+)(\[[^\[]+\])?')
    qargs = ['squeue', '-h', '-O',
             'jobid:400,name:400,userid:400,partition:400,state:400,' +
             'nodelist:400,numnodes:400,numcpus:400,exit_code:400']
    # Parse queue info by length
    squeue = [
        tuple(
            [k[i:i+200].rstrip() for i in range(0, 3600, 400)]
        ) for k in run.cmd(qargs)[1].split('\n')
    ]
    # SLURM sometimes clears the queue extremely fast, so we use sacct
    # to get old jobs by the current user
    qargs = ['sacct', '-p',
             '--format=jobid,jobname,user,partition,state,' +
             'nodelist,reqnodes,ncpus,exitcode']
    try:
        sacct = [tuple(i.strip(' |').split('|')) for i in
                 run.cmd(qargs)[1].split('\n')]
        sacct = sacct[1:]
    # This command isn't super stable and we don't care that much, so I will
    # just let it die no matter what
    except Exception as e:
        if logme.MIN_LEVEL == 'debug':
            raise e
        else:
            sacct = []

    if sacct:
        if len(sacct[0]) != 9:
            logme.log('sacct parsing failed unexpectedly as there are not ' +
                      '9 columns, aborting.', 'critical')
            raise ValueError('sacct output does not have 9 columns. Has:' +
                             '{}: {}'.format(len(sacct[0]), sacct[0]))
        jobids = [i[0] for i in squeue]
        for sinfo in sacct:
            # Skip job steps, only index whole jobs
            if '.' in sinfo[0]:
                logme.log('Skipping {} '.format(sinfo[0]) +
                          "in sacct processing as it is a job part.",
                          'verbose')
                continue
            # These are the values I expect
            try:
                [sid, sname, suser, spartition, sstate,
                 snodelist, snodes, scpus, scode] = sinfo
            except ValueError as err:
                logme.log('sacct parsing failed with error {} '.format(err) +
                          'due to an incorrect number of entries.\n' +
                          'Contents of sinfo:\n{}\n'.format(sinfo) +
                          'Expected 9 values\n:' +
                          '[sid, sname, suser, spartition, sstate, ' +
                          'snodelist, snodes, scpus, scode]',
                          'critical')
                raise
            # Skip jobs that were already in squeue
            if sid in jobids:
                logme.log('{} still in squeue output'.format(sid), 'verbose')
                continue
            scode = int(scode.split(':')[-1])
            squeue.append((sid, sname, suser, spartition, sstate,
                           snodelist, snodes, scpus, scode))
    else:
        logme.log('No job info in sacct', 'debug')

    # Sanitize data
    for sinfo in squeue:
        if len(sinfo) == 9:
            [sid, sname, suser, spartition, sstate, sndlst,
             snodes, scpus, scode] = sinfo
        else:
            sys.stderr.write('{}'.format(repr(sinfo)))
            raise ClusterError('Queue parsing error, expected 8 or 9 items '
                               'in output of squeue and sacct, got {}\n'
                               .format(len(sinfo)))
        if partition and spartition != partition:
            continue
        if not isinstance(sid, int):
            sid = int(sid) if sid else None
        if not isinstance(snodes, int):
            snodes = int(snodes) if snodes else None
        if not isinstance(scpus, int):
            scpus = int(scpus) if snodes else None
        if not isinstance(scode, int):
            scode = int(scode) if scode else None
        # Convert user from ID to name
        if suser.isdigit():
            suser = pwd.getpwuid(int(suser)).pw_name
        if user and suser != user:
            continue
        # Attempt to parse nodelist
        snodelist = []
        if sndlst:
            if nodequery.search(sndlst):
                nsplit = nodequery.findall(sndlst)
                for nrg in nsplit:
                    node, rge = nrg
                    if not rge:
                        snodelist.append(node)
                    else:
                        for reg in rge.strip('[]').split(','):
                            # Node range
                            if '-' in reg:
                                start, end = [int(i) for i in reg.split('-')]
                                for i in range(start, end):
                                    snodelist.append('{}{}'.format(node, i))
                            else:
                                snodelist.append('{}{}'.format(node, reg))
            else:
                snodelist = sndlst.split(',')

        yield (sid, sname, suser, spartition, sstate, snodelist,
               snodes, scpus, scode)


###########################################################
#  Set the global cluster type: slurm, torque, or local  #
###########################################################


def get_cluster_environment():
    """Detect the local cluster environment and set MODE globally.

    Uses which to search for sbatch first, then qsub. If neither is found,
    MODE is set to local.

    Returns:
        tuple: MODE variable ('torque', 'slurm', or 'local')
    """
    global MODE
    conf_queue = conf.get_option('queue', 'queue_type', 'auto')
    if conf_queue not in ['torque', 'slurm', 'local', 'auto']:
        logme.log('queue_type in the config file is {}, '.format(conf_queue) +
                  'but it should be one of torque, slurm, local, or auto. ' +
                  'Resetting it to auto', 'warn')
        conf.set_option('queue', 'queue_type', 'auto')
        conf_queue = 'auto'
    if conf_queue == 'auto':
        if run.which('sbatch'):
            MODE = 'slurm'
        elif run.which('qsub'):
            MODE = 'torque'
        else:
            MODE = 'local'
    else:
        MODE = conf_queue
    if MODE == 'slurm' or MODE == 'torque':
        logme.log('{} detected, using for cluster submissions'.format(MODE),
                  'debug')
    else:
        logme.log('No cluster environment detected, using multiprocessing',
                  'debug')
    return MODE


##############################
#  Check if queue is usable  #
##############################


def check_queue(qtype=None):
    """Raise exception if MODE is incorrect."""
    if 'MODE' not in globals():
        global MODE
        MODE = get_cluster_environment()
    if not MODE:
        MODE = get_cluster_environment()
    if qtype:
        if qtype not in ALLOWED_MODES:
            raise ClusterError('qtype value {} is not recognized, '
                               .format(qtype) +
                               'should be: local, torque, or slurm')
        else:
            if MODE not in ALLOWED_MODES:
                MODE = qtype
            return True
    elif MODE not in ALLOWED_MODES:
        raise ClusterError('MODE value {} is not recognized, '.format(MODE) +
                           'should be: local, torque, or slurm')


######################################################################
#  Expose the Queue waiting method without requiring a Queue object  #
######################################################################


def wait(jobs):
    """Wait for jobs to finish.

    Args:
        jobs: A single job or list of jobs to wait for. With torque or slurm,
              these should be job IDs, with local mode, these are
              multiprocessing job objects (returned by submit())
    """
    # Support python2, which hates reciprocal import for 80's reasons
    from .job import Job
    from .local import JobQueue

    check_queue()  # Make sure the MODE is usable

    # Sanitize argument
    if not isinstance(jobs, (list, tuple)):
        jobs = [jobs]
    for job in jobs:
        if not isinstance(job, (str, int, Job, JobQueue)):
            raise ClusterError('job must be int, string, or Job, ' +
                               'is {}'.format(type(job)))

    if MODE == 'local':
        for job in jobs:
            try:
                job = int(job)
            except TypeError:
                raise TypeError('Job must be a Job object or job #.')
            if not local.JQUEUE or not local.JQUEUE.runner.is_alive():
                raise ClusterError('Cannot wait on job ' + str(job) +
                                   'JobQueue does not exist')
            local.JQUEUE.wait(job)

    elif MODE == 'torque':
        # Wait for 1 seconds before checking, as jobs take a while to be queued
        # sometimes
        sleep(1)

        s = re.compile(r' +')  # For splitting qstat output
        # Jobs must be strings for comparison operations
        jobs = [str(j) for j in jobs]
        q = run.cmd('qstat -a', tries=8)[1].rstrip().split('\n')
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
        alljobs  = [s.split(j)[0].split('.')[0] for j in q[5:]]
        # Trim down job list
        jobs = [j for j in jobs if j in alljobs]
        jobs = [j for j in jobs if j not in complete]
        if len(jobs) == 0:
            return
    elif MODE == 'slurm':
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
            allj     = [i[0] for i in q]
            # Trim down job list, ignore failures
            jobs = [i for i in jobs if i not in allj]
            jobs = [i for i in jobs if i not in complete]
            jobs = [i for i in jobs if i not in failed]
            if len(jobs) == 0:
                return
            sleep(2)
