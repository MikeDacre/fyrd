"""
Monitor the queue for torque or slurm.

===============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2015-12-11
 Last modified: 2016-08-31 17:12

   DESCRIPTION: Provides a class to monitor the torque, slurm, or local
                jobqueue queues with identical syntax.

                At its simplest, you can use it like::
                    q = queue.Queue()
                    q.jobs
                    q.running
                    q.queued
                    q.complete

                All of the above commands return a dictionary of:
                    job_no: Queue.QueueJob

                Queue.QueueJob classes include information on job state, owner,
                queue, nodes, threads, exitcode, etc.

                Queue also defines a wait() method that takes a list of job
                numbers, job.Job() objects, or JobQueue.Job objects and
                blocks until those jobs to complete

                The default cluster environment is also defined in this file
                as MODE, it can be set directly or with the
                get_cluster_environment() function definied here.

===============================================================================
"""
import os
import re
import sys
import pwd      # Used to get usernames for queue
import socket   # Used to get the hostname
import getpass  # Used to get usernames for queue
from time import time, sleep
from subprocess import check_output, CalledProcessError

# For parsing torque queues
import xml.etree.ElementTree as ET

###############################################################################
#                                Our functions                                #
###############################################################################

from . import run
from . import logme
from . import config_file
from . import ClusterError

#########################
#  Which system to use  #
#########################

from . import ALLOWED_MODES

#########################################################
#  The multiprocessing pool, only used in 'local' mode  #
#########################################################

from . import jobqueue
from . import DEFAULTS

# Funtions to import if requested
__all__ = ['Queue', 'wait', 'check_queue', 'get_cluster_environment']

# We only need the queue defaults
_defaults = DEFAULTS['queue']

# This is set in the get_cluster_environment() function.
MODE = ''


###############################################################################
#                               The Queue Class                               #
###############################################################################


class Queue(object):

    """Handle torque, slurm, or multiprocessing objects.

    All methods are transparent and work the same regardless of queue type.

    Queue.queue is a list of jobs in the queue. For torque and slurm, this is
    all jobs in the queue for the specified user. In local mode, it is all jobs
    added to the pool, Queue must be notified of these by adding the job object
    to the queue directly with add().

    """

    def __init__(self, user=None, qtype=None, partition=None):
        """Create a queue object specific to a single queue and user.

        :qtype:     'torque', 'slurm', or 'local', defaults to auto-detect.
        :user:      Optional usernameto filter the queue with.
                    If user='self' or 'current', the current user will be used.
        :partition: Optional partition to filter the queue with.
        """
        # Get user ID as an int UID
        if user:
            if user == 'self' or user == 'current':
                self.user = getpass.getuser()
                self.uid  = pwd.getpwnam(self.user).pw_uid
            elif user == 'ALL':
                self.user = None
            else:
                if isinstance(user, int) or (isinstance(user, str)
                                             and user.isdigit()):
                    self.uid  = int(user)
                else:
                    self.uid = pwd.getpwnam(str(user)).pw_uid
        else:
            self.uid = None
        self.user = pwd.getpwuid(self.uid).pw_name if self.uid else None
        self.partition = partition

        # Support python2, which hates reciprocal import
        from .job import Job
        from .jobqueue import Job as QJob
        self._Job      = Job
        self._JobQueue = QJob

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

        self._update()

    ########################################
    #  Public functions: update(), wait()  #
    ########################################

    def wait(self, jobs):
        """ Block until all jobs in jobs are complete.

        Note: update time is dependant upon the queue_update parameter in
              your ~/.cluster file.

              In addition, wait() will not return until between 1 and 3
              seconds after a job has completed, irrespective of queue_update
              time. This allows time for any copy operations to complete after
              the job exits.

        :jobs: A job or list of jobs to check. Can be one of:
                    Job or multiprocessing.pool.ApplyResult objects, job ID
                    (int/str), or a object or a list/tuple of multiple Jobs or
                    job IDs.
        :returns:  True on success False or nothing on failure.
        """
        self.update()

        # Sanitize arguments
        if not isinstance(jobs, (list, tuple)):
            jobs = [jobs]
        for job in jobs:
            if not isinstance(job, (str, int, self._Job, self._JobQueue)):
                raise ClusterError('job must be int, string, or Job, ' +
                                   'is {}'.format(type(job)))

        # Wait for 1 second before checking, as jobs take a while to be
        # queued sometimes
        sleep(1)
        for job in jobs:
            qtype = job.qtype if isinstance(job, self._Job) else self.qtype
            if isinstance(job, (self._Job, self._JobQueue)):
                job = job.id
            if qtype == 'local':
                try:
                    job = int(job)
                except TypeError:
                    raise TypeError('Job must be a Job object or job #.')
                if not jobqueue.JQUEUE \
                        or not jobqueue.JQUEUE.runner.is_alive():
                    raise ClusterError('Cannot wait on job ' + str(job) +
                                       'JobQueue does not exist')
                jobqueue.JQUEUE.wait(job)
            else:
                if isinstance(job, self._Job):
                    job = job.id
                not_found = 0
                while True:
                    self.update()
                    # Allow two seconds to elapse before job is found in queue,
                    # if it is not in the queue by then, raise exception.
                    if job not in self.jobs:
                        sleep(1)
                        self.update()
                        not_found += 1
                        if not_found == 6:
                            raise self.QueueError(
                                '{} not in queue'.format(job))
                        continue
                    # Actually look for job in running/queued queues
                    if job in self.running.keys() or job in self.queued.keys():
                        sleep(2)
                    else:
                        break

        sleep(1)  # Sleep an extra second to allow post-run scripts to run.
        return True

    def can_submit(self, max_queue_len=None):
        """Return True if R/Q jobs are less than max_queue_len.

        If max_queue_len is None, default from config is used.
        """
        qlen = max_queue_len if max_queue_len else \
            config_file.get_option('queue', 'max_jobs', 3000)
        qlen = int(qlen)
        assert qlen > 0
        self.update()
        return True if len(self.queued)+len(self.running) < qlen else False


    def wait_to_submit(self, max_queue_len=None):
        """Wait until R/Q jobs are less than max_queue_len.

        If max_queue_len is None, default from config is used.
        """
        sleep_len = config_file.get_option('queue', 'sleep_len', 5)
        count   = 50
        written = False
        while True:
            if self.can_submit(max_queue_len):
                return
            if not written:
                logme.log(('The queue is full, there are {} jobs running and '
                           '{} jobs queued. Will wait to submit, retrying '
                           'every {} seconds.')
                          .format(len(self.running), len(self.queued),
                                  sleep_len),
                          'info')
                written = True
            if count == 0:
                logme.log('Still waiting to submit.', 'info')
                count = 50
            count -= 1
            sleep(sleep_len)

    def update(self):
        """Refresh the list of jobs from the server, limit queries."""
        if int(time()) - self.last_update > int(_defaults['queue_update']):
            self._update()
        else:
            logme.log('Skipping update as last update too recent', 'debug')
        return self

    ######################
    # Internal Functions #
    ######################

    def _update(self):
        """Refresh the list of jobs from the server.

        This is the core queue interaction function of this class.
        """
        if self._updating:
            return
        # Set the update time I don't care about microseconds
        self.last_update = int(time())

        jobs = [] # list of jobs created this session

        # Mode specific initialization
        if self.qtype == 'local':
            if not jobqueue.JQUEUE or not jobqueue.JQUEUE.runner.is_alive():
                jobqueue.JQUEUE = jobqueue.JobQueue(cores=jobqueue.THREADS)
            for job_id, job_info in jobqueue.JQUEUE:
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
                job.threads = int(job_nodecount) * int(job_cpus)
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

    def _get_jobs(self, key):
        """Return a dict of jobs where state matches key."""
        if self._updating:
            in_progress = True
        else:
            self.update()
            self._updating = True
        retjobs = {}
        for jobid, job in self.jobs.items():
            if job.state == key.lower():
                retjobs[jobid] = job
        if not in_progress:
            self._updating = False
        return retjobs

    def __getattr__(self, key):
        """Make running and queued attributes dynamic."""
        key = key.lower()
        if key == 'complete':
            key = 'completed'
        elif key == 'queued':
            key = 'pending'
        if key == 'running' or key == 'pending' or key == 'completed':
            return self._get_jobs(key)
        # Define whether the queue is open
        if key == 'can_submit':
            return self.can_submit()

    def __getitem__(self, key):
        """Allow direct accessing of jobs by job id."""
        self.update()
        if isinstance(key, self._Job):
            key = key.jobid
        key = int(key)
        try:
            return self.jobs[key]
        except KeyError:
            return None

    def __iter__(self):
        """Allow us to be iterable"""
        self.update()
        for jb in self.jobs.values():
            yield jb

    def __len__(self):
        """Length is the total job count."""
        self.update()
        return len(self.jobs)

    def __repr__(self):
        """For debugging."""
        # For this particular function, enforce 3s minimum update time
        if int(time()) - self.last_update > 3:
            self.update()
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
        # For this particular function, enforce 3s minimum update time
        if int(time()) - self.last_update > 3:
            self.update()
        return str(self.jobs.keys())

    ##############################################
    #  A simple class to hold jobs in the queue  #
    ##############################################

    class QueueJob(object):

        """Only used for torque/slurm jobs in the queue."""

        id          = None
        name        = None
        owner       = None
        user        = None
        queue       = None
        state       = None
        nodes       = None
        exitcode    = None
        threads     = None
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

        """ Simple Exception wrapper. """

        pass


###############################################################################
#                             Non-Class Functions                             #
###############################################################################


###################
#  Queue Parsers  #
###################

def queue_parser(qtype=None, user=None, partition=None):
    """Call either torque or slurm qtype parsers depending on qtype.

    :qtype:   Either 'torque' or 'slurm', defaults to current MODE
    :user:    optional user name to pass to queue to filter queue with

    :yields:  job_id, name, userid, partition, state, nodelist,
              numnodes, ntpernode, exit_code
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

    :user:     optional user name to pass to qstat to filter queue with
    :partiton: optional partition to filter the queue with

    :yields:   job_id, name, userid, partition, state, nodelist,
               numnodes, ntpernode, exit_code

    numcpus is currently always 1 as most torque queues treat every core as a
    node.
    """
    # I am not using run.cmd because I want to catch XML errors also
    try_count = 0
    qargs = ['qstat', '-x']
    while True:
        try:
            sleep(1)
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
            if job_state == 'Q':
                job_state = 'pending'
            elif job_state == 'R' or job_state == 'E':
                job_state = 'running'
            elif job_state == 'C':
                job_state = 'completed'
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

    :user:      optional user name to filter queue with
    :partition: optional partition to filter queue with

    :yields:    job_id, name, userid, partition, state, nodelist,
                numnodes, ntpernode, exit_code
    """
    nodequery = re.compile(r'([^\[,]+)(\[[^\[]+\])?')
    qargs = ['squeue', '-h', '-O',
             'jobid:400,name:400,userid:400,partition:400,state:400,' +
             'nodelist:400,numnodes:400,numcpus:400,exit_code:400']
    # Parse queue info by length
    squeue = [
        tuple(
            [ k[i:i+200].rstrip() for i in range(0, 3600, 400) ]
        ) for k in run.cmd(qargs)[1].split('\n')
    ]
    # SLURM sometimes clears the queue extremely fast, so we use sacct
    # to get old jobs by the current user
    qargs = ['sacct', '-p',
             '--format=jobid,jobname,user,partition,state,' +
             'nodelist,reqnodes,ncpus,exitcode']
    try:
        sacct = [tuple(re.split(r'|', i.rstrip())) for i in \
                 run.cmd(qargs)[1].split('\n')]
        sacct = sacct[2:]
    # This command isn't super stable and we don't care that much, so I will
    # just let it die no matter what
    except:
        logme.log('Sacct failed', 'debug')
        sacct = []

    if sacct and len(sacct[0]) == 9:
        jobids = [i[0] for i in squeue]
        for sinfo in sacct:
            # Skip job steps, only index whole jobs
            if '.' in sinfo[0]:
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
                continue
            scode = int(scode.split(':')[-1])
            squeue.append((sid, sname, suser, spartition, sstate,
                           snodelist, snodes, scpus, scode))

    # Sanitize data
    outqueue = []
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
        sid    = int(sid) if sid else None
        snodes = int(snodes) if snodes else None
        scpus  = int(scpus) if snodes else None
        scode  = int(scode) if scode else None
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

        outqueue.append((sid, sname, suser, spartition, sstate, snodelist,
                         snodes, scpus, scode))
    if outqueue:
        for sinfo in outqueue:
            yield sinfo


###########################################################
#  Set the global cluster type: slurm, torque, or local  #
###########################################################


def get_cluster_environment():
    """Detect the local cluster environment and set MODE globally.

    Uses which to search for sbatch first, then qsub. If neither is found,
    MODE is set to local.

    :returns: MODE variable ('torque', 'slurm', or 'local')
    """
    global MODE
    if run.which('sbatch'):
        MODE = 'slurm'
    elif run.which('qsub'):
        MODE = 'torque'
    else:
        MODE = 'local'
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
            raise ClusterError('qtype value {} is not recognized, '.format(qtype) +
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

    :jobs:    A single job or list of jobs to wait for. With torque or slurm,
              these should be job IDs, with local mode, these are
              multiprocessing job objects (returned by submit())
    """
    # Support python2, which hates reciprocal import for 80's reasons
    from .job import Job
    from .jobqueue import JobQueue

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
            if not jobqueue.JQUEUE or not jobqueue.JQUEUE.runner.is_alive():
                raise ClusterError('Cannot wait on job ' + str(job) +
                                   'JobQueue does not exist')
            jobqueue.JQUEUE.wait(job)

    elif MODE == 'torque':
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
            alljobs  = [s.split(j)[0].split('.')[0] for j in q[5:]]
            # Trim down job list
            jobs = [j for j in jobs if j in alljobs]
            jobs = [j for j in jobs if j not in complete]
            if len(jobs) == 0:
                return
            sleep(2)
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
