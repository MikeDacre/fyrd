# -*- coding: utf-8 -*-
"""
Monitor the queue for torque or slurm.

Provides a class to monitor the all defined batch queues with identical
syntax.

At its simplest, you can use it like::

    q = queue.Queue()
    q.jobs
    q.running
    q.pending
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
import pwd as _pwd          # Used to get usernames for queue
import getpass as _getpass  # Used to get usernames for queue
from datetime import datetime as _dt
from time import time as _time
from time import sleep as _sleep

from six import text_type as _txt
from six import string_types as _str
from six import integer_types as _int

###############################################################################
#                                Our functions                                #
###############################################################################

from . import run as _run
from . import logme as _logme
from . import conf as _conf
from . import ClusterError as _ClusterError
from . import batch_systems as _batch
from . import notify as _notify

# Funtions to import if requested
__all__ = ['Queue']

GOOD_STATES      = _batch.GOOD_STATES
ACTIVE_STATES    = _batch.ACTIVE_STATES
BAD_STATES       = _batch.BAD_STATES
UNCERTAIN_STATES = _batch.UNCERTAIN_STATES
ALL_STATES       = _batch.ALL_STATES
DONE_STATES      = _batch.DONE_STATES

###############################################################################
#                               The Queue Class                               #
###############################################################################


class Queue(object):

    """A wrapper for all defined batch systems.

    Attributes
    ----------
    jobs : dict
        A dictionary of all jobs in this queue in the form:
        `{jobid: Queue.QueueJob}`
    finished : dict
        A dictionary of all completed jobs, same format as jobs
    bad : dict
        A dictionary of all jobs with failed or unknown states, same format as
        jobs
    active_job_count : int
        Total jobs in the queue (including array job children)
    max_jobs : int
        The maximum number of jobs allowed in the queue
    can_submit : bool
        True if active_job_count < max_jobs, False otherwise
    job_states : list
        A list of the different states of jobs in this queue
    active_job_count : int
        A count of all jobs that are either pending or running in the current
        queue
    can_submit : bool
        True if total active jobs is less than max_jobs
    users : set
        A set of all users with active jobs
    job_states : set
        A set of all current job states

    Methods
    -------
    wait(jobs, return_disp=False)
        Block until all jobs in jobs are complete.
    get(jobs)
        Get all results from a bunch of Job objects.
    wait_to_submit(max_jobs=None)
        Block until fewer running/pending jobs in queue than max_jobs.
    update()
        Refresh the list of jobs from the server.
    get_jobs(key)
        Return a dict of jobs where state matches key.
    get_user_jobs(users)
        Return a dict of jobs for all all jobs by each user in users.
    """

    def __init__(self, user=None, partition=None, qtype=None):
        """Can filter by user, queue type or partition on initialization.

        Parameters
        ----------
        user : str
            Optional usernameto filter the queue with.  If user='self' or
            'current', the current user will be used.
        partition : str
            Optional partition to filter the queue with.
        qtype : str
            one of the defined batch queues (e.g. 'slurm')
        """
        # Get user ID as an int UID
        if user:
            if user == 'self' or user == 'current':
                self.user = _getpass.getuser()
                """The username if defined."""
                self.uid  = _pwd.getpwnam(self.user).pw_uid
            elif user == 'ALL':
                self.user = None
            else:
                if isinstance(user, int) \
                        or (isinstance(user, (_str, _txt)) and user.isdigit()):
                    self.uid  = int(user)
                else:
                    self.uid = _pwd.getpwnam(str(user)).pw_uid
        else:
            self.uid = None
        self.user = _pwd.getpwuid(self.uid).pw_name if self.uid else None
        self.partition = partition
        self.max_jobs = int(_conf.get_option('queue', 'max_jobs'))
        # Don't allow max jobs to be less than 5, otherwise basic split jobs
        # permanently hang
        if self.max_jobs < 4:
            self.max_jobs = 4

        # Support python2, which hates reciprocal import
        from .job import Job
        self._Job      = Job

        # Get sleep time and update time
        self.queue_update_time = float(
            _conf.get_option('queue', 'queue_update', 2)
        )
        self.sleep_len = float(_conf.get_option('queue', 'sleep_len', 0.5))

        # Set type
        if qtype:
            _batch.check_queue(qtype)
        else:
            _batch.check_queue()
        self.qtype = qtype if qtype else _batch.MODE
        self.batch_system = _batch.get_batch_system(self.qtype)

        # Allow tracking of updates to prevent too many updates
        self._updating = False

        # Will contain a dict of QueueJob objects indexed by ID
        self.jobs = {}
        """All jobs currently in this queue."""

        self._update()

    ####################
    #  Public Methods  #
    ####################

    def check_dependencies(self, dependencies):
        """Check if dependencies are running.

        Parameters
        ----------
        dependencies : list
            List of job IDs

        Returns
        -------
        str
            'active' if dependencies are running or queued, 'good' if
            completed, 'bad' if failed, cancelled, or suspended, 'absent'
            otherwise.
        """
        for dep in _run.listify(dependencies):
            dep = str(dep)
            if dep not in self.jobs:
                return 'absent'
            state = self.jobs[dep].state
            if state in ACTIVE_STATES:
                return 'active'
            elif state in GOOD_STATES:
                continue
            elif state in BAD_STATES or state in UNCERTAIN_STATES:
                return 'bad'
            else:
                raise _ClusterError('Invalid state {0}'.format(state))
        return 'good'

    def wait(self, jobs, return_disp=False, notify=False):
        """Block until all jobs in jobs are complete.

        Update time is dependant upon the queue_update parameter in your
        ~/.fyrd/config.txt file.

        Parameters
        ----------
        jobs : list
            List of either fyrd.job.Job, fyrd.queue.QueueJob, job_id
        return_disp : bool, optional
            If a job disappeares from the queue, return 'disapeared' instead of
            True
        notify : str or False, optional
            Email address to send notification to, defaults to address in
            config
            False means no notification

        Returns
        -------
        bool or str
            True on success False or None on failure unless return_disp is True
            and the job disappeares, then returns 'disappeared'
        """
        if notify is None:
            notify = _conf.get_option('notify', 'notify_address')
        self.update()
        _logme.log('Queue waiting.', 'debug')

        # Sanitize arguments
        jobs = _run.listify(jobs)
        for job in jobs:
            if not isinstance(job, (_str, _txt, _int, QueueJob, self._Job)):
                raise _ClusterError('job must be int, string, or Job, ' +
                                    'is {}'.format(type(job)))

        check_jobs = []
        for job in jobs:
            if isinstance(job, (self._Job, QueueJob)):
                job_id = job.id
            else:
                job_id = str(job)
            check_jobs.append(job_id)

        pbar = _run.get_pbar(jobs, name="Waiting for job completion",
                             unit='jobs')
        while check_jobs:
            self.update()
            for job in check_jobs:
                if isinstance(job, (self._Job, QueueJob)):
                    job_id = job.id
                else:
                    job_id = str(job)
                job_id, array_id = self.batch_system.normalize_job_id(job_id)
                _logme.log('Checking {}'.format(job_id), 'debug')
                lgd = False
                # Allow 12 seconds to elapse before job is found in queue,
                # if it is not in the queue by then, assume failure.
                if not self.test_job_in_queue(job_id):
                    if return_disp:
                        return 'disappeared'
                    else:
                        return False
                ## Actually look for job in running/queued queues
                lgd      = False
                lgd2     = False
                start    = _dt.now()
                res_time = float(_conf.get_option('queue', 'res_time'))
                count    = 0
                # Get job state
                job_state = self.jobs[job_id].state
                # Check the state
                if job_state in GOOD_STATES:
                    _logme.log('Queue wait for {} complete'
                               .format(job_id), 'debug')
                    check_jobs.pop(check_jobs.index(job))
                    pbar.update()
                    break
                elif job_state in ACTIVE_STATES:
                    if lgd:
                        _logme.log('{} not complete yet, waiting'
                                   .format(job_id), 'debug')
                        lgd = True
                    else:
                        _logme.log('{} still not complete, waiting'
                                   .format(job_id), 'verbose')
                elif job_state in BAD_STATES:
                    _logme.log('Job {} failed with state {}'
                               .format(job, job_state), 'error')
                    return False
                elif job_state in UNCERTAIN_STATES:
                    if not lgd2:
                        _logme.log('Job {} in state {}, waiting {} '
                                   .format(job, job_state, res_time) +
                                   'seconds for resolution', 'warn')
                        lgd2 = True
                    if (_dt.now() - start).seconds > res_time:
                        _logme.log('Job {} still in state {}, aborting'
                                   .format(job, job_state), 'error')
                        return False
                else:
                    if count == 5:
                        _logme.log('Job {} in unknown state {} '
                                   .format(job, job_state) +
                                   'cannot continue', 'critical')
                        raise QueueError('Unknown job state {}'
                                         .format(job_state))
                    _logme.log('Job {} in unknown state {} '
                               .format(job, job_state) +
                               'trying to resolve', 'debug')
                    count += 1
            _sleep(self.sleep_len)
        # Update jobs
        for job in jobs:
            if isinstance(job, self._Job):
                job.update()
        if notify:
            _notify.notify('{} Jobs complete'.format(len(jobs)), notify)
        return True

    def get(self, jobs):
        """Get all results from a bunch of Job objects.

        Parameters
        ----------
        jobs : list
            List of fyrd.Job objects

        Returns
        -------
        job_results : dict
            `{job_id: Job}`

        Raises
        ------
        fyrd.ClusterError
            If any job fails or goes missing.
        """
        self.update()
        _logme.log('Queue waiting.', 'debug')
        singular = True if isinstance(jobs, self._Job) else False

        # Force into enumerated list to preserve order
        jobs = dict(enumerate(_run.listify(jobs)))
        done = {}

        # Check that all jobs are valid
        for job in jobs.values():
            if not isinstance(job, self._Job):
                raise _ClusterError('This only works with cluster job '
                                    'objects')

        # Loop through all jobs continuously trying to get outputs
        pbar = _run.get_pbar(jobs, name="Getting Job Results", unit='jobs')
        while jobs:
            for i in list(jobs):
                job = jobs[i]
                job.update()
                if not self.test_job_in_queue(job.id):
                    raise _ClusterError('Job {} not queued'.format(job.id))
                if job.state == 'completed':
                    done[i] = jobs.pop(i).get()
                    pbar.update()
                elif job.state in BAD_STATES:
                    pbar.close()
                    raise _ClusterError('Job {} failed, cannot get output'
                                        .format(job.id))
            # Block between attempts
            _sleep(self.sleep_len)
        pbar.write('Done\n')
        pbar.close()

        # Correct the order, make it the same as the input list
        results = []
        for i in sorted(done.keys()):
            results.append(done[i])
        return results[0] if singular else results

    def test_job_in_queue(self, job_id, array_id=None):
        """Check to make sure job is in self.

        Tries 12 times with 1 second between each. If found returns True,
        else False.

        Parameters
        ----------
        job_id : str
        array_id : str, optional

        Returns
        -------
        exists : bool
        """
        lgd = False
        not_found = 0
        job_id = str(job_id)
        if array_id is not None:
            array_id = str(array_id)
        if isinstance(job_id, _QueueJob):
            job_id = job_id.id
        while True:
            self._update()
            # Allow 12 seconds to elapse before job is found in queue,
            # if it is not in the queue by then, assume completion.
            if job_id in self.jobs:
                job = self.jobs[job_id]
                if array_id and array_id not in job.children:
                    return False
                return True
            else:
                if lgd:
                    _logme.log('Attempt #{}/12'.format(not_found),
                               'debug')
                else:
                    _logme.log('{} not in queue, waiting up to 12s '
                               .format(job_id) +
                               'for it to appear', 'info')
                    lgd = True
                _sleep(self.sleep_len)
                not_found += 1
                if not_found == 12:
                    _logme.log(
                        '{} not in queue, tried 12 times over 12s'
                        .format(job_id) + '. Job likely completed, ' +
                        'assuming completion, stats will be ' +
                        'unavailable.','warn'
                    )
                    return False
                continue

    def wait_to_submit(self, max_jobs=None):
        """Block until fewer running/queued jobs in queue than max_jobs.

        Parameters
        ----------
        max_jobs : int
            Override self.max_jobs for wait
        """
        count   = 50
        written = False
        while True:
            if self._can_submit(max_jobs):
                return
            if not written:
                _logme.log(('The queue is full, there are {} jobs running and '
                            '{} jobs queued. Will wait to submit, retrying '
                            'every {} seconds.')
                           .format(len(self.running), len(self.queued),
                                   self.sleep_len),
                           'info')
                written = True
            if count == 0:
                _logme.log('Still waiting to submit.', 'info')
                count = 50
            count -= 1
            _sleep(self.sleep_len)

    def update(self):
        """Refresh the list of jobs from the server, limit queries."""
        if int(_time()) - self.last_update > self.queue_update_time:
            self._update()
        else:
            _logme.log('Skipping update as last update too recent', 'debug')
        return self

    def get_jobs(self, key):
        """Return a dict of jobs where state matches key."""
        retjobs = {}
        keys = [k.lower() for k in _run.listify(key)]
        for jobid, job in self.jobs.items():
            if job.get_state() in keys:
                retjobs[jobid] = job
        return retjobs

    def get_user_jobs(self, users):
        """Filter jobs by user.

        Parameters
        ----------
        users : list
            A list of users/owners

        Returns
        -------
        dict
            A filtered job dictionary of `{job_id: QueueJob}` for all jobs
            owned by the queried users.
        """
        users = _run.listify(users)
        return {k: v for k, v in self.jobs.items() if v.owner in users}

    @property
    def users(self):
        """Return a set of users with jobs running."""
        return set([job.owner for job in self.jobs.values()])

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
        """Return a count of all queued or running jobs, inc. array jobs."""
        self.update()
        jobcount = 0
        for j in self.get_jobs(ACTIVE_STATES):
            jobcount += j.jobcount()
        return int(jobcount)

    @property
    def can_submit(self):
        """Return True if R/Q jobs are less than max_jobs."""
        return self._can_submit()

    def _can_submit(self, max_jobs=None):
        """"Return True if R/Q jobs are less than max_jobs."""
        self.update()
        # Get max jobs
        max_jobs = int(max_jobs) if max_jobs else self.max_jobs
        if max_jobs < 4:
            max_jobs = 4
        # Fix self.max_jobs also
        if self.max_jobs < 4:
            self.max_jobs = 4
        # Update running and queued jobs can't use self.active_job_count as
        # rapid update breaks the property count and caused jobcount to be None
        jobcount = 0
        for j in self.get_jobs(ACTIVE_STATES).values():
            jobcount += j.jobcount()
        return jobcount < max_jobs

    ######################
    # Internal Functions #
    ######################

    def _update(self):
        """Refresh the list of jobs from the server.

        This is the core queue interaction function of this class.
        """
        if self._updating:
            return
        _logme.log('Queue updating', 'debug')
        # Set the update time I don't care about microseconds
        self.last_update = int(_time())

        jobs = []  # list of jobs created this session
        for [job_id, array_id, job_name, job_user, job_partition,
             job_state, job_nodelist, job_nodecount,
             job_cpus, job_exitcode] in self.batch_system.queue_parser(
                 self.user, self.partition):
            job_id = str(job_id)
            job_state = job_state.lower()
            if job_nodecount and job_cpus:
                job_threads = int(job_nodecount) * int(job_cpus)
            else:
                job_threads = None
            if job_state == 'completed' or job_state == 'failed':
                job_exitcode = job_exitcode
            else:
                job_exitcode = None

            # Get/Create the QueueJob object
            if job_id not in self.jobs:
                job = QueueJob()
            else:
                job = self.jobs[job_id]

            job.id    = job_id
            job.name  = job_name
            job.owner = job_user
            job.queue = job_partition
            job.state = job_state.lower()

            if array_id is not None:
                job.array_job = True
                cjob = QueueChild(job)
                cjob.id      = array_id
                cjob.name    = job_name
                cjob.owner   = job_user
                cjob.queue   = job_partition
                cjob.state   = job_state.lower()
                cjob.nodes   = job_nodelist
                cjob.threads = job_threads
                cjob.exitcode = job_exitcode
                job.children[array_id] = cjob
                job.state    = job.get_state()
                job.nodes    = job.get_nodelist()
                job.threads  = job.get_threads()
                job.exitcode  = job.get_exitcode()
            else:
                job.nodes   = job_nodelist
                job.threads = job_threads
                job.exitcode = job_exitcode

            # Assign the job to self.
            self.jobs[job_id] = job
            jobs.append(str(job_id))

        # We assume that if a job just disappeared it completed
        if self.jobs:
            for qjob in self.jobs.values():
                if str(qjob.id) not in jobs:
                    qjob.state = 'completed'
                    qjob.disappeared = True

    def __getattr__(self, key):
        """Make running and queued attributes dynamic."""
        key = self.batch_system.normalize_state(key.lower())
        if key == 'complete':
            key = 'completed'
        elif key == 'queued':
            key = 'pending'
        if key in ALL_STATES:
            return self.get_jobs(key)

    def __getitem__(self, key):
        """Allow direct accessing of jobs by job id."""
        if isinstance(key, self._Job):
            key = key.id
        key = str(key)
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
            outstr = 'Queue<jobs:{};completed:{};pending:{};user={}>'.format(
                len(self), len(self.complete), len(self.queued), self.user)
        else:
            outstr = 'Queue<jobs:{};completed:{};pending:{};user=ALL>'.format(
                len(self), len(self.complete), len(self.queued))
        self._updating = False
        return outstr

    def __str__(self):
        """A list of keys."""
        return str(list(self.jobs.keys()))


##############################################
#  A simple class to hold jobs in the queue  #
##############################################


class _QueueJob(object):

    """A very simple class to store info about jobs in the queue.

    Attributes
    ----------
    id : int
        Job ID
    name : str
        Job name
    children : dict
        If array job, list of child job numbers
    owner : str
        User who owns the job
    threads : int
        Number of cores used by the job
    queue : str
        The queue/partition the job is running in
    state : str
        Current state of the job, normalized to slurm states
    nodes : list
        List of nodes job is running on
    exitcode : int
        Exit code of completed job
    disappeared : bool
        Job cannot be found in the queue anymore
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
    array_job   = False
    children    = {}
    parent      = None
    _child_job  = False
    _cname      = None

    def get_state(self):
        """return the current state of the job."""
        if self.array_job:
            pending_jobs = False
            running_jobs = False
            failed_jobs  = False
            for job_info in self.children.values():
                if job_info.state == 'pending':
                    pending_jobs = True
                elif job_info.state in ACTIVE_STATES:
                    running_jobs = True
                elif job_info.state in BAD_STATES:
                    failed_jobs = True
            if running_jobs:
                return 'running'
            if pending_jobs:
                return 'pending'
            if failed_jobs:
                return 'failed'
            return 'completed'
        return self.state

    def get_nodelist(self):
        """return the current state of the job."""
        if self.array_job:
            nodelist = []
            for job_info in self.children.values():
                if job_info.nodes:
                    nodelist = nodelist + job_info.nodes
            return nodelist if nodelist else None
        return self.nodes

    def get_threads(self, state=None):
        """Return a count of how many running jobs we have."""
        states = [i.lower() for i in _run.listify(state)]
        if self.array_job:
            if state:
                return sum([j.threads for j in self.children.values()
                            if j.state in states])
            return len(self.children)
        if state:
            return self.threads if self.state in states else 0
        return self.threads

    def get_exitcode(self):
        """Return sum of exitcodes for all completed jobs."""
        if self.array_job:
            code = 0
            some_done = False
            for child in self.children.values():
                if child.state in DONE_STATES:
                    some_done = True
                    if child.exitcode:
                        code += child.exitcode
            if some_done:
                return code
            return None
        return self.exitcode

    def jobcount(self, state=None):
        """Return a count of how many running jobs we have."""
        states = [i.lower() for i in _run.listify(state)]
        if self.array_job:
            if state:
                return len([j for j in self.children.values()
                            if j.state in states])
            return len(self.children)
        if state:
            return 1 if self.state in states else 0
        return 1

    def __repr__(self):
        """Show all info."""
        if not self._child_job:
            if self.array_job:
                children = len(self.children) if self.children else None
                child_str = ':children:{0}'.format(children)
            else:
                child_str = ''
        else:
            child_str = ':parent:{0}'.format(self.parent.id)
        outstr = ("{cname}<{id}:{state}{child}" +
                  "({name},owner:{owner}," +
                  "queue:{queue},nodes:{nodes},threads:{threads}," +
                  "exitcode:{code})").format(
                      id=self.id, name=self.name, owner=self.owner,
                      queue=self.queue, nodes=self.nodes,
                      code=self.exitcode, threads=self.threads,
                      state=self.state, child=child_str, cname=self._cname
                  )
        if self.disappeared:
            outstr += 'DISAPPEARED>'
        else:
            outstr += '>'
        return outstr

    def __str__(self):
        """Print job ID."""
        return str(self.id)


class QueueJob(_QueueJob):

    """A very simple class to store info about jobs in the queue.

    Only used for torque and slurm queues.

    Attributes
    ----------
    id : int
        Job ID
    name : str
        Job name
    owner : str
        User who owns the job
    threads : int
        Number of cores used by the job
    queue : str
        The queue/partition the job is running in
    state : str
        Current state of the job, normalized to slurm states
    nodes : list
        List of nodes job is running on
    exitcode : int
        Exit code of completed job
    disappeared : bool
        Job cannot be found in the queue anymore
    array_job : bool
        This job is an array job and has children
    children : dict
        If array job, list of child job numbers
    """

    def __init__(self):
        """Initialize."""
        self._cname   = 'QueueJob'
        self.children = {}

    def __getitem__(self, key):
        """Allow direct accessing of child jobs by job id."""
        key = str(key)
        if not self.array_job:
            _logme.log('Not an array job', 'error')
            return
        return self.children[key]


class QueueChild(_QueueJob):

    """A very simple class to store info about child jobs in the queue.

    Only used for torque and slurm queues.

    Attributes
    ----------
    id : int
        Job ID
    name : str
        Job name
    owner : str
        User who owns the job
    threads : int
        Number of cores used by the job
    queue : str
        The queue/partition the job is running in
    state : str
        Current state of the job, normalized to slurm states
    nodes : list
        List of nodes job is running on
    exitcode : int
        Exit code of completed job
    disappeared : bool
        Job cannot be found in the queue anymore
    parent : QueueJob
        Backref to parent job
    """

    def __init__(self, parent):
        """Initialize with a parent."""
        self._cname   = 'QueueChild'
        self.parent = parent


################
#  Exceptions  #
################

class QueueError(Exception):

    """Simple Exception wrapper."""

    pass
