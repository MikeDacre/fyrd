#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A simple torque/slurm style batch submission system.

Uses multiprocessing, SQLAlchemy with sqlite3, Pyro4, and daemonocle to create
a batch system with dependency tracking.

The batch system is a daemon process that is launched if not already running.

Everything required for this queue is defined here. In addition, all fyrd
parsing functions are also defined here for full integration with fyrd.

Because local jobs are very simple `parse_strange_options()` strips all
keyword arguments, the only arguments we pay attention to are:
    outfile : STDERR goes here
    errfile : STDERR goes here
    cores : Sets the number of threads per process
    depends : Job dependencies

All others are ignored (although note that many others are actually handled by
the Job object anyway).
"""
from __future__ import print_function
import os as _os
import sys
import errno as _errno
#  import atexit as _atexit
import signal as _signal
import socket as _socket    # Used to get hostname
import getpass as _getpass  # Used to get usernames for queue
import argparse as _argparse
import subprocess
import multiprocessing as mp
from time import sleep as _sleep
from datetime import datetime as _dt
from datetime import timedelta as _td
from collections import OrderedDict as _OD

try:
    from Queue import Empty
except ImportError:  # python3
    from queue import Empty

from six import text_type as _txt
from six import string_types as _str
from six import integer_types as _int

import psutil as _psutil

import Pyro4

from sqlalchemy.exc import InvalidRequestError

from sqlalchemy import create_engine as _create_engine
from sqlalchemy import Column as _Column
from sqlalchemy import String as _String
from sqlalchemy import Integer as _Integer

from sqlalchemy.types import DateTime as _DateTime
from sqlalchemy.orm import sessionmaker as _sessionmaker
from sqlalchemy.orm import scoped_session as _scoped_session
from sqlalchemy.ext.declarative import declarative_base as _base

# Fyrd imports (not necessary for main functionality) no relative imports
# as we run as a script also
from fyrd import run as _run
from fyrd import conf as _conf
from fyrd import logme as _logme
from fyrd import options as _options
from fyrd import script_runners as _scrpts
from fyrd import submission_scripts as _sscrpt
_Script = _sscrpt.Script

# For SQLAlchemy DB
Base = _base()

#############################
#  User Editable Constants  #
#############################

PID = None
RUN_DIR  = _conf.CONFIG_PATH
PID_FILE = _os.path.join(RUN_DIR, 'local_queue.pid')
URI_FILE = _os.path.join(RUN_DIR, 'local_queue.uri')
DATABASE = _os.path.join(RUN_DIR, 'local_queue.db')

# Time to block between looping in job_runner
SLEEP_LEN = 0.1

# Time in seconds to wait for QueueManager to terminate
STOP_WAIT = 5

# Number of days to wait before cleaning out old jobs, obtained from the
# config file, this just sets the default
CLEAN_OLDER_THAN = 7

# Default max jobs, can be overriden by config file or QueueManager init
MAX_JOBS = mp.cpu_count()-1
MAX_JOBS = MAX_JOBS if MAX_JOBS >= 0 else 1

############################
#  Do Not Edit Below Here  #
############################
_WE_ARE_A_SERVER = False

# Fyrd file prefix, we don't use file commands so this is empty
PREFIX = ''
# This will be appended to job submission scripts
SUFFIX = 'job'

# Reset broken multithreading
# Some of the numpy C libraries can break multithreading, this command
# fixes the issue.
try:
    subprocess.check_output(
        "taskset -p 0xff %d >/dev/null 2>/dev/null" % _os.getpid(), shell=True
    )
except subprocess.CalledProcessError:
    pass  # This doesn't work on Macs or Windows


###############################################################################
#                                  Exception                                  #
###############################################################################


class QueueError(Exception):

    """Generic Exception for handling errors."""

    pass


###############################################################################
#                                  Database                                   #
###############################################################################


class Job(Base):

    """A Job record for every queued job.

    Attributes
    ----------
    jobno : int
        Job ID
    name : str
        The name of the job
    command : str
        A full shell script that can be executed
    state : {'queued', 'running', 'completed', 'failed'}
        The current state of the job
    threads : int
        The requested number of cores
    exitcode : int
        The exit code of the job if state in {'completed', 'failed'}
    pid : int
        The pid of the process
    runpath : str, optional
        Path to the directory to run in
    outfile, errfile : str, optional
        Paths to the output files
    """

    __tablename__ = 'jobs'

    jobno       = _Column(_Integer, primary_key=True, index=True)
    name        = _Column(_String, nullable=False)
    command     = _Column(_String, nullable=False)
    submit_time = _Column(_DateTime, nullable=False)
    threads     = _Column(_Integer, nullable=False)
    state       = _Column(_String, nullable=False, index=True)
    exitcode    = _Column(_Integer)
    pid         = _Column(_Integer)
    runpath     = _Column(_String)
    outfile     = _Column(_String)
    errfile     = _Column(_String)

    def __repr__(self):
        """Display summary."""
        return 'LocalQueueJob<{0}:{1};{2};state:{3};exitcode:{4}>'.format(
            self.jobno, self.pid, self.name, self.state, self.exitcode
        )


class LocalQueue(object):

    """A database to hold job information.

    Should only be accessed by a running QueueManager daemon.
    """

    db_file = DATABASE

    def __init__(self, db_file=None):
        """Attach to a database, create if does not exist."""
        db_file = db_file if db_file else self.db_file
        self.db_file = _os.path.abspath(db_file)
        self.engine  = _create_engine(
            'sqlite:///{}?check_same_thread=False'.format(self.db_file)
        )
        if not _os.path.isfile(self.db_file):
            self.create_database(confirm=False)

    ##########################################################################
    #                           Basic Connectivity                           #
    ##########################################################################

    def get_session(self):
        """Return session for this database."""
        session_factory = _sessionmaker(bind=self.engine)
        Session = _scoped_session(session_factory)
        return Session()

    @property
    def session(self):
        """Simple wrapper for a new session."""
        return self.get_session()


    ##########################################################################
    #                                Querying                                #
    ##########################################################################

    def query(self, *args):
        """Wrapper for the SQLAlchemy query method of session.

        Parameters
        ----------
        args
            Any arguments allowed by session.query. If blank, Job is used,
            which will return the whole database. To limit by columns, simply
            pass columns: `query(Job.jobno)` would return only a list of job
            numbers.
        """
        if not args:
            args = (Job,)
        session = self.get_session()
        return session.query(*args)

    ##########################################################################
    #                           Job State Searches                           #
    ##########################################################################

    def get_jobs(self, state=None):
        """Return list of Jobs for all jobs that match state."""
        q = self.query()
        if state:
            q = q.filter(Job.state == state)
        return q.all()

    @property
    def running(self):
        """All running jobs."""
        return self.get_jobs(state='running')

    @property
    def queued(self):
        """All queued jobs."""
        return self.get_jobs(state='pending')

    @property
    def completed(self):
        """All completed jobs."""
        return self.get_jobs(state='completed')

    @property
    def failed(self):
        """All failed jobs."""
        return self.get_jobs(state='failed')

    ##########################################################################
    #                             Job Management                             #
    ##########################################################################

    def set_running_jobs_failed(self):
        """Change all running jobs to failed."""
        pass

    ##########################################################################
    #                          Maintenance Methods                           #
    ##########################################################################

    def create_database(self, confirm=True):
        """Create the db from scratch.

        Note: Will DELETE the current database!!!
        """
        if confirm:
            ans = _run.get_yesno(
                'Are you sure you want to erase and recreate the db?'
            )
            if not ans:
                sys.stderr.write('Aborting\n')
                return False
        _logme.log('Recreating database', 'info', also_write='stderr')
        if _os.path.exists(self.db_file):
            _os.remove(self.db_file)
        Base.metadata.create_all(self.engine)
        _logme.log('Done', 'info', also_write='stderr')

    ##########################################################################
    #                               Internals                                #
    ##########################################################################

    def __getitem__(self, x):
        """Quick access to jobs by ID."""
        if isinstance(x, (_str, _txt)):
            return self.query().filter(Job.jobno == x).all()

    def __len__(self):
        """Print the length."""
        return self.query(Job).count()

    def __repr__(self):
        """Basic information about self."""
        return 'LocalQueue<{location}>'.format(location=self.db_file)

    def __str__(self):
        """Basic information about self."""
        return self.__repr__()


###############################################################################
#                            Management Functions                             #
###############################################################################


def initialize():
    """Initialize the database and config directory.

    Returns
    -------
    success : bool
    """
    if not _os.path.isdir(RUN_DIR):
        _os.makedirs(RUN_DIR)
    if not _os.path.isfile(DATABASE):
        db = LocalQueue(DATABASE)
        db.create_database(confirm=False)
    return True


def check_conf():
    """Make sure the config directory exists, initialize if not.

    Returns
    -------
    success : bool
    """
    if _os.path.isdir(RUN_DIR) and _os.path.isfile(DATABASE):
        return True
    return initialize()


def server_running():
    """Return True if server currently running."""
    if not _os.path.isfile(PID_FILE):
        return False
    with open(PID_FILE) as fin:
        pid = int(fin.read().strip())
    return _pid_exists(pid)


def start_server():
    """Start the server as a separate thread.

    Returns
    -------
    pid : int
    """
    _logme.log('Starting local queue server with 2 second sleep', 'info')
    _sleep(2)
    #  subprocess.check_call([sys.executable, us, 'start'])
    if not server_running():
        daemon_manager('start')
    _sleep(1)
    if not server_running():
        _logme.log('Cannot start server', 'critical')
        raise QueueError('Cannot start server')
    with open(PID_FILE) as fin:
        pid = int(fin.read().strip())
    return pid


def get_server_uri(start=True):
    """Check status and return a server URI."""
    check_conf()
    if (_WE_ARE_A_SERVER or not start) and not _os.path.isfile(PID_FILE):
        return False
    if not _os.path.isfile(PID_FILE):
        if _os.path.isfile(URI_FILE):
            _os.remove(URI_FILE)
        if not start_server():
            return False
    with open(PID_FILE) as fin:
        pid = int(fin.read().strip())
    if not _pid_exists(pid):
        _os.remove(PID_FILE)
        if _os.path.isfile(URI_FILE):
            _os.remove(URI_FILE)
        if _WE_ARE_A_SERVER or not start:
            return False
        pid = start_server()
        if not pid or not _pid_exists(pid):
            raise OSError('Server not starting')
    # Server running now
    with open(URI_FILE) as fin:
        return fin.read().strip()


RESTART_TRY = False


def get_server(start=True, raise_on_error=False):
    """Return a client-side QueueManager instance."""
    uri = get_server_uri(start=start)
    if not uri:
        if raise_on_error:
            raise QueueError('Cannot get server')
        return None
    server =  Pyro4.Proxy(uri)
    # Test for bad connection
    try:
        server._pyroBind()
    except Pyro4.errors.CommunicationError:
        global RESTART_TRY
        if RESTART_TRY:
            _logme.log(
                "Cannot bind to server still. Failing. Try to kill the "
                "process in {}".format(PID_FILE),
                'critical'
            )
            if raise_on_error:
                raise QueueError('Cannot get server')
            return None
        RESTART_TRY = True
        _logme.log("Cannot bind to server, killing and retrying.", 'error')
        kill_queue()
        server = get_server(start, raise_on_error)
        RESTART_TRY = False
    return server


def _pid_exists(pid):
    """Check whether pid exists in the current process table.
    UNIX only.

    From: https://stackoverflow.com/questions/568271/#6940314
    """
    pid = int(pid)
    if pid < 0:
        return False
    if pid == 0:
        # According to "man 2 kill" PID 0 refers to every process
        # in the process group of the calling process.
        # On certain systems 0 is a valid PID but we have no way
        # to know that in a portable fashion.
        raise ValueError('invalid PID 0')
    try:
        _os.kill(pid, 0)
    except OSError as err:
        if err.errno == _errno.ESRCH:
            # ESRCH == No such process
            return False
        elif err.errno == _errno.EPERM:
            # EPERM clearly means there's a process to deny access to
            return True
        else:
            # According to "man 2 kill" possible error values are
            # (EINVAL, EPERM, ESRCH)
            raise
    else:
        return True


###############################################################################
#                        Core Queue Manager and Runner                        #
###############################################################################


def get_queue_manager():
    """Return a client of QueueManager."""
    return get_server()


@Pyro4.expose
class QueueManager(object):

    """Monitor a queue and run jobs.

    Actual job execution done by the job_runner process, which forks.

    .. note:: This class should not be accessed directly, it is intended to
        run as a daemon. Use `get_queue_manager()` to get the client class.

    Attributes
    ----------
    jobs : dict
        Dictionary of running jobs
    max_jobs : int
        The maximum number of jobs to run at one time. Defaults to current
        CPUs - 1
    """

    jobs = {}
    all_jobs = []
    max_jobs = None
    inqueue  = mp.Queue()
    outqueue = mp.Queue()

    _job_runner = None

    def __init__(self, daemon, max_jobs=None):
        """Create the QueueManager

        Paramenters
        -----------
        max_jobs : int, optional
            The maximum number of jobs to run at one time. Defaults to current
            CPUs - 1
        """
        if not max_jobs:
            cmax = _conf.get_option('local', 'max_jobs')
            max_jobs = cmax if cmax else MAX_JOBS
        self.max_jobs = int(max_jobs)
        # Don't use more than the available cores
        if self.max_jobs > mp.cpu_count():
            self.max_jobs = mp.cpu_count()
        # Unless there are fewer than 4 cores, we need at least 4 for split
        # jobs, so we hard code 4 as the minimum
        if self.max_jobs < 4:
            self.max_jobs = 4
        self.db = LocalQueue(DATABASE)
        self.daemon = daemon
        self.all_jobs = [i[0] for i in self.db.query(Job.jobno).all()]
        self.check_runner()
        # Set all existing to disappeared, as they must be dead if we are
        # starting again
        session = self.db.get_session()
        q = session.query(Job).filter(
            Job.state.in_(['running', 'pending', 'queued'])
        )
        bad = []
        for job in q.all():
            bad.append(str(job.jobno))
            job.state = 'killed'
        session.commit()
        session.close()
        if bad:
            _logme.log('Jobs {0} were marked as killed as we are restarting'
                       .format(','.join(bad)), 'warn')


    ##########################################################################
    #                     Job Submission and Management                      #
    ##########################################################################

    @Pyro4.expose
    def submit(self, command, name, threads=1, dependencies=None,
               stdout=None, stderr=None, runpath=None):
        """Submit a job and add it to the database.

        Parameters
        ----------
        command : str
            A full executable shell script/shell command.
        name : str
            A name to give the job
        threads : int, optional
            The number of cores required by the job
        dependencies : list of int, optional
            A list of job numbers that must be complete prior to execution
        stdout, stderr : str, optional
            A path to a file to write STDOUT and STDERR to respectively
        runpath : str, optional
            A path to execute the command in

        Returns
        -------
        jobno : int

        Raises
        ------
        QueueError
            If a dependency is not in the job db already or command is invalid
        """
        session = self.db.get_session()
        threads = int(threads)
        if not isinstance(command, (_str, _txt)):
            raise ValueError('command is {0}, type{1}, cannot continue'
                             .format(command, type(command)))
        depends = []
        if dependencies:
            for dep in dependencies:
                dep = int(dep)
                if not self.check_jobno(dep):
                    raise QueueError('Invalid dependencies')
                depends.append(dep)
        job = Job(name=name, command=command, threads=threads, state='pending',
                  submit_time=_dt.now())
        if stdout:
            job.outfile = stdout
        if stderr:
            job.errfile = stderr
        if runpath:
            job.runpath = runpath
        try:
            session.add(job)
        except InvalidRequestError:
            # In case open in another thread
            local_job = session.merge(job)
            other_session = session.object_session(job)
            session.add(local_job)
            session.commit()
            other_session.close()
        session.flush()
        jobno = int(job.jobno)
        session.commit()
        session.close()
        self.check_runner()
        self.inqueue.put(
            ('queue',
             (jobno, command, threads, depends, stdout, stderr, runpath))
        )
        self.jobs[jobno] = job
        self.all_jobs.append(jobno)
        return jobno

    @Pyro4.expose
    def get(self, jobs=None, preclean=True):
        """Return a list of updated jobs.

        Parameters
        ----------
        jobs : list of int, optional
            A list of job numbers, a single job number is also fine
        preclean : bool
            If True run `clean()` first to remove old jobs

        Returns
        -------
        jobs : list of tuple
            [(jobno, name, command, state, threads,
              exitcode, runpath, outfile, errfile)]
        """
        if preclean:
            self.clean()
        session = self.db.get_session()
        # Pyro cannot serialize Job objects, so we get a tuple instead
        q = session.query(
            Job.jobno, Job.name, Job.command, Job.state, Job.threads,
            Job.exitcode, Job.runpath, Job.outfile, Job.errfile
        )
        if jobs:
            jobs = [jobs] if isinstance(jobs, (_int, _str, _txt)) else jobs
            jobs = [int(j) for j in jobs]
            q = q.filter(Job.jobno.in_(jobs))
        res = q.all()
        session.close()
        return res

    @Pyro4.expose
    def clean(self, days=None):
        """Delete all jobs in the queue older than days days.

        Very fast if no jobs are older than the cutoff as the query returns
        an empty set.

        Parameters
        ----------
        days : int, optional
            Set number of days to clean, default set in the config file.
        """
        if days:
            clean_days = int(days)
        else:
            clean_days = int(_conf.get_option(
                'local', 'local_clean_days',CLEAN_OLDER_THAN
            ))
        current_time = _dt.now()
        cutoff = current_time - _td(days=clean_days)
        jobs = self.db.query().filter(Job.submit_time < cutoff).all()
        if not jobs:
            return
        session = self.db.get_session()
        for job in jobs:
            try:
                session.delete(job)
            except InvalidRequestError:
                # In case open in another thread
                local_job = session.merge(job)
                other_session = session.object_session(job)
                session.delete(local_job)
                session.commit()
                other_session.close()
        session.commit()
        session.close()

    @Pyro4.expose
    def kill(self, jobs):
        """Kill running or pending jobs.

        Parameters
        ----------
        jobs : list of int
        """
        if isinstance(jobs, (_str, _txt)):
            jobs = [int(jobs)]
        elif isinstance(jobs, int):
            jobs = [jobs]
        else:
            try:
                jobs = list(jobs)
            except TypeError:
                jobs = [jobs]
        for job in jobs:
            self.check_jobno(job)
            self.inqueue.put('kill', int(job))
        jobs = self.get(jobs)
        ok_states = ['killed', 'completed', 'failed']
        for job in jobs:
            if job.state not in ok_states:
                return False
        return True

    @Pyro4.expose
    @property
    def available_cores(self):
        """Return an integer count of free cores."""
        self.inqueue.put('available_cores')
        cores = self.outqueue.get()
        return int(cores)

    ##########################################################################
    #                          Database Management                           #
    ##########################################################################

    @Pyro4.expose
    def update_job(self, jobno, state=None, exitcode=None, pid=None):
        """Update either the state or the exitcode of a job in the DB."""
        session = self.db.get_session()
        job = session.query(Job).filter(Job.jobno == int(jobno)).first()
        if state:
            job.state = state
        if isinstance(exitcode, int):
            job.exitcode = exitcode
        if isinstance(pid, int):
            job.pid = pid
        session.flush()
        session.commit()
        session.close()

    #  def _housekeeping(self):
        #  """Run by Pyro4, update all_jobs, db cache, and clean up."""
        #  self.clean()
        #  all_jobs = []
        #  cache    = {}
        #  session = self.db.get_session()
        #  for job in session.query(Job).all():
            #  all_jobs.append(job.id)
            #  cache[job.id] = job
        #  self.all_jobs = all_jobs
        #  self._cache   = cache

    ##########################################################################
    #                                Shutdown                                #
    ##########################################################################

    @Pyro4.expose
    def shutdown_jobs(self):
        """Kill all jobs and terminate."""
        result = None
        if not self.inqueue._closed:
            self.inqueue.put('stop')
            print('waiting for jobs to terminate gracefully')
            try:
                result = self.outqueue.get(timeout=STOP_WAIT)
            except Empty:
                pass
        print('killing runner')
        _kill_proc_tree(self._job_runner.pid)
        if _pid_exists(self._job_runner.pid):
            _os.kill(self._job_runner.pid, _signal.SIGKILL)
        print('job_runner killing done')
        try:
            self.inqueue.close()
            self.outqueue.close()
        except AttributeError:
            pass
        self.daemon.shutdown()
        if result is None:
            return None
        elif result:
            return True
        return False

    ##########################################################################
    #                             Error Checking                             #
    ##########################################################################

    def check_jobno(self, jobno):
        """Check if jobno in self.all_jobs, raise QueueError if not."""
        if jobno not in self.all_jobs:
            _logme.log('Job number {0} does not exist.'.format(jobno), 'error')
            return False
        return True

    def check_runner(self):
        """Make sure job_runner is active and start it if inactive."""
        if self._job_runner and self._job_runner.is_alive():
            return self._job_runner
        runner = mp.Process(
            target=job_runner,
            args=(self.inqueue, self.outqueue, self.max_jobs)
        )
        runner.start()
        self._job_runner = runner

    @property
    def job_runner(self):
        """A job runner process."""
        self.check_runner()
        return self._job_runner

    ##########################################################################
    #                           Interaction Stuff                            #
    ##########################################################################

    #  @Pyro4.expose
    #  @property
    #  def running(self):
        #  """Return all running job ids from the cache, no new database query."""
        #  return [job.id for job in self._cache if job.state == 'running']

    #  @Pyro4.expose
    #  @property
    #  def pending(self):
        #  """Return all pending job ids from the cache, no new database query."""
        #  return [job.id for job in self._cache if job.state == 'pending']

    #  @Pyro4.expose
    #  @property
    #  def completed(self):
        #  """Return all completed job ids from the cache, no new database query."""
        #  return [job.id for job in self._cache if job.state == 'completed']

    #  @Pyro4.expose
    #  @property
    #  def failed(self):
        #  """Return all failed job ids from the cache, no new database query."""
        #  return [job.id for job in self._cache if job.state == 'failed']

    #  def __repr__(self):
        #  """Simple information."""
        #  return "LocalQueue<running:{0};pending:{1};completed:{2}".format(
            #  self.running, self.pending, self.completed
        #  )

    #  def __str__(self):
        #  """Simple information."""
        #  return self.__repr__()

    @Pyro4.expose
    def __len__(self):
        """Length from the cache, no new database query."""
        return len(self.all_jobs)

    #  @Pyro4.expose
    #  def __getitem__(self, key):
        #  """Get a job by id."""
        #  job = self._cache[key]
        #  return (
            #  job.jobno, job.name, job.command, job.state, job.threads,
            #  job.exitcode, job.runpath, job.outfile, job.errfile
        #  )


def job_runner(inqueue, outqueue, max_jobs):
    """Run jobs with dependency tracking.

    Terminate by sending 'stop' to inqueue

    Parameters
    ----------
    inqueue : multiprocessing.Queue
        inqueue puts must be in the form : (command, extra):
            `('stop')` : immediately shutdown this process
            `('queue', job_info)` : queue and run this job
            `('kill', jobno)` : immediately kill this job
            `('available_cores')` : put available core count in outqueue
        job_info must be in the form:
            `(int(jobno), str(command), int(threads), list(dependencies))`
    outqueue : multiprocessing.Queue
        job information available_cores if argument was available_cores
    max_jobs : int
        The maximum number of concurrently running jobs, will be adjusted to
        be 4 <= max_jobs <= cpu_count. The minimum of 4 jobs is a hard limit
        and is enforced, so a machine with only 2 cores will still end up with
        4 jobs running. This is required to avoid hangs on some kinds of fyrd
        jobs, where a split job is created from a child process.

    Returns
    -------
    bool
        If 'stop' is sent, will return `True` if there are no running or
        pending jobs and `False` if there are still running or pending jobs.

    Raises
    ------
    QueueError
        If invalid argument put into inqueue
    """
    if not _WE_ARE_A_SERVER:
        return
    tries = 5
    while tries:
        qserver = get_server()
        if qserver:
            break
        _sleep(1)
        tries -= 1
        continue
    if not qserver:
        qserver = get_server(raise_on_error=True)
    max_jobs = int(max_jobs)
    if max_jobs < mp.cpu_count():
        max_jobs = mp.cpu_count()
    if max_jobs < 4:
        max_jobs = 4
    available_cores = max_jobs
    running = {}     # {jobno: Process}
    queued  = _OD()  # {jobno: {'command': command, 'depends': depends, ...}
    done    = {}     # {jobno: Process}
    jobs    = []     # [jobno, ...]
    put_core_info = False
    while True:
        # Get everything from the input queue first, queue everything
        while True:
            if inqueue.empty():
                break
            info = inqueue.get()  # Will block if input queue empty
            if info == 'stop' or info[0] == 'stop':
                good = True
                pids = []
                if running:
                    good = False
                    for jobno, job in running.items():
                        qserver.update_job(jobno, state='killed')
                        pids.append(job.pid)
                        job.terminate()
                if queued:
                    good = False
                    for jobno, job in queued.items():
                        qserver.update_job(jobno, state='killed')
                for pid in pids:
                    if _pid_exists(pid):
                        _os.kill(pid, _signal.SIGKILL)
                outqueue.put(good)
                return good
            if info == 'available_cores' or info[0] == 'available_cores':
                put_core_info = True
                continue
            if info[0] == 'kill':
                jobno = int(info[1])
                if jobno in running:
                    running[jobno].terminate()
                    qserver.update_job(jobno, state='killed')
                    running.pop(jobno)
                if jobno in queued:
                    queued.pop(jobno)
                    qserver.update_job(jobno, state='killed')
                continue
            if info[0] != 'queue':
                raise QueueError('Invalid argument: {0}'.format(info[0]))
            jobno, command, threads, depends, stdout, stderr, runpath = info[1]
            if not command:
                raise QueueError('Job command is {0}, cannot continue'
                                 .format(type(command)))
            jobno = int(jobno)
            threads = int(threads)
            # Run anyway
            if threads >= max_jobs:
                threads = max_jobs-1
            # Add to queue
            if jobno in jobs:
                # This should never happen
                raise QueueError('Job already submitted!')
            jobs.append(jobno)
            queued[jobno] = {'command': command, 'threads': threads,
                             'depends': depends, 'stdout': stdout,
                             'stderr': stderr, 'runpath': runpath}
            qserver.update_job(jobno, state='pending')
        # Update running and done queues
        for jobno, process in running.items():
            if process.is_alive():
                continue
            # Completed
            process.join()
            code = process.exitcode
            state = 'completed' if code == 0 else 'failed'
            qserver.update_job(jobno, state=state, exitcode=code)
            done[jobno] = process
        # Remove completed jobs from running
        for jobno in done:
            if jobno in running:
                p = running.pop(jobno)
                available_cores += p.cores
        # Start jobs if can run
        if available_cores > max_jobs:
            available_cores = max_jobs
        if available_cores < 0:  # Shouldn't happen
            available_cores = 0
        if put_core_info:
            outqueue.put(available_cores)
            put_core_info = False
        for jobno, info in queued.items():
            if info['depends']:
                not_done = []
                for dep_id in info['depends']:
                    if dep_id not in done:
                        not_done.append(dep_id)
                if not_done:
                    continue
            if info['threads'] <= available_cores:
                if info['runpath']:
                    curpath = _os.path.abspath('.')
                    _os.chdir(info['runpath'])
                p = mp.Process(
                    target=_run.cmd,
                    args=(info['command'],),
                    kwargs={
                        'stdout': info['stdout'],
                        'stderr': info['stderr'],
                    }
                )
                p.daemon = True
                p.start()
                running[jobno] = p
                available_cores -= info['threads']
                p.cores = info['threads']
                if info['runpath']:
                    _os.chdir(curpath)
            qserver.update_job(jobno, state='running', pid=p.pid)
        # Clear running jobs from queue
        for jobno in running:
            if jobno in queued:
                queued.pop(jobno)
        # Block for a moment to avoid running at 100% cpu
        _sleep(SLEEP_LEN)


###############################################################################
#                  Daemon Creation and Management Functions                   #
###############################################################################


def get_uri():
    """Get the URI from the config or file.

    Tests if URI is active before returning.

    Returns
    -------
    uri : str or None
        If file does not exist or URI is inactive, returns None and deletes
        URI_FILE, else returns the URI as a string.
    """
    curi = _conf.get_option('local', 'server_uri')
    if curi:
        t = _test_uri(curi)
        if t == 'connected':
            return curi
        if t == 'invalid':
            _conf.set_option('local', 'server_uri', None)
        return None
    if not _os.path.isfile(URI_FILE):
        return None
    with open(URI_FILE) as fin:
        uri = fin.read().strip()
    t = _test_uri(uri)
    if t == 'connected':
        return uri
    _os.remove(URI_FILE)
    return None


def _test_uri(uri):
    """Test if a URI refers to an accessible Pyro4 object."""
    try:
        p = Pyro4.Proxy(uri)
    except Pyro4.errors.PyroError:
        _logme.log('URI {0} in an invalid URI', 'error')
        return 'invalid'
    try:
        if p._pyroBind():
            out = 'connected'
        elif p.available_cores:
            out = 'connected'
        else:
            out = 'disconnect'
        if out == 'connected':
            p._pyroRelease()
        return out
    except Pyro4.errors.CommunicationError:
        _logme.log('URI {0} is not connected', 'warn')
        return 'disconnect'


def daemonizer():
    """Create the server daemon."""
    # Get pre-configured URI if available
    curi = _conf.get_option('local', 'server_uri')
    utest = _test_uri(curi) if curi else None
    # Test if there is already another daemon running
    crun = True if utest == 'connected' else False
    if crun or server_running():
        raise QueueError('Daemon already running, cannot start')
    # Set port and host if present in URI
    if utest == 'disconnect':
        uri = Pyro4.URI(curi)
        args = {'host': uri.host, 'port': uri.port}
        objId = uri.object
    else:
        args = {}
        objId = "QueueManager"
    # Create the daemon
    with Pyro4.Daemon(**args) as daemon:
        queue_manager = QueueManager(daemon)
        uri = daemon.register(queue_manager, objectId=objId)
        #  daemon.housekeeping = queue_manager._housekeeping

        with open(PID_FILE, 'w') as fout:
            fout.write(str(_os.getpid()))
        with open(URI_FILE, 'w') as fout:
            fout.write(str(uri))

        print("Ready. Object uri =", uri)
        daemon.requestLoop()


def shutdown_queue():
    """Kill the server and queue gracefully."""
    good = True
    server = get_server(start=False)
    if server:
        try:
            res = server.shutdown_jobs()
        except OSError:
            res = None
        except Pyro4.errors.CommunicationError:
            res = None
        _logme.log('Local queue runner terminated.', 'debug')
        if res is None:
            _logme.log('Could not determine process completion state',
                       'warn')
            good = False
        elif res:
            _logme.log('All jobs completed', 'debug')
        else:
            _logme.log('Some jobs failed!', 'error', also_write='stderr')
            good = False
    else:
        _logme.log('Server appears already stopped', 'info')
    kill_queue()
    _logme.log('Local queue terminated', 'info')
    return 0 if good else 1


def kill_queue():
    """Kill the server and queue without trying to clean jobs."""
    if _os.path.isfile(PID_FILE):
        with open(PID_FILE) as fin:
            pid = int(fin.read().strip())
        _os.remove(PID_FILE)
        _kill_proc_tree(pid, including_parent=True)
        if _pid_exists(pid):
            _os.kill(pid, _signal.SIGKILL)
    if _os.path.isfile(URI_FILE):
        _os.remove(URI_FILE)


def daemon_manager(mode):
    """Manage the daemon process

    Parameters
    ----------
    mode : {'start', 'stop', 'restart', 'status'}

    Returns
    -------
    status : int
        0 on success, 1 on failure
    """
    global _WE_ARE_A_SERVER
    _WE_ARE_A_SERVER = True
    check_conf()
    if mode == 'start':
        return _start()
    elif mode == 'stop':
        return _stop()
    elif mode == 'restart':
        _stop()
        return _start()
    elif mode == 'status':
        running = server_running()
        if running:
            _logme.log('Local queue server is running', 'info',
                       also_write='stderr')
        else:
            _logme.log('Local queue server is not running', 'info',
                       also_write='stderr')
        return 0 if running else 1
    _logme.log('Invalid mode {0}'.format(mode), 'error')
    return 1

def _start():
    """Start the daemon process as a fork."""
    if _os.path.isfile(PID_FILE):
        with open(PID_FILE) as fin:
            pid = fin.read().strip()
        if _pid_exists(pid):
            _logme.log('Local queue already running with pid {0}'
                       .format(pid), 'info')
            return 1
        _os.remove(PID_FILE)
    pid = _os.fork()
    if pid == 0: # The first child.
        daemonizer()
    else:
        _logme.log('Local queue starting', 'info')
        _sleep(1)
        if server_running():
            return 0
        _logme.log('Server failed to start', 'critical')
        return 1


def _stop():
    """Stop the daemon process."""
    if not _os.path.isfile(PID_FILE):
        _logme.log('Queue does not appear to be running, cannot stop',
                   'info')
        return 1
    return shutdown_queue()


def _kill_proc_tree(pid, including_parent=True):
    """Kill an entire process tree."""
    parent = _psutil.Process(int(pid))
    if hasattr(parent, 'get_children'):
        parent.children = parent.get_children
    for child in parent.children(recursive=True):
        child.kill()
    if including_parent:
        parent.kill()

###############################################################################
#                               Fyrd Functions                                #
###############################################################################


###############################################################################
#                             Functionality Test                              #
###############################################################################


def queue_test(warn=True):
    """Check that this batch system can be used.

    Parameters
    ----------
    warn : bool
        log a warning on fail

    Returns
    -------
    batch_system_functional : bool
    """
    log_level = 'error' if warn else 'debug'
    try:
        if not server_running():
            start_server()
        return server_running()
    except:
        _logme.log('Cannot get local queue sever address', log_level)
        return False


###############################################################################
#                           Normalization Functions                           #
###############################################################################


def normalize_job_id(job_id):
    """Convert the job id into job_id, array_id."""
    return str(int(job_id)), None


def normalize_state(state):
    """Convert state into standardized (slurm style) state."""
    state = state.lower()
    if state == 'queued':
        state = 'pending'
    return state


###############################################################################
#                               Job Submission                                #
###############################################################################


def gen_scripts(job_object, command, args, precmd, modstr):
    """Build the submission script objects.

    Parameters
    ---------
    job_object : fyrd.job.Job
    command : str
        Command to execute
    args : list
        List of additional arguments, not used in this script.
    precmd : str
        String from options_to_string() to add at the top of the file, should
        contain batch system directives
    modstr : str
        String to add after precmd, should contain module directives.

    Returns
    -------
    fyrd.script_runners.Script
        The submission script
    None
        Would be the exec_script, not used here.
    """
    scrpt = _os.path.join(
        job_object.scriptpath,
        '{0}.{1}.{2}'.format(job_object.name, job_object.suffix, SUFFIX)
    )

    sub_script = _scrpts.CMND_RUNNER_TRACK.format(
        precmd=precmd, usedir=job_object.runpath, name=job_object.name,
        command=command
    )
    return _Script(script=sub_script, file_name=scrpt), None


def submit(file_name, dependencies=None, job=None, args=None, kwds=None):
    """Submit any file with dependencies.

    .. note:: this function can only use the following fyrd keywords:
        cores, name, outfile, errfile, runpath

    We get those in the following order:
        1. Job object
        2. args
        3. kwds

    Any keyword in a later position will overwrite the earlier position. ie.
    'name' in kwds would overwrite 'name' in args which would overwrite 'name'
    in Job.

    None of these keywords are required. If they do not exist, cores is set to
    1, name is set to `file_name`, no runpath is used, and STDOUT/STDERR are
    not saved

    Parameters
    ----------
    file_name : str
        Path to an existing file
    dependencies : list
        List of dependencies
    job : fyrd.job.Job, optional
        A job object for the calling job, used to get cores, outfile, errfile,
        runpath, and name if available
    args : list, optional
        A list of additional arguments, only parsed if list of tuple in the
        format `[(key, value)]`. Comes from output of `parse_strange_options()`
    kwds : dict or str, optional
        A dictionary of keyword arguments to parse with options_to_string, or
        a string of option:value,option,option:value,....
        Used to get any of cores, outfile, errfile, runpath, or name

    Returns
    -------
    job_id : str
    """
    params = {}
    needed_params = ['cores', 'outfile', 'errfile', 'runpath', 'name']
    if job:
        params['cores'] = job.cores
        params['outfile'] = job.outfile
        params['errfile'] = job.errfile
        params['runpath'] = job.runpath
        params['name'] = job.name
    if kwds:
        if not isinstance(kwds, dict):
            kwds = {k: v for k, v in [i.split(':') for i in kwds.split(',')]}
        _, extra_args = _options.options_to_string(kwds)
        for k, v in extra_args:
            if k in needed_params:
                params[k] = v
    if args and isinstance(args[0], (list, tuple)):
        for k, v in args:
            if k in needed_params:
                params[k] = v
    if 'cores' not in params:
        params['cores'] = 1
    if 'name' not in params:
        params['name'] = _os.path.basename(file_name)
    for param in needed_params:
        if param not in params:
            params[param] = None
    # Submit the job
    server = get_server()
    if not _os.path.isfile(file_name):
        raise QueueError('File {0} does not exist, cannot submit'
                         .format(file_name))
    command = 'bash {0}'.format(_os.path.abspath(file_name))
    jobno = server.submit(
        command, params['name'], threads=params['cores'],
        dependencies=dependencies, stdout=params['outfile'],
        stderr=params['errfile'], runpath=params['runpath']
    )
    return str(jobno)


###############################################################################
#                               Job Management                                #
###############################################################################


def kill(job_ids):
    """Terminate all jobs in job_ids.

    Parameters
    ----------
    job_ids : list or str
        A list of valid job ids or a single valid job id

    Returns
    -------
    success : bool
    """
    server = get_server()
    return server.kill(job_ids)


###############################################################################
#                                Queue Parsing                                #
###############################################################################


def queue_parser(user=None, partition=None):
    """Iterator for queue parsing.

    Simply ignores user and partition requests.

    Parameters
    ----------
    user : str, NOT IMPLEMENTED
        User name to pass to qstat to filter queue with
    partition : str, NOT IMPLEMENTED
        Partition to filter the queue with

    Yields
    ------
    job_id : str
    array_id : str or None
    name : str
    userid : str
    partition : str
    state :str
    nodelist : list
    numnodes : int
    cntpernode : int or None
    exit_code : int or Nonw
    """
    server = get_server(start=True)
    user = _getpass.getuser()
    host = _socket.gethostname()
    for job in server.get():  # Get all jobs in the database
        job_id     = str(job[0])
        array_id   = None
        name       = job[1]
        userid     = user
        partition  = None
        state      = normalize_state(job[3])
        nodelist   = [host]
        numnodes   = 1
        cntpernode = job[4]
        exit_code  = job[5]
        yield (job_id, array_id, name, userid, partition, state, nodelist,
               numnodes, cntpernode, exit_code)


def parse_strange_options(option_dict):
    """Parse all options that cannot be handled by the regular function.

    Because we do not parse the submission file, all keywords will be placed
    into the final return list.

    Parameters
    ----------
    option_dict : dict
        All keyword arguments passed by the user that are not already defined
        in the Job object

    Returns
    -------
    list
        An empty list
    dict
        An empty dictionary
    list
        A list of options that can be used by `submit()`
        Ends up in the `args` parameter of the submit function
    """
    outlist = []
    good_items = ['outfile', 'cores', 'errfile', 'runpath']
    for opt, var in option_dict.items():
        if opt in good_items:
            outlist.append((opt, var))
    return [], {}, outlist


###############################################################################
#                              User Interaction                               #
###############################################################################


def command_line_parser():
    """Parse command line options.

    Returns
    -------
    parser
    """
    parser  = _argparse.ArgumentParser(
        description=__doc__,
        formatter_class=_argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        'mode', choices={'start', 'stop', 'status', 'restart'},
        metavar='{start,stop,status,restart}', help='Server command'
    )

    return parser


def main(argv=None):
    """Parse command line options to run as a script."""
    if not argv:
        argv = sys.argv[1:]

    parser = command_line_parser()

    args = parser.parse_args(argv)

    # Call the subparser function
    return daemon_manager(args.mode)

if __name__ == '__main__' and '__file__' in globals():
    sys.exit(main())
