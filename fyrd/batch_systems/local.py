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
import atexit as _atexit
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

from queue import Empty

import Pyro4
import daemonocle as _daemon

from sqlalchemy import create_engine as _create_engine
from sqlalchemy import Column as _Column
from sqlalchemy import String as _String
from sqlalchemy import Integer as _Integer

from sqlalchemy.types import DateTime as _DateTime
from sqlalchemy.orm import sessionmaker as _sessionmaker
from sqlalchemy.ext.declarative import declarative_base as _base

# Fyrd imports (not necessary for main functionality)
from .. import run as _run
from .. import conf as _conf
from .. import logme as _logme
from .. import options as _options
from .. import script_runners as _scrpts
from .. import submission_scripts as _sscrpt
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
# Must be at least 10
STOP_WAIT = 30

# Number of days to wait before cleaning out old jobs, obtained from the
# config file, this just sets the default
CLEAN_OLDER_THAN = 7

############################
#  Do Not Edit Below Here  #
############################


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
    runpath     = _Column(_String)
    outfile     = _Column(_String)
    errfile     = _Column(_String)

    def __repr__(self):
        """Display summary."""
        return 'LocalQueueJob<{0};{1};state:{2};exitcode:{3}>'.format(
            self.jobno, self.name, self.state, self.exitcode
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
        self.engine  = _create_engine('sqlite:///{}'.format(self.db_file))
        if not _os.path.isfile(self.db_file):
            self.create_database(confirm=False)

    ##########################################################################
    #                           Basic Connectivity                           #
    ##########################################################################

    def get_session(self):
        """Return session for this database."""
        Session = _sessionmaker(bind=self.engine)
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
            pass columns: query(self, self.table.rsID) would return only a list
            of rsids.
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
        return self.get_jobs(state='queued')

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
        if isinstance(x, str):
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


def get_server_uri(round_2=False):
    """Check status and return a server URI."""
    check_conf()
    us = _os.path.realpath(__file__)
    if not _os.path.isfile(PID_FILE):
        subprocess.check_call([sys.executable, us, 'server', 'start'])
    if not _os.path.isfile(PID_FILE):
        raise OSError('Cannot start server')
    with open(PID_FILE) as fin:
        pid = int(fin.read().strip())
        if round_2:
            return pid
    if not _pid_exists(pid):
        _os.remove(PID_FILE)
        pid = get_server_uri(round_2=True)
        if not _pid_exists(pid):
            raise OSError('Server not starting')
    # Server running now
    with open(URI_FILE) as fin:
        return fin.read().strip()


def get_server():
    """Return a client-side QueueManager instance."""
    uri = get_server_uri()
    return Pyro4.Proxy(uri)


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

    def __init__(self, max_jobs=None):
        """Create the QueueManager

        Paramenters
        -----------
        max_jobs : int, optional
            The maximum number of jobs to run at one time. Defaults to current
            CPUs - 1
        """
        self.max_jobs = max_jobs if max_jobs else mp.cpu_count()-1
        if self.max_jobs > mp.cpu_count():
            self.max_jobs = mp.cpu_count()-1
        self.db = LocalQueue(DATABASE)
        self.all_jobs = [i[0] for i in self.db.query(Job.jobno).all()]
        self.check_runner()

        # Properly shutdown if process terminated
        _atexit.register(self.shutdown)


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
            If a dependency is not in the job db already
        """
        session = self.db.get_session()
        threads = int(threads)
        depends = []
        if dependencies:
            for dep in dependencies:
                dep = int(dep)
                self.check_jobno(dep)
                depends.append(dep)
        job = Job(name=name, command=command, threads=threads, state='queued',
                  submit_time=_dt.now())
        if stdout:
            job.outfile = stdout
        if stderr:
            job.errfile = stderr
        if runpath:
            job.runpath = runpath
        session.add(job)
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
    def get(self, jobs=None, this_session_only=False, preclean=True):
        """Return a list of updated jobs as Job objects.

        Parameters
        ----------
        jobs : list of int, optional
            A list of job numbers, a single job number is also fine
        this_session_only : bool, optional
            If True, limit results to those from this class instance only
        preclean : bool
            If True run `clean()` first to remove old jobs

        Returns
        -------
        jobs : list of Job
        """
        if preclean:
            self.clean()
        q = self.db.query()
        if jobs:
            jobs = [jobs] if isinstance(jobs, (int, str)) else jobs
            jobs = [int(j) for j in jobs]
            q = q.filter(Job.jobno.in_(jobs))
        if this_session_only:
            q = q.filter(Job.jobno.in_(self.all_jobs))
        return q.all()

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
            clean_days = _conf.get_option(
                'queue', 'local_clean_days',CLEAN_OLDER_THAN
            )
        current_time = _dt.now()
        cutoff = current_time - _td(days=clean_days)
        jobs = self.db.query().filter(Job.submit_time < cutoff).all()
        if not jobs:
            return
        session = self.db.get_session()
        for job in jobs:
            session.delete(job)
        session.commit()
        session.close()

    @Pyro4.expose
    def kill(self, jobs):
        """Kill running or queued jobs.

        Parameters
        ----------
        jobs : list of int
        """
        if isinstance(jobs, str):
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
    def update_job(self, jobno, state=None, exitcode=None):
        """Update either the state or the exitcode of a job in the DB."""
        session = self.db.get_session()
        job = session.query(Job).filter(Job.jobno == int(jobno)).first()
        if state:
            job.state = state
        if isinstance(exitcode, int):
            job.exitcode = exitcode
        session.flush()
        session.commit()
        session.close()

    ##########################################################################
    #                                Shutdown                                #
    ##########################################################################

    @Pyro4.expose
    def shutdown(self):
        """Kill all jobs and terminate."""
        self.inqueue.put('stop')
        try:
            result = self.outqueue.get(STOP_WAIT-5)
        except Empty:
            result = None
        try:
            self._job_runner.terminate()
            self.inqueue.close()
            self.outqueue.close()
        except AttributeError:
            pass
        if _pid_exists(self._job_runner.pid):
            _os.kill(self._job_runner.pid, _signal.SIGKILL)
        _logme.log('Local queue terminated.', 'info')
        if result is None:
            _logme.log('Could not determine process completeion state',
                       'warn')
        elif result:
            _logme.log('All jobs completed', 'debug')
            return True
        else:
            _logme.log('Some jobs failed!', 'error', also_write='stderr')
            return False

    ##########################################################################
    #                             Error Checking                             #
    ##########################################################################

    def check_jobno(self, jobno):
        """Check if jobno in self.all_jobs, raise QueueError if not."""
        if jobno not in self.all_jobs:
            raise QueueError('Job number {0} does not exist.'.format(jobno))
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
        The maximum number of concurrently running jobs

    Returns
    -------
    bool
        If 'stop' is sent, will return `True` if there are no running or
        queued jobs and `False` if there are still running or queued jobs.

    Raises
    ------
    QueueError
        If invalid argument put into inqueue
    """
    qserver = get_server()
    running = {}    # {jobno: Process}
    queued  = _OD()  # {jobno: {'command': command, 'depends': depends}
    done    = {}    # {jobno: Process}
    jobs    = []    # [jobno, ...]
    put_core_info = False
    while True:
        # Get everything from the input queue first, queue everything
        while True:
            if inqueue.empty():
                break
            info = inqueue.get()  # Will block if input queue empty
            if info == 'stop' or info[0] == 'stop':
                if running:
                    for jobno, job in running.items():
                        qserver.update_job(jobno, state='killed')
                        job.terminate()
                    outqueue.put(False)
                    return False
                if queued:
                    outqueue.put(False)
                    return False
                outqueue.put(True)
                return True
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
            jobno = int(jobno)
            threads = int(threads)
            # Run anyway
            if threads > max_jobs:
                threads = max_jobs
            # Add to queue
            if jobno in jobs:
                # This should never happen
                raise QueueError('Job already submitted!')
            jobs.append(jobno)
            queued[jobno] = {'command': command, 'threads': threads,
                             'depends': depends, 'stdout': stdout,
                             'stderr': stderr, 'runpath': runpath}
            qserver.update_job(jobno, state='queued')
        # Update running and done queues
        for jobno, process in running.items():
            if process.is_alive():
                continue
            # Completed
            process.join()
            code = process.exitcode
            print(code)
            state = 'completed' if code == 0 else 'failed'
            qserver.update_job(jobno, state=state, exitcode=code)
            done[jobno] = process
        # Remove completed jobs from running
        for jobno in done:
            if jobno in running:
                running.pop(jobno)
        # Start jobs if can run
        available_cores = max_jobs-len(running)
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
                print(info['command'])
                assert isinstance(info['command'], str)
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
                p.start()
                running[jobno] = p
                available_cores -= info['threads']
                if info['runpath']:
                    _os.chdir(curpath)
            qserver.update_job(jobno, state='running')
        # Clear running jobs from queue
        for jobno in running:
            if jobno in queued:
                queued.pop(jobno)
        # Block for a moment to avoid running at 100% cpu
        _sleep(SLEEP_LEN)


###############################################################################
#                  Daemon Creation and Management Functions                   #
###############################################################################


def daemonizer():
    """Create the server daemon."""
    daemon = Pyro4.Daemon()
    uri = daemon.register(QueueManager)

    with open(URI_FILE, 'w') as fout:
        fout.write(str(uri))

    print("Ready. Object uri =", uri)
    daemon.requestLoop()


def shutdown_queue():
    """Kill the queue."""
    server = get_server()
    server.shutdown()
    _os.remove(URI_FILE)


def daemon_manager(mode):
    """Manage the daemon process"""
    check_conf()
    daemon = _daemon.Daemon(
        worker=daemonizer,
        shutdown_callback=shutdown_queue,
        pidfile=PID_FILE,
        stop_timeout=STOP_WAIT,
    )
    return daemon.do_action(mode)


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
        batch_system = get_server()
    except:
        _logme.log('Cannot get local queue sever address', log_level)
        return False
    return True


###############################################################################
#                           Normalization Functions                           #
###############################################################################


def normalize_job_id(job_id):
    """Convert the job id into job_id, array_id."""
    return str(int(job_id)), None


def normalize_state(state):
    """Convert state into standadized (slurm style) state."""
    # Already normalized
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
    partiton : str, NOT IMPLEMENTED
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
    server = get_server()
    user = _getpass.getuser()
    host = _socket.gethostname()
    for job in server.get():  # Get all jobs in the database
        job_id     = job.jobno
        array_id   = None
        name       = job.name
        userid     = user
        partition  = None
        state      = job.state
        nodelist   = [host]
        numnodes   = 1
        cntpernode = job.threads
        exit_code  = job.exitcode
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

    # Subcommands
    modes = parser.add_subparsers(dest='modes')

    server_mode = modes.add_parser('server', help='Run the server')
    server_mode.add_argument(
        'mode', choices={'start', 'stop', 'status', 'restart'},
        metavar='{start,stop,status,restart}', help='Server command')

    return parser


def main(argv=None):
    """Parse command line options to run as a script."""
    if not argv:
        argv = sys.argv[1:]

    parser = command_line_parser()

    args = parser.parse_args(argv)

    if not args.modes:
        parser.print_help()
        return 0

    # Call the subparser function
    if args.modes == 'server':
        daemon_manager(args.mode)

if __name__ == '__main__' and '__file__' in globals():
    sys.exit(main())
