# -*- coding: utf-8 -*-
"""
Wrapper functions to make some common jobs easier.

Functions
---------
submit
    Submit a script to the cluster
make_job
    Make a job compatible with the chosen cluster but do not submit
make_job_file
    Make a job file compatible with the chosen cluster.
clean
    Delete all files in jobs list or single Job object.
submit_file
    Submit an existing job file to the cluster.
clean_work_dirs
    Clean all files in the scriptpath and outpath directories.
clean_dir
    Delete all files made by this module in directory.
wait
    Wait for jobs to finish.
get
    Get results of jobs when they complete.
"""
import os  as _os
import sys as _sys
from time import sleep as _sleep
from subprocess import CalledProcessError as _CalledProcessError

###############################################################################
#                               Import Ourself                                #
###############################################################################


from . import run   as _run
from . import conf  as _conf
from . import queue as _queue
from . import logme as _logme
from . import batch_systems as _batch
from . import ClusterError as _ClusterError
from .job import Job

__all__ = ['submit', 'make_job', 'make_job_file', 'submit_file', 'clean_dir',
           'clean_work_dirs', 'clean', 'wait', 'get']

###############################################################################
#                            Submission Functions                             #
###############################################################################


def submit(command, args=None, kwargs=None, name=None, qtype=None,
           profile=None, queue=None, **kwds):
    """Submit a script to the cluster.

    Parameters
    ----------
    command : function/str
        The command or function to execute.
    args : tuple/dict, optional
        Optional arguments to add to command, particularly useful for
        functions.
    kwargs : dict, optional
        Optional keyword arguments to pass to the command, only used for
        functions.
    name : str, optional
        Optional name of the job. If not defined, guessed. If a job of the
        same name is already queued, an integer job number (not the queue
        number) will be added, ie.  <name>.1
    qtype : str, optional
        Override the default queue type
    profile : str, optional
        The name of a profile saved in the conf
    queue : fyrd.queue.Queue, optional
        An already initiated Queue class to use.
    kwds
        *All other keywords are parsed into cluster keywords by the options
        system.* For available keywords see `fyrd.option_help()`

    Returns
    -------
    Job object
    """

    _batch.check_queue()  # Make sure the queue.MODE is usable

    job = Job(command=command, args=args, kwargs=kwargs, name=name,
              qtype=qtype, profile=profile, queue=queue, **kwds)

    job.write()
    job.submit()
    job.update()

    return job


#########################
#  Job file generation  #
#########################


def make_job(command, args=None, kwargs=None, name=None, qtype=None,
             profile=None, queue=None, **kwds):
    """Make a job compatible with the chosen cluster but do not submit.

    Parameters
    ----------
    command : function/str
        The command or function to execute.
    args : tuple/dict, optional
        Optional arguments to add to command, particularly useful for
        functions.
    kwargs : dict, optional
        Optional keyword arguments to pass to the command, only used for
        functions.
    name : str, optional
        Optional name of the job. If not defined, guessed. If a job of the
        same name is already queued, an integer job number (not the queue
        number) will be added, ie.  <name>.1
    qtype : str, optional
        Override the default queue type
    profile : str, optional
        The name of a profile saved in the conf
    queue : fyrd.queue.Queue, optional
        An already initiated Queue class to use.
    kwds
        *All other keywords are parsed into cluster keywords by the options
        system.* For available keywords see `fyrd.option_help()`

    Returns
    -------
    Job object
    """

    _batch.check_queue()  # Make sure the queue.MODE is usable

    job = Job(command=command, args=args, kwargs=kwargs, name=name,
              qtype=qtype, profile=profile, queue=queue, **kwds)

    # Return the path to the script
    return job


def make_job_file(command, args=None, kwargs=None, name=None, qtype=None,
                  profile=None, queue=None, **kwds):
    """Make a job file compatible with the chosen cluster.

    Parameters
    ----------
    command : function/str
        The command or function to execute.
    args : tuple/dict, optional
        Optional arguments to add to command, particularly useful for
        functions.
    kwargs : dict, optional
        Optional keyword arguments to pass to the command, only used for
        functions.
    name : str, optional
        Optional name of the job. If not defined, guessed. If a job of the
        same name is already queued, an integer job number (not the queue
        number) will be added, ie.  <name>.1
    qtype : str, optional
        Override the default queue type
    profile : str, optional
        The name of a profile saved in the conf
    queue : fyrd.queue.Queue, optional
        An already initiated Queue class to use.
    kwds
        *All other keywords are parsed into cluster keywords by the options
        system.* For available keywords see `fyrd.option_help()`

    Returns
    -------
    str
        Path to job file
    """

    _batch.check_queue()  # Make sure the queue.MODE is usable

    job = Job(command=command, args=args, kwargs=kwargs, name=name,
              qtype=qtype, profile=profile, queue=queue, **kwds)

    job = job.write()

    # Return the path to the script
    return job.submission.file_name


##############
#  Cleaning  #
##############


def clean(jobs, clean_outputs=False):
    """Delete all files in jobs list or single Job object.

    Parameters
    ----------
    jobs : fyrd.job.Job or list of fyrd.job.Job
        Job objects to clean
    clean_outputs : bool
        Also clean outputs.
    """
    jobs = _run.listify(jobs)
    for job in jobs:
        job.clean(delete_outputs=clean_outputs)


###############################################################################
#                      Job Object Independent Functions                       #
###############################################################################


def submit_file(script_file, dependencies=None, qtype=None, submit_args=None):
    """Submit an existing job file to the cluster.

    This function is independent of the Job object and just submits a file
    using a cluster appropriate method.

    Parameters
    ----------
    script_file : str
        The path to the file to submit
    dependencies: str or list of strings, optional
        A job number or list of job numbers to depend on
    qtype : str, optional
        The name of the queue system to use, auto-detected if not given.
    submit_args : dict
        A dictionary of keyword arguments for the submission script.

    Returns
    -------
    job_number : str
    """
    qtype = qtype if qtype else _batch.get_cluster_environment()
    _batch.check_queue(qtype)
    dependencies = _run.listify(dependencies)

    batch = _batch.get_batch_system(qtype)
    return batch.submit(script_file, dependencies)


def clean_work_dirs(outputs=False, confirm=False):
    """Clean all files in the scriptpath and outpath directories.

    Parameters
    ----------
    outputs : bool
        Also delete output files.
    confirm : bool
        Confirm on command line before deleteing.

    Returns
    -------
    files : list
        A list of deleted files.
    """
    files = []
    scriptpath = _conf.get_option('jobs', 'scriptpath')
    outpath    = _conf.get_option('jobs', 'outpath')
    if scriptpath:
        files += clean_dir(scriptpath, delete_outputs=outputs, confirm=confirm)
    if outputs and outpath and outpath != scriptpath:
        files += clean_dir(outpath, delete_outputs=True, confirm=confirm)
    return files


def clean_dir(directory=None, suffix=None, qtype=None, confirm=False,
              delete_outputs=None):
    """Delete all files made by this module in directory.

    CAUTION: The clean() function will delete **EVERY** file with
             extensions matching those these::
                 .<suffix>.err
                 .<suffix>.out
                 .<suffix>.out.func.pickle
                 .<suffix>.sbatch & .<suffix>.script for slurm mode
                 .<suffix>.qsub for torque mode
                 .<suffix>.job for local mode
                 _func.<suffix>.py
                 _func.<suffix>.py.pickle.in
                 _func.<suffix>.py.pickle.out

    .. note:: This function will change in the future to use batch system
              defined paths.

    Parameters
    ----------
    directory : str
        The directory to run in, defaults to the current directory.
    suffix : str
        Override the default suffix.
    qtype : str
        Only run on files of this qtype
    confirm : bool
        Ask the user before deleting the files
    delete_outputs : bool
        Delete all output files too.

    Returns
    -------
    list
        A set of deleted files
    """
    _batch.check_queue(qtype)  # Make sure the queue.MODE is usable

    if delete_outputs is None:
        delete_outputs = _conf.get_option('jobs', 'clean_outputs')

    # Sanitize arguments
    directory = _os.path.abspath(directory if directory else '.')
    suffix = suffix if suffix else _conf.get_option('jobs', 'suffix')

    # Extension patterns to delete
    extensions = ['_func.' + suffix + '.py']
    if delete_outputs:
        extensions += ['.' + suffix + '.err', '.' + suffix + '.out',
                       '_func.' + suffix + '.py.pickle.out',
                       '.' + suffix + '.out.func.pickle',
                       '.' + suffix + '.job']

    if qtype:
        if qtype == 'local':
            extensions.append('.' + suffix)
        elif qtype == 'slurm':
            extensions += ['.' + suffix + '.sbatch', '.' + suffix + '.script']
        elif qtype== 'torque':
            extensions.append('.' + suffix + '.qsub')
    else:
        extensions.append('.' + suffix)
        extensions.append('_func.' + suffix + '.py.pickle.in')
        extensions += ['.' + suffix + '.sbatch', '.' + suffix + '.script']
        extensions.append('.' + suffix + '.qsub')

    files = [_os.path.join(directory, i) for i in _os.listdir(directory)]
    files = [i for i in files if _os.path.isfile(i)]

    if not files:
        _logme.log('No files found.', 'debug')
        return []

    deleted = []
    for f in files:
        for extension in extensions:
            if f.endswith(extension):
                deleted.append(f)

    deleted = sorted(deleted)
    delete  = False

    if confirm:
        if deleted:
            prompt = [_os.path.basename(i) for i in deleted]
            _sys.stdout.write('Directory: {}\n'.format(directory))
            _sys.stdout.write('Files to delete::\n\t')
            _sys.stdout.write('\n\t'.join(prompt) + '\n')
            answer = _run.get_input("Do you want to delete these files? [Y/n]",
                                    'yesno', 'y')
            if answer == 'y':
                delete  = True
                _sys.stdout.write('Deleting...\n')
            else:
                _sys.stdout.write('Aborting\n')
                delete  = False
                deleted = []
        else:
            _sys.stdout.write('No files to delete.\n')
    else:
        delete = True

    if delete and deleted:
        for f in deleted:
            _os.remove(f)
        if confirm:
            _sys.stdout.write('Done\n')

    return deleted


###############################################################################
#               Simple Wrapper to Wait on Queue and Get Outputs               #
###############################################################################


def wait(jobs, notify=True, queue=None):
    """Wait for jobs to finish.

    Only works on user jobs by default. To work on jobs so someone else,
    initialize a fyrd.queue.Queue class with their user info and pass as an
    argument to queue.

    Parameters
    ----------
    jobs : fyrd.job.Job or str or list of either (mixed list fine)
        A single job or list of jobs, either Job objects or job numbers
    notify : str, True, or False, optional
        If True, both notification address and wait_time must be set in
        the [notify] section of the config. A notification email will be
        sent if the time exceeds this time. This is the default.
        If a string is passed, notification is forced and the string must
        be the to address.
        False means no notification
    queue : fyrd.queue.Queue, optional
        An already initiated Queue class to use.

    Returns
    -------
    success : bool
        True if all jobs successful, false otherwise
    """
    q = queue if queue else _queue.default_queue()
    return q.wait(jobs, notify=notify)


def get(jobs, queue=None):
    """Get results of jobs when they complete.

    Only works on user jobs by default. To work on jobs so someone else,
    initialize a fyrd.queue.Queue class with their user info and pass as an
    argument to queue.

    Parameters
    ----------
    jobs : fyrd.job.Job or list of fyrd.job.Job

    Returns
    -------
    list
        Outputs (STDOUT or return value) of jobs
    queue : fyrd.queue.Queue, optional
        An already initiated Queue class to use.

    .. note:: This function also modifies the input Job objects, so they will
              contain all outputs and state information.
    """
    q = queue if queue else _queue.default_queue()
    return q.get(jobs)
