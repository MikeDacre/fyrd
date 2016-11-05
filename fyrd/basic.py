# -*- coding: utf-8 -*-
"""
Functions to allow simple job and file submission without the Job class.

Last modified: 2016-11-04 17:49
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
from . import local as _local
from . import logme as _logme
from . import ClusterError as _ClusterError
from .job import Job

__all__ = ['submit', 'make_job', 'make_job_file', 'submit_file', 'clean_dir']

###############################################################################
#                            Submission Functions                             #
###############################################################################


def submit(command, args=None, name=None, path=None, qtype=None,
           profile=None, **kwargs):
    """Submit a script to the cluster.

    Args:
        command:   The command or function to execute.
        args:      Optional arguments to add to command, particularly
                   useful for functions.
        name:      The name of the job.
        path:      Where to create the script, if None, current dir used.
        qtype:     'torque', 'slurm', or 'normal'
        profile:   The name of a profile saved in the conf
        kwargs:    Keyword arguments to control job options

    There are many keyword arguments available for cluster job submission.
    These vary somewhat by queue type. For info run::

        fyrd.options.option_help()

    Returns:
        Job object
    """

    _queue.check_queue()  # Make sure the queue.MODE is usable

    job = Job(command=command, args=args, name=name, path=path, qtype=qtype,
              profile=profile, **kwargs)

    job.write()
    job.submit()
    job.update()

    return job


#########################
#  Job file generation  #
#########################


def make_job(command, args=None, name=None, path=None, qtype=None,
             profile=None, **kwargs):
    """Make a job file compatible with the chosen cluster.

    If mode is local, this is just a simple shell script.

    Args:
        command:   The command or function to execute.
        args:      Optional arguments to add to command, particularly
                   useful for functions.
        name:      The name of the job.
        path:      Where to create the script, if None, current dir used.
        qtype:     'torque', 'slurm', or 'normal'
        profile:   The name of a profile saved in the conf

    There are many keyword arguments available for cluster job submission.
    These vary somewhat by queue type. For info run::

        fyrd.options.option_help()

    Returns:
        A Job object
    """

    _queue.check_queue()  # Make sure the queue.MODE is usable

    job = Job(command=command, args=args, name=name, path=path, qtype=qtype,
              profile=profile, **kwargs)

    # Return the path to the script
    return job


def make_job_file(command, args=None, name=None, path=None, qtype=None,
                  profile=None, **kwargs):
    """Make a job file compatible with the chosen cluster.

    If mode is local, this is just a simple shell script.

    Args:
        command:   The command or function to execute.
        args:      Optional arguments to add to command, particularly
                   useful for functions.
        name:      The name of the job.
        path:      Where to create the script, if None, current dir used.
        qtype:     'torque', 'slurm', or 'normal'
        profile:   The name of a profile saved in the profiles file.
        kwargs:    Keyword arguments to control job options

    There are many keyword arguments available for cluster job submission.
    These vary somewhat by queue type. For info run::

        fyrd.options.option_help()

    Returns:
        Path to job script
    """

    _queue.check_queue()  # Make sure the queue.MODE is usable

    job = Job(command=command, args=args, name=name, path=path, qtype=qtype,
              profile=profile, **kwargs)

    job = job.write()

    # Return the path to the script
    return job.submission


##############
#  Cleaning  #
##############


def clean(jobs):
    """Delete all files in jobs list or single Job object."""
    if isinstance(jobs, Job):
        jobs = [jobs]
    if not isinstance(jobs, (list, tuple)):
        raise _ClusterError('Job list must be a Job, list, or tuple')
    for job in jobs:
        job.clean()


###############################################################################
#                      Job Object Independent Functions                       #
###############################################################################


def submit_file(script_file, dependencies=None, threads=None, qtype=None):
    """Submit a job file to the cluster.

    If qtype or queue.MODE is torque, qsub is used; if it is slurm, sbatch
    is used; if it is local, the file is executed with subprocess.

    This function is independent of the Job object and just submits a file.

    Args:
        dependencies: A job number or list of job numbers.
                      In slurm: `--dependency=afterok:` is used
                      For torque: `-W depend=afterok:` is used

        threads:      Total number of threads to use at a time, defaults to all.
                      ONLY USED IN LOCAL MODE

    Returns:
        job number for torque or slurm multiprocessing job object for local
        mode
    """
    _queue.check_queue()  # Make sure the queue.MODE is usable

    if not qtype:
        qtype = _queue.get_cluster_environment()

    # Check dependencies
    if dependencies:
        if isinstance(dependencies, (str, int)):
            dependencies = [dependencies]
        if not isinstance(dependencies, (list, tuple)):
            raise Exception('dependencies must be a list, int, or string.')
        dependencies = [str(i) for i in dependencies]

    if qtype == 'slurm':
        if dependencies:
            dependencies = '--dependency=afterok:{}'.format(
                ':'.join([str(d) for d in dependencies]))
            args = ['sbatch', dependencies, script_file]
        else:
            args = ['sbatch', script_file]
        # Try to submit job 5 times
        count = 0
        while True:
            code, stdout, stderr = _run.cmd(args)
            if code == 0:
                job = int(stdout.split(' ')[-1])
                break
            else:
                if count == 5:
                    _logme.log('sbatch failed with code {}\n'.format(code),
                               'stdout: {}\nstderr: {}'.format(stdout, stderr),
                               'critical')
                    raise _CalledProcessError(code, args, stdout, stderr)
                _logme.log('sbatch failed with err {}. Resubmitting.'.format(
                    stderr), 'debug')
                count += 1
                _sleep(1)
                continue
            break
        return job

    elif qtype == 'torque':
        if dependencies:
            dependencies = '-W depend={}'.format(
                ','.join(['afterok:' + d for d in dependencies]))
            args = ['qsub', dependencies, script_file]
        else:
            args = ['qsub', script_file]
        # Try to submit job 5 times
        count = 0
        while True:
            code, stdout, stderr = _run.cmd(args)
            if code == 0:
                job = int(stdout.split('.')[0])
                break
            else:
                if count == 5:
                    _logme.log('qsub failed with code {}\n'.format(code),
                               'stdout: {}\nstderr: {}'.format(stdout, stderr),
                               'critical')
                    raise _CalledProcessError(code, args, stdout, stderr)
                _logme.log('qsub failed with err {}. Resubmitting.'.format(
                    stderr), 'debug')
                count += 1
                _sleep(1)
                continue
            break
        return job

    elif qtype == 'local':
        # Normal mode dependency tracking uses only integer job numbers
        depends = []
        if dependencies:
            for depend in dependencies:
                if isinstance(depend, Job):
                    depends.append(int(depend.id))
                else:
                    depends.append(int(depend))
        command = 'bash {}'.format(script_file)
        # Make sure the global job pool exists
        if not _local.JQUEUE or not _local.JQUEUE.runner.is_alive():
            _local.JQUEUE = _local.JobQueue(cores=threads)
        return _local.JQUEUE.add(_run.cmd, (command,), dependencies=depends)


def clean_dir(directory='.', suffix=None, qtype=None, confirm=False,
              delete_outputs=None):
    """Delete all files made by this module in directory.

    CAUTION: The clean() function will delete **EVERY** file with
             extensions matching those these::
                 .<suffix>.err
                 .<suffix>.out
                 .<suffix>.out.func.pickle
                 .<suffix>.sbatch & .<suffix>.script for slurm mode
                 .<suffix>.qsub for torque mode
                 .<suffix> for local mode
                 _func.<suffix>.py
                 _func.<suffix>.py.pickle.in
                 _func.<suffix>.py.pickle.out

    Args:
        directory (str):       The directory to run in, defaults to the current
                               directory.
        suffix (str):          Override the default suffix.
        qtype (str):           Only run on files of this qtype
        confirm (bool):        Ask the user before deleting the files
        delete_outputs (bool): Delete all output files too.

    Returns:
        A set of deleted files
    """
    _queue.check_queue(qtype)  # Make sure the queue.MODE is usable

    if delete_outputs is None:
        delete_outputs = _conf.get_option('jobs', 'clean_outputs')

    # Sanitize arguments
    if not directory:
        directory = '.'
    if not suffix:
        suffix = _conf.get_option('jobs', 'suffix')

    # Extension patterns to delete
    extensions = ['_func.' + suffix + '.py']
    if delete_outputs:
        extensions += ['.' + suffix + '.err', '.' + suffix + '.out',
                       '_func.' + suffix + '.py.pickle.in',
                       '_func.' + suffix + '.py.pickle.out',
                       '.' + suffix + '.out.func.pickle']

    if qtype:
        if qtype == 'local':
            extensions.append('.' + suffix)
        elif qtype == 'slurm':
            extensions += ['.' + suffix + '.sbatch', '.' + suffix + '.script']
        elif qtype== 'torque':
            extensions.append('.' + suffix + '.qsub')
    else:
        extensions.append('.' + suffix)
        extensions += ['.' + suffix + '.sbatch', '.' + suffix + '.script']
        extensions.append('.' + suffix + '.qsub')

    files = [i for i in _os.listdir(_os.path.abspath(directory))
             if _os.path.isfile(i)]

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
            _sys.stdout.write('Files to delete::\n\t')
            _sys.stdout.write('\n\t'.join(deleted) + '\n')
            answer = _run.get_input("Do you want to delete these files? [Y/n]",
                                    ['y', 'n'])
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
