# -*- coding: utf-8 -*-
"""
Sample parsing functions, adapt these to make your own fyrd queue parser.
"""
import os as _os
import re as _re
import sys as _sys
import pwd as _pwd     # Used to get usernames for queue
from subprocess import CalledProcessError as _CalledProcessError

from .. import run as _run
from .. import logme as _logme
from .. import ClusterError as _ClusterError
from .. import script_runners as _scrpts
from .. import submission_scripts as _sscrpt
_Script = _sscrpt.Script


# This will be prepended to parsed text in script files
# e.g. #SBATCH for slurm
PREFIX = ''
# This will be appended to job submission scripts, e.g. '.qsub' for torque or
# '.sbatch' for slurm
SUFFIX = '.sh'


###############################################################################
#                           Normalization Functions                           #
###############################################################################


def normalize_job_id(job_id):
    """Convert the job id into job_id, array_id."""
    pass


def normalize_state(state):
    """Convert state into standadized (slurm style) state."""
    return state


###############################################################################
#                               Job Submission                                #
###############################################################################


def gen_scripts(job_object, command, args, precmd, modstr):
    """Build the submission script objects.

    This script should almost certainly work by formatting `_scrpts.CMND_RUNNER_TRACK`.
    The result should be a script that can be executed on a node by the batch system.
    The format of the output is important, which is why `_scrpts.CMND_RUNNER_TRACK`
    should be used; if it is not used, then be sure to copy the format of the outfile
    in that script.

    Parameters
    ---------
    job_object : fyrd.job.Job
    command : str
        Command to execute
    args : list
        List of additional arguments
    precmd : str
        String from options_to_string() to add at the top of the file, should
        contain batch system directives
    modstr : str
        String to add after precmd, should contain module directives.

    Returns
    -------
    fyrd.script_runners.Script
        The submission script
    fyrd.script_runners.Script, or None if unneeded
        As execution script that will be called by the submission script,
        optional
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

    If your batch system does not handle dependencies, then raise a
    NotImplemented error if dependencies are passed.

    Parameters
    ----------
    file_name : str
        Path to an existing file
    dependencies : list, optional
        List of dependencies
    job : fyrd.job.Job, optional, not required
        A job object for the calling job
    args : list, optional, not required
        A list of additional command line arguments to pass when submitting
    kwds : dict or str, optional, not required
        A dictionary of keyword arguments to parse with options_to_string, or
        a string of option:value,option,option:value,....

    Returns
    -------
    job_id : str
    """
    pass


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
    pass


###############################################################################
#                                Queue Parsing                                #
###############################################################################


def queue_parser(user=None, partition=None):
    """Iterator for queue parsing.

    Parameters
    ----------
    user : str, optional
        User name to pass to qstat to filter queue with
    partiton : str, optional
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
    pass


def parse_strange_options(option_dict):
    """Parse all options that cannot be handled by the regular function.

    Parameters
    ----------
    option_dict : dict
        All keyword arguments passed by the user that are not already defined
        in the Job object

    Returns
    -------
    list
        A list of strings to be added at the top of the script file
    dict
        Altered version of option_dict with all options that can't be handled
        by `fyrd.batch_systems.options.option_to_string()` removed.
    list
        A list of command line arguments to pass straight to the submit
        function
    """
    pass
