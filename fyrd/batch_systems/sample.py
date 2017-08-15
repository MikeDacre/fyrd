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
    fyrd.script_runners.Script, or None if unneeded
        As execution script that will be called by the submission script,
        optional
    """
    pass

def submit(file_name, dependencies=None, job=None, args=None, kwds=None):
    """Submit any file with dependencies.

    If your batch system does not handle dependencies, then raise a
    NotImplemented error if dependencies are passed.

    Parameters
    ----------
    file_name : str
        Path to an existing file
    dependencies : list
        List of dependencies
    job : fyrd.job.Job, not implemented
        A job object for the calling job, not used by this functions
    args : list, not implemented
        A list of additional command line arguments to pass when submitting,
        not used by this function
    kwds : dict or str, not implemented
        A dictionary of keyword arguments to parse with options_to_string, or
        a string of option:value,option,option:value,.... Not used by this
        function.

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
    None
        Would contain additional arguments to pass to sbatch, but these are not
        needed so we just return None
    """
    pass
