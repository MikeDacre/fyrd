"""
Description:   Submit a job to sbatch

Created:       2015-12-11
Last modified: 2015-12-18 16:24
"""
from subprocess import check_output as _sub

# Our imports
from pyslurm import job
from . import defaults

# Get job options
job_defaults = defaults['jobs']

# Funtions to import if requested
__all__ = ['submit_file', 'run']


class run(object):
    """ A structure to build a job for submission to the queue """
    def __init__(commands, default_params='small', name='', nodes='', cores='',
                 mem='', walltime='', ):
        pass


def submit_file(script_file, dependency=None):
    """ Submit a job with sbatch and return a job number (int)
        If dependency is provided, then '--dependency=afterok:'
        is added to the submission string
        script_file is the path to a valid sbatch file
        dependency is either an integer or a list of integers of
        existing jobs already in the queue """
    dependency = ':'.join([str(d) for d in dependency]) \
        if type(dependency) == list else dependency
    args = ['--dependency=afterok:' + str(dependency), script_file] \
        if dependency else [script_file]
    return int(_sub(['sbatch'] + args).decode().rstrip().split(' ')[-1])
