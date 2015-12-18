"""
Description:   Submit a job to sbatch

Created:       2015-12-11
Last modified: 2015-12-11 23:03
"""
from subprocess import check_output as sub
from pyslurm import job
from . import _defaults

# File wide default
_inifaults = _defaults['jobs']


class run():
    """ A structure to build a job for submission to the queue """
    def __init__(commands, name, nodes=_inifaults['nodes'], cores=_inifaults['cores'],
                 mem=_inifaults['mem'], walltime=_inifaults['time'], default_params='small'):
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
    return int(sub(['sbatch'] + args).decode().rstrip().split(' ')[-1])


def create_job(commands, name, nodes=_inifaults['nodes'], cores=_inifaults['cores'],
               mem=_inifaults['mem'], walltime=_inifaults['time'], default_params='small'):
    """ Write a script file for submission """
    pass
