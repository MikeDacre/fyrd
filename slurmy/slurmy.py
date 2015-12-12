"""
Description:   Submit a job to sbatch

Created:       2015-12-11
Last modified: 2015-12-11 22:24
"""
from subprocess import check_output as sub
from pyslurm import job

# Set defaults from config # Maybe use a config file?
(default_nodes,
 default_cores,
 default_mem,
 default_walltime) = (1, 1, '4GB', '12:00:00')


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


def create_job(commands, name, nodes=default_nodes, cores=default_cores,
               mem=default_mem, walltime=default_walltime):
    """ Write a script file for submission """
    pass
