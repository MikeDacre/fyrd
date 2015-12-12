"""
Submit a job to sbatch
"""
from subprocess import check_output as sub
from pyslurm import job

# Set defaults from config # Maybe use a config file?
(default_nodes,
 default_cores,
 default_mem,
 default_walltime) = (1, 1, '4GB', '12:00:00')


def submit_job(script_file, dependency=None):
    """ Submit a job with sbatch and return a job number (int)
        If dependency is provided, then '--dependency=afterok:'
        is added to the submission string """
    args = [script_file, '--dependency=afterok:' + str(dependency)] if dependency else [script_file]
    return sub(args).decode().rstrip().split(' ')[-1]


def create_job(commands, name, nodes=default_nodes, cores=default_cores,
               mem=default_mem, walltime=default_walltime):
    """ Write a script file for submission """
    pass
