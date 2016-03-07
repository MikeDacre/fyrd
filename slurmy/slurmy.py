"""
Description:   Submit a job to sbatch

Created:       2015-12-11
Last modified: 2016-03-03 15:51
"""
import os
from subprocess import check_output as _sub

# Our imports
from pyslurm import job
from . import defaults

# Get job options
job_defaults = defaults['jobs']

# Funtions to import if requested
__all__ = ['submit_file', 'run', 'make_job_file']


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


def make_job_file(command, name, time, cores, mem=None, partition='normal',
                  modules=[]):
    """Make a job file compatible with sbatch to run command.

    Note: Only requests one node, with 'cores' cores.

    :command:   The command to execute.
    :name:      The name of the job.
    :time:      The time to run for in D-HH:MM:SS.
    :cores:     How many cores to run on.
    :mem:       Memory to use in MB.
    :partition: Partition to run on, default 'normal'.
    :modules:   Modules to load with the 'module load' command.
    :returns:   The absolute path of the submission script.
    """
    modules = [modules] if isinstance(modules, str) else modules
    curdir = os.path.abspath('.')
    scrpt = os.path.join(curdir, '{}.sbatch'.format(name))
    with open(scrpt, 'w') as outfile:
        outfile.write('#!/bin/bash\n')
        outfile.write('#SBATCH -p {}\n'.format(partition))
        outfile.write('#SBATCH --ntasks 1\n')
        outfile.write('#SBATCH --cpus-per-task {}\n'.format(cores))
        outfile.write('#SBATCH --time={}\n'.format(time))
        if mem:
            outfile.write('#SBATCH --mem={}\n'.format(mem))
        outfile.write('#SBATCH -o {}.o.%j\n'.format(name))
        outfile.write('#SBATCH -e {}.e.%j\n'.format(name))
        outfile.write('cd {}\n'.format(curdir))
        outfile.write('srun bash {}.script\n'.format(
            os.path.join(curdir, name)))
    with open(name + '.script', 'w') as outfile:
        outfile.write('#!/bin/bash\n')
        for module in modules:
            outfile.write('module load {}\n'.format(module))
        outfile.write('echo "SLURM_JOBID="$SLURM_JOBID\n')
        outfile.write('echo "SLURM_JOB_NODELIST"=$SLURM_JOB_NODELIST\n')
        outfile.write('echo "SLURM_NNODES"=$SLURM_NNODES\n')
        outfile.write('echo "SLURMTMPDIR="$SLURMTMPDIR\n')
        outfile.write('echo "working directory = "$SLURM_SUBMIT_DIR\n')
        outfile.write('cd {}\n'.format(curdir))
        outfile.write('mkdir -p $LOCAL_SCRATCH\n')
        outfile.write('echo "Running {}"\n'.format(name))
        outfile.write(command + '\n')
    return scrpt


