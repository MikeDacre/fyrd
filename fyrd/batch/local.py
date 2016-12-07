"""
Fallback local multiprocessing mode.
"""
from getpass import getuser as _getuser
from socket import gethostname as _gethost
from .. import local as _local
from .. import run as _run

###############################################################################
#                                    Queue                                    #
###############################################################################


class BatchSystem(_Batch):

    """Methods needed for the torque batch system."""

    name        = 'torque'
    submit_cmnd = 'sbatch'
    arg_prefix  = '#SBATCH'
    queue_cmnd  = ('squeue -h -O'
                   'jobid:400,arraytaskid:400,name:400,userid:400,'
                   'partition:400,state:400,nodelist:400,numnodes:400,'
                   'numcpus:400,exit_code:400')

    identifying_scripts = ['sbatch', 'squeue']

    def queue_parser(self, user=None, partition=None):
        """Iterator for slurm queues.

        Use the `squeue -O` command to get standard data across implementation,
        supplement this data with the results of `sacct`. sacct returns data only
        for the current user but retains a much longer job history. Only jobs not
        returned by squeue are added with sacct, and they are added to *the end* of
        the returned queue, i.e. *out of order with respect to the actual queue*.

        Args:
            user:      optional user name to filter queue with
            partition: optional partition to filter queue with

        Yields:
            tuple: job_id, name, userid, partition, state, nodelist, numnodes,
                ntpernode, exit_code
        """
        self._start_queue()
        for job_id, job_info in _local.JQUEUE:
            job_name   = job_info.function.__name__
            job_owner  = _getuser()
            job_nodes  = _gethost()
            if job_info.state == 'Not Submitted':
                job_state = 'pending'
            elif job_info.state == 'waiting' \
                    or job_info.state == 'submitted':
                job_state = 'pending'
            elif job_info.state == 'started' \
                    or job_info.state == 'running':
                job_state = 'running'
            elif job_info.state == 'done':
                job.state = 'completed'
                job_exitcode = int(job_info.exitcode)
            else:
                raise Exception('Unrecognized state')


    def format_script(self, kwds):
        """Create a submission script for qsub.

        Args:
            kwds (dict): Allowable keyword arguments for a fyrd Job

        Returns:
            str: A formatted submission script
        """
        self._start_queue()
        script  = '#!/bin/bash\n'
        script += options.options_to_string(kwds)
        return script

    def _start_queue(self):
        """Ensure the the local batch system is running."""
        if not _local.JQUEUE or not _local.JQUEUE.runner.is_alive():
            threads = kwds['threads'] if 'threads' in kwds \
                    else _local.THREADS
            _local.JQUEUE = _local.JobQueue(cores=threads)
