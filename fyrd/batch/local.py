"""
Fallback local multiprocessing mode.
"""
from . import local as _local

###############################################################################
#                                    Queue                                    #
###############################################################################


def queue_parser(user=None, partition=None):
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


def format_script(kwds):
    # Create the pool
    if not _local.JQUEUE or not _local.JQUEUE.runner.is_alive():
        threads = kwds['threads'] if 'threads' in kwds \
                else _local.THREADS
        _local.JQUEUE = _local.JobQueue(cores=threads)
    script  = '#!/bin/bash\n'
    script += options.options_to_string(kwds)
    return script
