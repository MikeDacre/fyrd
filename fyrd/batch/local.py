"""
Fallback local multiprocessing mode.
"""


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
