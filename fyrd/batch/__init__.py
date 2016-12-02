"""
Defined all available cluster environements.
"""
from .. import run as _run
from .. import ClusterError as _ClusterError

CLUSTERS = [
    'slurm',
    'torque',
    'local',
]
"""These batch systems are evaluated in order when detecting cluster env."""

###############################################################################
#                         Do Not Edit Below This Line                         #
###############################################################################
CLUSTER_DICT = {}
for cluster in CLUSTERS:
    CLUSTER_DICT[cluster] = exec('from . import {}'.format(cluster))

MODE = ''

###############################################################################
#                 Define the Generic Batch System Class Here                  #
###############################################################################


class Batch(object):

    """Defines a generic batch system."""

    name        = 'Override in child'
    submit_cmnd = 'Override in child'
    queue_cmnd  = 'Override in child'
    arg_prefix  = 'Override in child'

    identifying_scripts = []

    status_dict = {}

    def __init__(self):
        """Sanity check self."""
        self._check_self()

    def queue_parser(self, user=None, partition=None):
        """This method should be an iterator and yield a tuple of 9 items.

        Args:
            user (list):      Filter by user ID
            partition (list): Filter by queue/partition

        Yields:
            job_id (int/str):       An integer or string representation of the
                                    job ID
            array_index (int/None): An array job index, no required but if
                                    present should be an int
            userid (str):           The job owners user ID as a string, not an
                                    ID number
            partition (str):        The partition/queue the job is running in
            state (str):            The job state, must be standardized to one
                                    of the allowed states
            nodes (list/None):      A list of nodes the  job is assigned to,
                                    should be None if job has not yet started
                                    running.
            numnodes (int/None):    An integer count of the number of nodes the
                                    job is assigned, should be None if not yet
                                    running.
            threads (int/None):     The number of threads the job has on each
                                    node.
            exitcode (int/None):    The exitcode of the job on completion
        """
        raise ValueError('This method must be overwridden by child')

    def id_from_stdout(self, stdout):
        """Parse STDOUT from submit command to get job ID.

        This doesn't need to be a method, but it is for ease of use.

        Returns:
            int/str: Job ID
        """
        raise ValueError('This method must be overwridden by child')

    def format_script(self, kwds):
        """Create a submission script for this batch system.

        This doesn't need to be a method, but it is for ease of use.

        Args:
            kwds (dict): Allowable keyword arguments for a fyrd Job

        Returns:
            str: A formatted submission script
        """
        return '#!/bin/bash\n{}\n'.format(_options.options_to_string(kwds))

    def submit_args(self, kwds, dependencies=None):
        """Use kwds and dependencies to create args for submit script.

        This doesn't need to be a method, but it is for ease of use.

        Args:
            kwds (dict):         Allowable keyword arguments for a fyrd Job
            dependencies (list): A list of job IDs to wait for

        Returns:
            str: A string of submission arguments to be appended to the call
                 to the batch system submit script
        """
        return ''

    @property
    def fetch_queue():
        """Get the current output of self.queue_cmnd."""
        return _run.cmd(self.queue_cmnd)[1]

    def _check_self(self):
        """Sanity check self."""
        assert self.suffix      != 'Override in child'
        assert self.submit_cmnd != 'Override in child'
        assert self.queue_cmnd  != 'Override in child'
        assert self.arg_prefix  != 'Override in child'
        assert isinstance(self.identifying_scripts, list)
        assert len(self.identifying_scripts) > 0
        list(self.queue_parser())
        for script in self.identifying_scripts:
            if _run.which(script):
                return True
        return False

    def __getattr__(self, key):
        """Create a default for suffix."""
        if key == 'suffix':
            return name

    def __len__(self):
        """The length is the number of items in the queue."""
        return len(list.self.queue_parser())


###############################################################################
#                    Batch System Detection and Management                    #
###############################################################################


def get_batch(qtype=None):
    """Return the appropriate BatchSystem for qtype (autodetected if None)."""
    qtype = qtype if qtype else get_cluster_environment()
    if qtype not in CLUSTERS:
        raise _ClusterError(
            'qtype value {} is not recognized, '.format(qtype) +
            'should be: local, torque, or slurm'
        )
    batch_system = CLUSTER_DICT[MODE].BatchSystem()
    batch_system._check_self()
    return batch_system


def get_cluster_environment():
    """Detect the local cluster environment and set MODE globally.

    Checks all clusters in the CLUSTERS dictionary in batch/__init__.py in
    order

    Returns:
        str: MODE variable
    """
    global MODE
    conf_queue = conf.get_option('queue', 'queue_type', 'auto')
    if conf_queue not in CLUSTERS + ['auto']:
        logme.log('queue_type in the config file is {}, '.format(conf_queue) +
                  'but it should be one of {} or auto. '.format(CLUSTERS) +
                  'Resetting it to auto', 'warn')
        conf.set_option('queue', 'queue_type', 'auto')
        conf_queue = 'auto'
    if conf_queue == 'auto':
        for system, data in CLUSTER_DICT.items():
            for script in data.BatchSystem.identifying_scripts:
                if run.which(script):
                    MODE = system
        MODE = 'local'
    else:
        MODE = conf_queue
    logme.log('Using {} batch system'.format(MODE), 'debug')
    return MODE
