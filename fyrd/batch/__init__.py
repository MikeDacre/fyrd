"""
Defined all available cluster environements.
"""

CLUSTERS = [
    'slurm',
    'torque',
    'local',
]
"""These batch systems are evaluated in order when detecting cluster env."""

for cluster in CLUSTERS:
    exec('from . import {}'.format(cluster))


###############################################################################
#                 Define the Generic Batch System Class Here                  #
###############################################################################


class BatchSystem(object):

    """Defines a generic batch system."""

    name        = 'Override in child'
    submit_cmnd = 'Override in child'
    arg_prefix  = 'Override in child'
    identifying_scripts = []

    def __init__(self):
        """Sanity check self."""
        assert self.suffix      != 'Override in child'
        assert self.submit_cmnd != 'Override in child'
        assert self.arg_prefix  != 'Override in child'
        assert isinstance(self.identifying_scripts, list)
        assert len(self.identifying_scripts) > 0
        queue_parser()

    def queue_parser(self, user=None, partition=None):
        raise ValueError('This method must be overwridden by child')

    def id_from_stdout(self, stdout):
        """Parse STDOUT from submit command to get job ID.

        Returns:
            int/str: Job ID
        """
        raise ValueError('This method must be overwridden by child')

    def format_script(self, kwds):
        pass

    def submit_args(self, kwds, dependencies=None):
        """Use kwds and dependencies to create an argument string for submit script."""
        return ''

    def __getattr__(self, key):
        """Create a default for suffix."""
        if key == 'suffix':
            return name


###############################################################################
#                    Batch System Detection and Management                    #
###############################################################################


def get_batch(qtype=None):
    """Return the appropriate BatchSystem for qtype (autodetected if None)."""
    return BatchSystem()
