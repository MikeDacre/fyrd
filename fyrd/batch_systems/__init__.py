# -*- coding: utf-8 -*-
"""
Modular batch system handling.

All batch system specific functions are contained within files in the
batch_systems folder.  The files must have the same name as the batch system,
and possible batch systems are set in the DEFINED_SYSTEMS set. Most batch
system functions are set in the modules in this package, but system detection
methods are hardcoded into get_cluster_environment() also.

To add new systems, create a new batch system with identical function and
classes names and return/yield values to those in an existing definition. You
will also need to update the options.py script to include keywords for your
system and the get_cluster_environment() function to include autodetection.
"""
from importlib import import_module as _import

from .. import run as _run
from .. import logme as _logme
from .. import ClusterError as _ClusterError

DEFINED_SYSTEMS = {'torque', 'slurm'}

MODE = None

# Define job states all batch systems must return one of these states
GOOD_STATES      = ['complete', 'completed', 'special_exit']
ACTIVE_STATES    = ['configuring', 'completing', 'pending',
                    'running']
BAD_STATES       = ['boot_fail', 'cancelled', 'failed',
                    'node_fail', 'timeout', 'disappeared']
UNCERTAIN_STATES = ['hold', 'preempted', 'stopped',
                    'suspended']
ALL_STATES = GOOD_STATES + ACTIVE_STATES + BAD_STATES + UNCERTAIN_STATES
DONE_STATES = GOOD_STATES + BAD_STATES


def get_batch_system(qtype=None):
    """Return a batch_system module."""
    qtype = qtype if qtype else get_cluster_environment()
    if qtype not in DEFINED_SYSTEMS:
        raise _ClusterError(
            'qtype value {} is not recognized, '.format(qtype) +
            'should be: local, torque, or slurm'
        )
    return _import('fyrd.batch_systems.{}'.format(qtype))


#################################
#  Set the global cluster type  #
#################################


def get_cluster_environment():
    """Detect the local cluster environment and set MODE globally.

    Detect the current batch system by looking for command line utilities.
    Order is important here, so we hard code the batch system lookups.

    Paths to files can also be set in the config file.

    Returns:
        tuple: MODE variable ('torque', 'slurm', or 'local')
    """
    global MODE
    from .. import conf as _conf
    conf_queue = _conf.get_option('queue', 'queue_type', 'auto')
    if conf_queue not in list(DEFINED_SYSTEMS) + ['auto']:
        _logme.log('queue_type in the config file is {}, '.format(conf_queue) +
                   'but it should be one of {}'.format(DEFINED_SYSTEMS) +
                   ' or auto. Resetting it to auto', 'warn')
        _conf.set_option('queue', 'queue_type', 'auto')
        conf_queue = 'auto'
    if conf_queue == 'auto':
        # Hardcode queue lookups here
        sbatch_cmnd = _conf.get_option('queue', 'sbatch')
        qsub_cmnd   = _conf.get_option('queue', 'qsub')
        sbatch_cmnd = sbatch_cmnd if sbatch_cmnd else 'sbatch'
        qsub_cmnd   = qsub_cmnd if qsub_cmnd else 'qsub'
        if _run.which(sbatch_cmnd):
            MODE = 'slurm'
        elif _run.which(qsub_cmnd):
            MODE = 'torque'
        else:
            MODE = None
    else:
        MODE = conf_queue
    if MODE == 'local':
        _logme.log('No cluster environment detected, using multiprocessing',
                   'debug')
    else:
        _logme.log('{} detected, using for cluster submissions'.format(MODE),
                   'debug')
    return MODE


##############################
#  Check if queue is usable  #
##############################


def check_queue(qtype=None):
    """Raise exception if MODE is incorrect."""
    if 'MODE' not in globals():
        global MODE
        MODE = get_cluster_environment()
    if not MODE:
        MODE = get_cluster_environment()
    if not MODE:
        _logme.log('Queue system not detected', 'error')
        return False
    if qtype:
        if qtype not in DEFINED_SYSTEMS:
            raise _ClusterError('qtype value {} is not recognized, '
                                .format(qtype) +
                                'should be one of {}'.format(DEFINED_SYSTEMS))
        else:
            MODE = qtype
            return True
    elif MODE not in DEFINED_SYSTEMS:
        raise _ClusterError('MODE value {} is not recognized, '.format(MODE) +
                            'should be: local, torque, or slurm')

# Make options easily available everywhere
from . import options
