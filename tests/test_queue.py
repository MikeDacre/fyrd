"""Test remote queues, we can't test local queues in py.test."""
import os
import sys
import pytest
sys.path.append(os.path.abspath('.'))
import fyrd
env = fyrd.queue.get_cluster_environment()


def test_queue_inspection():
    """Make sure that if qsub or sbatch are available, the queue is right."""
    queue_type = fyrd.conf.get_option('queue', 'queue_type')
    if queue_type != 'auto':
        assert fyrd.queue.MODE == queue_type
        cfile = fyrd.conf.CONFIG_FILE
        fyrd.conf.CONFIG_FILE = 'conftest'
        fyrd.conf.create_config()
        fyrd.queue.get_cluster_environment()
    if fyrd.run.which('sbatch'):
        assert fyrd.queue.MODE == 'slurm'
    elif fyrd.run.which('qsub'):
        assert fyrd.queue.MODE == 'torque'
    else:
        assert fyrd.queue.MODE == 'local'
    if queue_type != 'auto':
        fyrd.conf.CONFIG_FILE = cfile
        os.remove('conftest')
        fyrd.queue.get_cluster_environment()
    assert env == fyrd.queue.MODE


def test_queue_creation():
    """Test Queue object creation."""
    assert env == 'torque' or env == 'slurm' or env == 'local'
    fyrd.check_queue()
    queue = fyrd.Queue()
    assert queue.qtype == env
    len(queue)


def test_queue_parsers():
    """Test the queue parsers."""
    with pytest.raises(fyrd.ClusterError):
        fyrd.queue.queue_parser('local')
