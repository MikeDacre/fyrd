"""Test remote queues, we can't test local queues in py.test."""
import os
import sys
import pytest
sys.path.append(os.path.abspath('.'))
import fyrd
env = fyrd.batch_systems.get_cluster_environment()


def test_queue_inspection():
    """Make sure that if qsub or sbatch are available, the queue is right."""
    queue_type = fyrd.conf.get_option('queue', 'queue_type')
    if queue_type != 'auto':
        assert fyrd.batch_systems.MODE == queue_type
        cfile = fyrd.conf.CONFIG_FILE
        fyrd.conf.CONFIG_FILE = 'conftest'
        fyrd.conf.create_config()
        fyrd.batch_systems.get_cluster_environment()
    if fyrd.run.which('sbatch'):
        assert fyrd.batch_systems.MODE == 'slurm'
    elif fyrd.run.which('qsub'):
        assert fyrd.batch_systems.MODE == 'torque'
    else:
        assert fyrd.batch_systems.MODE == 'local'
    if queue_type != 'auto':
        fyrd.conf.CONFIG_FILE = cfile
        os.remove('conftest')
        fyrd.batch_systems.get_cluster_environment()
    assert env == fyrd.batch_systems.MODE



@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_queue_creation():
    """Test Queue object creation."""
    assert env == 'torque' or env == 'slurm' or env == 'local'
    fyrd.check_queue()
    queue = fyrd.Queue()
    assert queue.qtype == env
    len(queue)
