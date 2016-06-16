"""Test remote queues, we can't test local queues in py.test."""
import os
import sys
sys.path.append(os.path.abspath('.'))
import cluster
env = cluster.get_cluster_environment()


def test_queue_inspection():
    """Make sure that if qsub or sbatch are available, the queue is right."""
    if cluster.run.which('sbatch'):
        assert cluster.queue.MODE == 'slurm'
    elif cluster.run.which('qsub'):
        assert cluster.queue.MODE == 'torque'
    else:
        assert cluster.queue.MODE == 'local'
    assert env == cluster.queue.MODE


def test_queue_creation():
    """Test Queue object creation."""
    assert env == 'torque' or env == 'slurm' or env == 'local'
    cluster.check_queue()
    queue = cluster.Queue()
    assert queue.qtype == env
    len(queue)
