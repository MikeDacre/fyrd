"""Test remote queues, we can't test local queues in py.test."""
import os
import sys
import pytest
sys.path.append(os.path.abspath('.'))
import cluster
env = cluster.get_cluster_environment()


@pytest.mark.skipif(env == 'local',
                    reason="Implemented elsewhere")
def test_job_creation():
    """Make a job and print it."""
    env = 'local'
    cluster.queue.MODE = 'local'
    job = cluster.Job('echo hi', cores=2, time='00:02:00', mem='2000',
                      threads=4, qtype='local')
    assert job.qtype == 'local'
    env = cluster.get_cluster_environment()
    cluster.queue.MODE = env
