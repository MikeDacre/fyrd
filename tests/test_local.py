"""Test remote queues, we can't test local queues in py.test."""
import os
import sys
import pytest
sys.path.append(os.path.abspath('.'))
import fyrd
env = fyrd.get_cluster_environment()


@pytest.mark.skipif(env == 'local',
                    reason="Implemented elsewhere")
def test_job_creation():
    """Make a job and print it."""
    env = 'local'
    fyrd.queue.MODE = 'local'
    job = fyrd.Job('echo hi', cores=2, time='00:02:00', mem='2000',
                      threads=4, qtype='local')
    assert job.qtype == 'local'
    env = fyrd.get_cluster_environment()
    fyrd.queue.MODE = env
