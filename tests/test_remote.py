"""Test remote queues, we can't test local queues in py.test."""
import os
import sys
import pytest
sys.path.append(os.path.abspath('.'))
import cluster
env = cluster.get_cluster_environment()

def write_to_file(string, file):
    """Write a string to a file."""
    with open(file, 'w') as fout:
        fout.write(string + '\n')


def test_job_creation():
    """Make a job and print it."""
    job = cluster.Job('echo hi', cores=2, time='00:02:00', mem='2000',
                      threads=4)
    assert job.qtype == env
    return job


@pytest.mark.skipif(env == 'local',
                    reason="Fails in local mode")
def test_job_execution():
    """Run a job"""
    job = test_job_creation()
    job.submit()
    code, stdout, stderr = job.get()
    sys.stdout.write('{};\nSTDOUT: {}\nSTDERR: {}\n'
                     .format(code, stdout, stderr))
    assert code == 0
    assert stdout == 'hi\n'
    assert stderr == ''
    return job


@pytest.mark.skipif(env == 'local',
                    reason="Fails in local mode")
def test_job_cleaning():
    """Delete intermediate files."""
    job = test_job_execution()
    job.clean()
    assert 'echo.cluster' not in os.listdir('.')


@pytest.mark.skipif(env == 'local',
                    reason="Fails in local mode")
def test_function_submission():
    """Submit a function."""
    job = cluster.Job(write_to_file, ('42', 'bobfile'))
    job.submit()
    code, stdout, stderr = job.get()
    sys.stdout.write('{};\nSTDOUT: {}\nSTDERR: {}\n'
                     .format(code, stdout, stderr))
    assert code == 0
    assert stdout == '\n'
    #  assert stderr == ''
    with open('bobfile') as fin:
        assert fin.read().rstrip() == '42'
    os.remove('bobfile')
    job.clean()


def test_dir_clean():
    """Clean all job files in this dir."""
    cluster.job.clean_dir()
