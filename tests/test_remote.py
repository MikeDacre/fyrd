"""Test remote queues, we can't test local queues in py.test."""
import os
import sys
import pytest
from datetime import datetime as dt
from datetime import timedelta as td
sys.path.append(os.path.abspath('.'))
import fyrd
env = fyrd.get_cluster_environment()

fyrd.logme.MIN_LEVEL = 'debug'

def write_to_file(string, file):
    """Write a string to a file."""
    with open(file, 'w') as fout:
        fout.write(string + '\n')
    return 0


def test_job_creation():
    """Make a job and print it."""
    job = fyrd.Job('echo hi', cores=2, time='00:02:00', mem='2000',
                   threads=4, clean_files=False, clean_outputs=False)
    assert job.qtype == env


@pytest.mark.skipif(env == 'local',
                    reason="Fails in local mode")
def test_job_execution():
    """Run a job and autoclean."""
    job = fyrd.Job('echo hi', profile='default', clean_files=True,
                   clean_outputs=True).submit()
    job.wait()
    assert os.path.isfile(job.outfile)
    assert os.path.isfile(job.errfile)
    assert os.path.isfile(job.submission.file_name)
    out = job.get()
    assert not os.path.isfile(job.outfile)
    assert not os.path.isfile(job.errfile)
    assert not os.path.isfile(job.submission.file_name)
    sys.stdout.write('{};\nSTDOUT: {}\nSTDERR: {}\n'
                     .format(job.exitcode, job.stdout, job.stderr))
    assert job.exitcode == 0
    assert out == 'hi\n'
    assert job.stdout == 'hi\n'
    assert job.stderr == ''
    assert isinstance(job.start, dt)
    assert isinstance(job.end, dt)
    assert isinstance(job.runtime, td)


@pytest.mark.skipif(env == 'local',
                    reason="Fails in local mode")
def test_job_cleaning():
    """Delete intermediate files without autoclean."""
    job = fyrd.Job('echo hi', profile='default', clean_files=False,
                   clean_outputs=False).submit()
    job.wait()
    assert os.path.isfile(job.outfile)
    assert os.path.isfile(job.errfile)
    assert os.path.isfile(job.submission.file_name)
    job.clean(delete_outputs=True)
    assert not os.path.isfile(job.outfile)
    assert not os.path.isfile(job.errfile)
    assert not os.path.isfile(job.submission.file_name)


@pytest.mark.skipif(env == 'local',
                    reason="Fails in local mode")
def test_function_submission():
    """Submit a function."""
    job = fyrd.Job(write_to_file, ('42', 'bobfile'))
    job.submit()
    out = job.get()
    sys.stdout.write('{};\nOut: {}\nSTDOUT: {}\nSTDERR: {}\n'
                     .format(job.exitcode, out, job.stdout, job.stderr))
    assert job.exitcode == 0
    assert out == 0
    assert job.out == 0
    assert job.stdout == '\n'
    assert job.stderr == ''
    with open('bobfile') as fin:
        assert fin.read().rstrip() == '42'
    os.remove('bobfile')
    job.clean(delete_outputs=True)


def test_dir_clean():
    """Clean all job files in this dir."""
    fyrd.basic.clean_dir(delete_outputs=True)
