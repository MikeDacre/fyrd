"""Test remote queues, we can't test local queues in py.test."""
import os
import sys
sys.path.append(os.path.abspath('.'))
import cluster

def write_to_file(string, file):
    """Write a string to a file."""
    with open(file, 'w') as fout:
        fout.write(string + '\n')


def test_job_creation():
    """Make a job and print it."""
    cluster.queue.MODE = 'local'
    job = cluster.Job('echo hi', cores=2, time='00:02:00', mem='2000',
                      threads=4)
    assert job.qtype == 'local'
    return job


def test_job_execution():
    """Run a job"""
    cluster.queue.MODE = 'local'
    job = test_job_creation()
    job.submit()
    code, stdout, stderr = job.get()
    assert code == 0
    assert stdout == 'hi\n'
    assert stderr == ''
    return job


def test_job_cleaning():
    """Delete intermediate files."""
    cluster.queue.MODE = 'local'
    job = test_job_execution()
    job.clean()
    assert 'echo.cluster' not in os.listdir('.')


def test_function_submission():
    """Submit a function."""
    cluster.queue.MODE = 'local'
    job = cluster.Job(write_to_file, ('42', 'bobfile'))
    job.submit()
    code, stdout, stderr = job.get()
    assert code == 0
    assert stdout == '\n'
    assert stderr == ''
    with open('bobfile') as fin:
        assert fin.read().rstrip() == '42'
    os.remove('bobfile')
    job.clean()


def test_dir_clean():
    """Clean all job files in this dir."""
    cluster.job.clean_dir()

if __name__ == "__main__":
    test_job_creation()
    test_job_execution()
    test_job_cleaning()
    test_function_submission()
    test_dir_clean()
    sys.stdout.write('Tests complete\n')
