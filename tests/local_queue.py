"""Test remote queues, we can't test local queues in py.test."""
import os
import sys
sys.path.append(os.path.abspath('.'))
import fyrd


def write_to_file(string, file):
    """Write a string to a file."""
    with open(file, 'w') as fout:
        fout.write(string + '\n')
    return 0


def test_job_creation():
    """Make a job and print it."""
    fyrd.queue.MODE = 'local'
    job = fyrd.Job('echo hi', cores=2, time='00:02:00', mem='2000',
                      threads=4)
    assert job.qtype == 'local'
    return job


def test_job_execution(autoclean=True):
    """Run a job"""
    fyrd.queue.MODE = 'local'
    job = test_job_creation()
    job.submit()
    out = job.get(autoclean)
    assert job.exitcode == 0
    assert out == 'hi\n'
    assert job.stderr == ''
    return job


def test_job_cleaning():
    """Delete intermediate files."""
    fyrd.queue.MODE = 'local'
    job = test_job_execution(False)
    job.clean()
    assert 'echo.cluster' not in os.listdir('.')
    return 0


def test_function_submission():
    """Submit a function."""
    failed = False
    fyrd.queue.MODE = 'local'
    job = fyrd.Job(write_to_file, ('42', 'bobfile'))
    job.submit()
    out = job.get()
    assert job.exitcode == 0
    assert out == 0
    assert job.out == 0
    assert job.stdout == '\n'
    if job.stderr != '':
        sys.stderr.write('STDERR should be empty, but contains:\n')
        sys.stderr.write(stderr)
        failed = True
    with open('bobfile') as fin:
        assert fin.read().rstrip() == '42'
    os.remove('bobfile')
    job.clean()
    if failed:
        return 1
    return 0


def test_dir_clean():
    """Clean all job files in this dir."""
    fyrd.job.clean_dir()
    return 0


if __name__ == "__main__":
    count = 0
    test_job_creation()
    test_job_execution()
    count += test_job_cleaning()
    count += test_function_submission()
    count += test_dir_clean()
    if count > 0:
        sys.stderr.write('Some tests failed')
        sys.exit(1)
    sys.stdout.write('Tests complete\n')
