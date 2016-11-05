"""Test remote queues, we can't test local queues in py.test."""
import os
import sys
import argparse
from datetime import datetime as dt
from datetime import timedelta as td
sys.path.append(os.path.abspath('.'))
import fyrd

fyrd.logme.MIN_LEVEL = 'info'

def write_to_file(string, file):
    """Write a string to a file."""
    with open(file, 'w') as fout:
        fout.write(string + '\n')
    return 0


def test_job_creation():
    """Make a job and print it."""
    fyrd.queue.MODE = 'local'
    job = fyrd.Job('echo hi', cores=2, time='00:02:00', mem='2000',
                   threads=4, clean_files=False, clean_outputs=False)
    assert job.qtype == 'local'
    return 0


def test_job_execution():
    """Run a job and autoclean."""
    fyrd.queue.MODE = 'local'
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
    assert job.exitcode == 0
    assert out == 'hi\n'
    assert job.stdout == 'hi\n'
    assert job.stderr == ''
    assert isinstance(job.start, dt)
    assert isinstance(job.end, dt)
    assert isinstance(job.runtime, td)
    return 0


def test_job_cleaning():
    """Delete intermediate files without autoclean."""
    fyrd.queue.MODE = 'local'
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
        sys.stderr.write(job.stderr)
        failed = True
    with open('bobfile') as fin:
        assert fin.read().rstrip() == '42'
    os.remove('bobfile')
    job.clean(delete_outputs=True)
    if failed:
        return 1
    return 0


def test_dir_clean():
    """Clean all job files in this dir."""
    fyrd.job.clean_dir(delete_outputs=True)
    return 0


def main(argv=None):
    """Get arguments and run tests."""
    if not argv:
        argv = sys.argv[1:]

    parser  = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-v', '--verbose', action="store_true",
                        help="Verbose")

    args = parser.parse_args(argv)

    if args.verbose:
        fyrd.logme.MIN_LEVEL = 'debug'

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

if __name__ == '__main__' and '__file__' in globals():
    sys.exit(main())
