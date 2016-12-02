"""Test remote queues, we can't test local queues in py.test."""
import os
import sys
import argparse
from datetime import datetime as dt
from datetime import timedelta as td
sys.path.append(os.path.abspath('.'))
import fyrd

fyrd.logme.MIN_LEVEL = 'info'


###############################################################################
#                              Support Functions                              #
###############################################################################


def write_to_file(string, file):
    """Write a string to a file."""
    with open(file, 'w') as fout:
        fout.write(string + '\n')
    return 0


def raise_me(number, power=2):
    """Raise number to power."""
    return number**power


def dosomething(x):
    """Simple file operation."""
    out = []
    with fyrd.run.open_zipped(x) as fin:
        for line in fin:
            out.append((line, line.split('\t')[1]*2))
    return out


def dosomethingbad(x):
    """Try to operate on a file, but do it stupidly."""
    out = []
    with open(x) as j:
        out.append(j, j.split('\t')[1]*2)



###############################################################################
#                              Class For Testing                              #
###############################################################################


class TestMe(object):

    """This class is just used to test method submission."""

    def __init__(self):
        """Initialize self."""
        self.me  = 24
        self.out = None

    def do_math(self, number):
        """Multiply self.me by number."""
        self.out = self.me*number

    def get_out(self):
        """Return out."""
        return self.out


###############################################################################
#                               Test Functions                                #
###############################################################################


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
    job.submission.clean()
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
    job.wait()
    job.fetch_outputs()
    out = job.get(delete_outfiles=False)
    job.function.clean(delete_output=True)
    job.clean()
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


def test_method_submission():
    """Submit a method."""
    t = TestMe()
    job = fyrd.Job(t.do_math, (2,))
    t2 = job.get()
    assert t2.get_out() == t.me*2


def test_function_keywords():
    """Submit a simple function with keyword arguments."""
    job = fyrd.Job(raise_me, (10,), kwargs={'power': 10}).submit()
    assert job.get() == 10**10
    job.clean(delete_outputs=True)
    return 0


def test_splitfile():
    """Use the splitfile helper function."""
    out = fyrd.helpers.splitrun(2, 'tests/test.txt.gz',
                                False, dosomething, ('{file}',))
    assert sorted(out) == sorted(dosomething('tests/test.txt.gz'))
    return 0


def test_splitfile_script():
    """Test splitfile() with a script and outfile."""
    out = fyrd.helpers.splitrun(2, 'tests/test.txt.gz',
                                False, dosomething, ('{file}',))
    assert out == dosomething('tests/test.txt.gz')
    return 0


def test_splitfile_indirect():
    """Use the splitfile helper function."""
    job = fyrd.helpers.splitrun(2, 'tests/test.txt.gz',
                                False, dosomething, ('{file}',), direct=False)
    out = job.get()
    assert sorted(out) == sorted(dosomething('tests/test.txt.gz'))
    return 0


def test_splitfile_bad():
    """Use the splitfile helper function and fail."""
    if not fyrd.logme.MIN_LEVEL == 'debug':
        old_level = fyrd.logme.MIN_LEVEL
        fyrd.logme.MIN_LEVEL = 'critical'
    try:
        fyrd.helpers.splitrun(2, 'tests/test.txt.gz',
                              False, dosomethingbad, ('{file}',))
    except AttributeError:
        fyrd.basic.clean_dir('.', delete_outputs=True, confirm=False)
        try:
            os.remove('test.txt.gz.split_0001.gz')
        except OSError:
            pass
        try:
            os.remove('test.txt.gz.split_0002.gz')
        except OSError:
            pass
        return 0
    finally:
        if not fyrd.logme.MIN_LEVEL == 'debug':
            fyrd.logme.MIN_LEVEL = old_level


def test_dir_clean():
    """Clean all job files in this dir."""
    fyrd.basic.clean_dir(delete_outputs=True)
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
    print('Cleaning')
    count += test_job_cleaning()
    print('Function submission')
    count += test_function_submission()
    print('Function keywords')
    count += test_function_keywords()
    print('Dir clean')
    count += test_dir_clean()
    # These tests frequently stall, I don't know why.
    #  print('Splitfile')
    #  count += test_splitfile()
    #  print('Splitfile Script')
    #  count += test_splitfile_script()
    #  print('Splitfile bad')
    #  count += test_splitfile_bad()
    if count > 0:
        sys.stderr.write('Some tests failed')
        return count
    sys.stdout.write('Tests complete\n')

if __name__ == '__main__' and '__file__' in globals():
    sys.exit(main())
