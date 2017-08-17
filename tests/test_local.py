"""Test remote queues, we can't test local queues in py.test."""
import os
import sys
from datetime import datetime as dt
from datetime import timedelta as td
import pytest

sys.path.append(os.path.abspath('.'))
import fyrd
env = 'local'
fyrd.batch_systems.MODE = 'local'

fyrd.logme.MIN_LEVEL = 'debug'


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


@fyrd.jobify(name='test', mem='1000MB', time='00:20:00', submit=True,
             qtype='local')
def raise_me_deco(number, power=2):
    """Raise number to power."""
    return number**power


@fyrd.jobify(name='test', mem='1000MB', time='00:20:00', submit=False,
             qtype='local')
def raise_me_deco2(number, power=2):
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


class MethodSubmission(object):

    """This class is just used to test method submission."""

    def __init__(self):
        """Initialize self."""
        self.me  = 24
        self.out = None

    def do_math(self, number):
        """Multiply self.me by number."""
        self.out = self.me*number
        return self.out


###############################################################################
#                             Script for testing                              #
###############################################################################


SCRIPT = r"""zcat {file} | sed 's/\t/  ' > {outfile}"""


###############################################################################
#                               Test Functions                                #
###############################################################################


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_job_creation():
    """Make a job and print it."""
    fyrd.batch_systems.MODE = 'local'
    job = fyrd.Job('echo hi', cores=2, time='00:02:00', mem='2000')
    assert job.qtype == env


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_job_execution():
    """Run a job and autoclean."""
    job = fyrd.Job('echo hi', profile='default', clean_files=True,
                   clean_outputs=True, qtype='local').submit()
    job.wait()
    print(repr(job))
    print(str(job))
    print(repr(job.submission))
    print(str(job.submission))
    print(job.outfile)
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


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_multi_job_get():
    """Run a job and autoclean."""
    job = fyrd.Job('echo hi', profile='default', clean_files=True,
                   clean_outputs=True, qtype='local').submit()
    job2 = fyrd.Job('echo ho', profile='default', clean_files=True,
                    clean_outputs=True, qtype='local').submit()
    outs = fyrd.basic.get([job, job2])
    assert outs[0] == 'hi\n'
    assert outs[1] == 'ho\n'
    assert job.stdout == 'hi\n'
    assert job2.stdout == 'ho\n'
    assert not os.path.isfile(job.outfile)
    assert not os.path.isfile(job2.outfile)


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_basic_job():
    """Run a job using the basic.py submit function."""
    fyrd.basic.make_job('echo hi', profile='default', qtype='local')
    job = fyrd.submit('echo hi', profile='default', qtype='local')
    job.wait()
    print(repr(job))
    print(str(job))
    print(repr(job.submission))
    print(str(job.submission))
    print(job.outfile)
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


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_make_job_file():
    """Use the basic function to make a job file."""
    job_file = fyrd.basic.make_job_file('echo hi', qtype='local')
    assert os.path.isfile(job_file)
    os.remove(job_file)


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_job_execution_paths():
    """Run a job and autoclean with defined paths."""
    os.makedirs('out')
    job = fyrd.Job('echo hi', profile='default', clean_files=True, qtype='local',
                   clean_outputs=True, scriptpath='..', outpath='.').submit()
    job.wait()
    print(repr(job))
    print(str(job))
    print(repr(job.submission))
    print(str(job.submission))
    print(job.outfile)
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
    os.removedirs('out')
    os.system('rm -rf {}'.format('out'))


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_job_params():
    """Run a job with some explicit parameters set."""
    job = fyrd.Job('echo ho', profile='default', clean_files=True, qtype='local',
                   clean_outputs=True, cores=2, mem=2000, time='00:02:00')
    job.submit()
    out = job.get()
    assert out == 'ho\n'
    assert job.stdout == 'ho\n'
    assert job.stderr == ''


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_outfiles():
    """Run a job with outfile and errfile overriden parameters set."""
    job = fyrd.Job('echo ho', profile='default', clean_files=True, qtype='local',
                   clean_outputs=True, outfile='joe', errfile='john')
    job.submit()
    out = job.get()
    assert out == 'ho\n'
    assert job.stdout == 'ho\n'
    assert job.stderr == ''


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_depends():
    """Run some jobs with dependencies."""
    job = fyrd.Job('sleep 3', profile='default', clean_files=True,
                   clean_outputs=True, qtype='local')
    job.submit()
    job.submit()  # Test submission abort
    job2 = fyrd.Job('echo eggs', profile='default', clean_files=True,
                    clean_outputs=True, depends=job, qtype='local').submit()
    out = job2.get()
    assert out == 'eggs\n'
    assert job2.stdout == 'eggs\n'
    assert job2.stderr == ''
    job3 = fyrd.Job('echo cheese', profile='default', clean_files=True,
                    clean_outputs=True, depends=job2.id, qtype='local').submit()
    out = job3.get()
    assert out == 'cheese\n'
    assert job3.stdout == 'cheese\n'
    assert job3.stderr == ''


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_resubmit():
    """Alter a job and resubmit."""
    job = fyrd.Job('echo ho', profile='default', clean_files=True, qtype='local',
                   clean_outputs=True, cores=2, mem=2000, time='00:02:00')
    job.submit()
    out = job.get()
    assert out == 'ho\n'
    assert job.stdout == 'ho\n'
    assert job.stderr == ''
    job.command = 'echo hi'
    job.resubmit()
    out = job.get()
    assert out == 'hi\n'
    assert job.stdout == 'hi\n'
    assert job.err == ''


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_job_cleaning():
    """Delete intermediate files without autoclean."""
    job = fyrd.Job('echo hi', profile='default', clean_files=False,
                   clean_outputs=False, qtype='local').submit()
    job.wait()
    assert os.path.isfile(job.outfile)
    assert os.path.isfile(job.errfile)
    assert os.path.isfile(job.submission.file_name)
    job.submission.clean(delete_output=True)
    job.clean(delete_outputs=True)
    assert not os.path.isfile(job.outfile)
    assert not os.path.isfile(job.errfile)
    assert not os.path.isfile(job.submission.file_name)


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_multi_job_cleaning():
    """Delete intermediate files for more than one job."""
    job = fyrd.Job('echo hi', profile='default', clean_files=False,
                   clean_outputs=False, qtype='local').submit()
    job2 = fyrd.Job('echo ho', profile='default', clean_files=False,
                    clean_outputs=False, qtype='local').submit()
    fyrd.basic.wait([job, job2])
    assert os.path.isfile(job.outfile)
    assert os.path.isfile(job.errfile)
    assert os.path.isfile(job.submission.file_name)
    assert os.path.isfile(job2.outfile)
    assert os.path.isfile(job2.errfile)
    assert os.path.isfile(job2.submission.file_name)
    fyrd.basic.clean([job, job2], clean_outputs=True)
    assert not os.path.isfile(job.outfile)
    assert not os.path.isfile(job.errfile)
    assert not os.path.isfile(job.submission.file_name)
    assert not os.path.isfile(job2.outfile)
    assert not os.path.isfile(job2.errfile)
    assert not os.path.isfile(job2.submission.file_name)


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_function_submission():
    """Submit a function."""
    job = fyrd.Job(write_to_file, ('42', 'bobfile'), clean_files=False,
                   qtype='local')
    job.submit()
    job.wait()
    job.fetch_outputs()
    out = job.get(delete_outfiles=False)
    job.function.clean(delete_output=True)
    job.clean()
    sys.stdout.write('{};\nOut: {}\nSTDOUT: {}\nSTDERR: {}\n'
                     .format(job.exitcode, out, job.stdout, job.stderr))
    print(repr(job))
    print(str(job))
    print(repr(job.submission))
    print(str(job.submission))
    print(repr(job.function))
    print(str(job.function))
    assert job.exitcode == 0
    assert out == 0
    assert job.out == 0
    assert job.stdout == '\n'
    assert job.stderr == ''
    print(job.runpath)
    assert os.path.isfile('bobfile')
    with open('bobfile') as fin:
        assert fin.read().rstrip() == '42'
    os.remove('bobfile')
    job.clean(delete_outputs=True)


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_method_submission():
    """Submit a method."""
    t = MethodSubmission()
    job = fyrd.Job(t.do_math, (2,), qtype='local')
    t2 = job.get()
    assert t2 == t.me*2


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_function_keywords():
    """Submit a simple function with keyword arguments."""
    job = fyrd.Job(raise_me, (10,), kwargs={'power': 10},
                   qtype='local').submit()
    assert job.get() == 10**10
    job.clean(delete_outputs=True)


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_function_deco():
    """Submit a simple decorated function."""
    fyrd.batch_systems.MODE = 'local'
    job = raise_me_deco(10, power=10)
    assert job.get() == 10**10
    job.clean(delete_outputs=True)


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_function_deco2():
    """Submit a simple decorated function."""
    fyrd.batch_systems.MODE = 'local'
    job = raise_me_deco2(10)
    job.submit()
    assert job.get() == 10**2
    job.clean(delete_outputs=True)


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_splitfile():
    """Use the splitfile helper function."""
    out = fyrd.helpers.splitrun(
        2, 'tests/test.txt.gz', False, dosomething, ('{file}',),
        qtype='local'
    )
    assert out == dosomething('tests/test.txt.gz')


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_splitfile_script():
    """Test splitfile() with a script and outfile."""
    out = fyrd.helpers.splitrun(
        2, 'tests/test.txt.gz', False, dosomething, ('{file}',),
        qtype='local'
    )
    assert out == dosomething('tests/test.txt.gz')


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_splitfile_indirect():
    """Use the splitfile helper function."""
    fyrd.batch_systems.MODE = 'local'
    job = fyrd.helpers.splitrun(
        2, 'tests/test.txt.gz', False, SCRIPT, name='test',
        outfile='test.out.txt', direct=False
    )
    job.wait()
    os.remove('test.txt.gz.split_0001.gz.out')
    os.remove('test.txt.gz.split_0002.gz.out')
    assert os.path.isfile('test.out.txt')
    os.remove('test.out.txt')
    return 0


@pytest.mark.skipif(not env,
                    reason="No valid batch system detected")
def test_dir_clean():
    """Clean all job files in this dir."""
    fyrd.basic.clean_dir(delete_outputs=True)
