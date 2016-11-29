"""Test misc functions."""
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


def test_raiser():
    """Test the raiser function."""
    with pytest.raises(Exception):
        try:
            raise(Exception('bob'))
        except Exception:
            i = sys.exc_info()
            assert fyrd.run.is_exc(i)
            fyrd.run.raiser(i)


def simple_iterator():
    """Use with test_listify."""
    for i in [1,2,3,4]:
        yield i


def test_listify():
    """Test the listify function."""
    assert fyrd.run.listify('hi') == ['hi']
    assert fyrd.run.listify(1) == [1]
    assert fyrd.run.listify(('hi',)) == ['hi']
    assert fyrd.run.listify(('hi',)) == ['hi']
    assert fyrd.run.listify(simple_iterator()) == [1,2,3,4]
    assert fyrd.run.listify(simple_iterator) == [1,2,3,4]


def test_count_lines():
    """Test the line counter."""
    assert fyrd.run.count_lines('tests/test.txt.gz') == 2200
    assert fyrd.run.count_lines('tests/test.txt.gz', True) == 2200


def test_file_type():
    """Test fyrd.run.file_type()."""
    assert fyrd.run.file_type('my_file.txt') == 'txt'
    assert fyrd.run.file_type('my_file.txt.gz') == 'txt'
    assert fyrd.run.file_type('my_file.txt.bz2') == 'txt'
    assert fyrd.run.file_type('/a/dir/my_file.txt') == 'txt'
    assert fyrd.run.file_type('/a/dir/my_file.txt.gz') == 'txt'
    assert fyrd.run.file_type('/a/dir/my_file.txt.bz2') == 'txt'
    assert fyrd.run.file_type('my_file.bob.txt') == 'txt'
    assert fyrd.run.file_type('my_file.bob.txt.gz') == 'txt'
    assert fyrd.run.file_type('my_file.bob.txt.bz2') == 'txt'


def test_is_file_type():
    """Test fyrd.run.is_file_type()."""
    assert fyrd.run.is_file_type('bob.txt', 'txt')
    assert fyrd.run.is_file_type('bob.txt', ['txt', 'john'])
    assert fyrd.run.is_file_type('bob.john', ['txt', 'john'])
    assert fyrd.run.is_file_type('bob.fred', ['txt', 'john']) is False
    with open('./tests/test.txt.gz') as fin:
        assert fyrd.run.is_file_type(fin, 'txt')


def test_replace_args():
    """Test fyrd.run.replace_argument()."""
    arg1 = ('{file}',)
    arg2 = dict(file='{file}')
    arg3 = [arg1, arg2]
    out1 = ('spam',)
    out2 = dict(file='spam')
    out3 = [out1, out2]
    assert fyrd.run.replace_argument(arg1, '{file}', 'spam') == out1
    assert fyrd.run.replace_argument(arg2, '{file}', 'spam') == out2
    assert fyrd.run.replace_argument(arg3, '{file}', 'spam') == out3
