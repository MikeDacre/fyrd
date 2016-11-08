"""Test options handling."""
import os
import sys
from collections import OrderedDict
import pytest
sys.path.append(os.path.abspath('.'))
import fyrd
fyrd.local.THREADS = 5


def test_help():
    """Check that the output of option_help() matches saved output."""
    if os.path.isfile(os.path.join('tests', 'options_help.txt')):
        ofile = os.path.join('tests', 'options_help.txt')
    elif os.path.isfile('options_help.txt'):
        ofile = 'options_help.txt'
    else:
        raise Exception('Cannot find options_help.txt file')
    assert fyrd.option_help(mode='string') == open(ofile).read()


def test_dict_types():
    """Make sure all expected dictionaries exist and have the right type."""
    assert hasattr(fyrd.options, 'COMMON')
    assert hasattr(fyrd.options, 'NORMAL')
    assert hasattr(fyrd.options, 'CLUSTER_OPTS')
    assert hasattr(fyrd.options, 'TORQUE')
    assert hasattr(fyrd.options, 'SLURM')
    assert hasattr(fyrd.options, 'SLURM_KWDS')
    assert hasattr(fyrd.options, 'TORQUE_KWDS')
    assert hasattr(fyrd.options, 'CLUSTER_KWDS')
    assert hasattr(fyrd.options, 'NORMAL_KWDS')
    assert hasattr(fyrd.options, 'ALLOWED_KWDS')
    assert isinstance(fyrd.options.COMMON, OrderedDict)
    assert isinstance(fyrd.options.NORMAL, OrderedDict)
    assert isinstance(fyrd.options.CLUSTER_OPTS, OrderedDict)
    assert isinstance(fyrd.options.TORQUE, OrderedDict)
    assert isinstance(fyrd.options.SLURM, OrderedDict)
    assert isinstance(fyrd.options.SLURM_KWDS, OrderedDict)
    assert isinstance(fyrd.options.TORQUE_KWDS, OrderedDict)
    assert isinstance(fyrd.options.CLUSTER_KWDS, OrderedDict)
    assert isinstance(fyrd.options.NORMAL_KWDS, OrderedDict)
    assert isinstance(fyrd.options.ALLOWED_KWDS, OrderedDict)


def test_sane_keywords():
    """Run check_arguments() on some made up keywords."""
    # Should succeed
    fyrd.options.check_arguments(
        {
            'cores': 49,
            'mem': '60GB',
            'modules': ['python', 'jeremy'],
            'imports': 'pysam',
            'filedir': '/tmp',
            'dir': '.',
            'suffix': 'bob',
            'outfile': 'hi!',
            'errfile': 'err',
            'threads': 6,
            'nodes': 2,
            'features': 'bigmem',
            'partition': 'default',
            'account': 'richjoe',
            'export': 'PYTHONPATH',
            'begin': '00:02:00',
        }
    )
    # Should fail
    with pytest.raises(TypeError):
        fyrd.options.check_arguments({'nodes': 'bob'})
    with pytest.raises(ValueError):
        fyrd.options.check_arguments({'mem': 'bob'})
    # Should succeed
    fyrd.options.check_arguments({'nodes': '14'})
    # Check mem
    i = fyrd.options.check_arguments({'mem': '4GB'})
    assert i == {'mem': 4096}
    # Check time
    j = fyrd.options.check_arguments({'time': '01-00:00:00'})
    assert j == {'time': '24:00:00'}


def test_split():
    """Run with good and bad arguments, expect split."""
    good, bad = fyrd.options.split_keywords(
        {'cores': 2, 'memory': '4GB', 'bob': 'dylan'}
    )
    assert good == {'cores': 2, 'mem': 4096}
    assert bad == {'bob': 'dylan'}


def test_string_formatting():
    """Test options_to_string."""
    fyrd.queue.MODE = 'torque'
    outstr = fyrd.options.options_to_string({'nodes': '2', 'cores': 5})
    assert outstr == '#PBS -l nodes=2:ppn=5'
    fyrd.queue.MODE = 'slurm'
    outstr = fyrd.options.options_to_string({'nodes': '2', 'cores': 5})
    assert outstr == '#SBATCH --ntasks 2\n#SBATCH --cpus-per-task 5'
    fyrd.queue.MODE = 'local'
    outstr = fyrd.options.options_to_string({'nodes': '2', 'cores': 5})
    assert outstr == ''
    with pytest.raises(fyrd.options.OptionsError):
        fyrd.options.option_to_string('nodes', 2)


def test_back_to_normal():
    """Return the queue to the normal setting."""
    fyrd.queue.get_cluster_environment()
