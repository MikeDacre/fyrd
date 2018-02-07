"""Test options handling."""
import os
import sys
from collections import OrderedDict
import pytest
sys.path.append(os.path.abspath('.'))
import fyrd


def test_help():
    """Check that the output of option_help() matches saved output."""
    if os.path.isfile(os.path.join('tests', 'options_help.txt')):
        ofile = os.path.join('tests', 'options_help.txt')
    elif os.path.isfile('options_help.txt'):
        ofile = 'options_help.txt'
    else:
        raise Exception('Cannot find options_help.txt file')
    assert fyrd.option_help(mode='string') == open(ofile).read()
    fyrd.option_help(mode='table')
    fyrd.option_help(mode='merged_table')


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
    # Should succeed
    fyrd.options.check_arguments({'nodes': '14'})


def test_memory_formatting():
    """Format memory several different ways."""
    assert fyrd.options.check_arguments({'mem': '4000b'}) == {'mem': 5}
    assert fyrd.options.check_arguments({'mem': '40000KB'}) == {'mem': 39}
    assert fyrd.options.check_arguments({'mem': '4000mB'}) == {'mem': 4000}
    assert fyrd.options.check_arguments({'mem': '4GB'}) == {'mem': 4096}
    assert fyrd.options.check_arguments({'mem': '4TB'}) == {'mem': 4194304}
    assert fyrd.options.check_arguments({'memory': 4000}) == {'mem': 4000}
    with pytest.raises(ValueError):
        fyrd.options.check_arguments({'mem': 'bob'})
    with pytest.raises(ValueError):
        fyrd.options.check_arguments({'mem': '4000zb'})
    with pytest.raises(ValueError):
        fyrd.options.check_arguments({'mem': '4000tb0'})
    with pytest.raises(ValueError):
        fyrd.options.check_arguments({'mem': 'tb0'})
    with pytest.raises(ValueError):
        fyrd.options.check_arguments({'mem': 'tb'})


def test_time_formatting():
    """Format time several different ways."""
    assert fyrd.options.check_arguments(
        {'time': '01-00:00:00'}
    ) == {'time': '24:00:00'}
    assert fyrd.options.check_arguments(
        {'walltime': '03'}
    ) == {'time': '00:00:03'}
    assert fyrd.options.check_arguments(
        {'time': '99:99'}
    ) == {'time': '01:40:39'}
    with pytest.raises(fyrd.options.OptionsError):
        fyrd.options.check_arguments({'time': '00:00:00:03'})


def test_split():
    """Run with good and bad arguments, expect split."""
    good, bad = fyrd.options.split_keywords(
        {'cores': 2, 'memory': '4GB', 'bob': 'dylan'}
    )
    assert good == {'cores': 2, 'mem': 4096}
    assert bad == {'bob': 'dylan'}


def test_string_formatting():
    """Test options_to_string."""
    test_options = {
        'nodes': '2', 'cores': 5, 'account': 'richguy',
        'features': 'bigmem', 'time': '20', 'mem': 2000,
        'partition': 'large', 'export': 'PATH', 'outfile': 'joe',
        'errfile': 'john'
    }
    assert sorted(
        fyrd.options.options_to_string(
            test_options,
            qtype='torque'
        ).split('\n')
    ) == [
        '#PBS -A richguy',
        '#PBS -e john',
        '#PBS -l mem=2000MB',
        '#PBS -l nodes=2:ppn=5:bigmem',
        '#PBS -l walltime=00:00:20',
        '#PBS -o joe',
        '#PBS -q large',
        '#PBS -v PATH'
    ]
    assert sorted(
        fyrd.options.options_to_string(
            test_options,
            qtype='slurm'
        ).split('\n')
    ) == [
        '#SBATCH --account=richguy',
        "#SBATCH --constraint=['bigmem']",
        '#SBATCH --cpus-per-task 5',
        '#SBATCH --export=PATH',
        '#SBATCH --mem=2000',
        '#SBATCH --ntasks 2',
        '#SBATCH --time=00:00:20',
        '#SBATCH -e john',
        '#SBATCH -o joe',
        '#SBATCH -p large'
    ]
    assert fyrd.options.options_to_string(
        test_options,
        qtype='local'
    ) == '\n'
    with pytest.raises(fyrd.options.OptionsError):
        fyrd.options.option_to_string('nodes', 2)
    with pytest.raises(ValueError):
        fyrd.options.option_to_string({'nodes': 2})


def test_back_to_normal():
    """Return the queue to the normal setting."""
    fyrd.queue.get_cluster_environment()
