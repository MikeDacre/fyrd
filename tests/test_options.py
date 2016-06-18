"""Test options handling."""
import os
import sys
from collections import OrderedDict
import pytest
sys.path.append(os.path.abspath('.'))
import cluster
cluster.jobqueue.THREADS = 5

def test_help():
    """Check that the output of option_help() matches saved output."""
    if os.path.isfile(os.path.join('tests', 'options_help.txt')):
        ofile = os.path.join('tests', 'options_help.txt')
    elif os.path.isfile('options_help.txt'):
        ofile = 'options_help.txt'
    else:
        raise Exception('Cannot find options_help.txt file')
    assert cluster.option_help(mode='string') == open(ofile).read()

def test_dict_types():
    """Make sure all expected dictionaries exist and have the right type."""
    assert hasattr(cluster.options, 'COMMON')
    assert hasattr(cluster.options, 'NORMAL')
    assert hasattr(cluster.options, 'CLUSTER_OPTS')
    assert hasattr(cluster.options, 'TORQUE')
    assert hasattr(cluster.options, 'SLURM')
    assert hasattr(cluster.options, 'SLURM_KWDS')
    assert hasattr(cluster.options, 'TORQUE_KWDS')
    assert hasattr(cluster.options, 'CLUSTER_KWDS')
    assert hasattr(cluster.options, 'NORMAL_KWDS')
    assert hasattr(cluster.options, 'ALLOWED_KWDS')
    assert isinstance(cluster.options.COMMON, OrderedDict)
    assert isinstance(cluster.options.NORMAL, OrderedDict)
    assert isinstance(cluster.options.CLUSTER_OPTS, OrderedDict)
    assert isinstance(cluster.options.TORQUE, OrderedDict)
    assert isinstance(cluster.options.SLURM, OrderedDict)
    assert isinstance(cluster.options.SLURM_KWDS, OrderedDict)
    assert isinstance(cluster.options.TORQUE_KWDS, OrderedDict)
    assert isinstance(cluster.options.CLUSTER_KWDS, OrderedDict)
    assert isinstance(cluster.options.NORMAL_KWDS, OrderedDict)
    assert isinstance(cluster.options.ALLOWED_KWDS, OrderedDict)

def test_sane_keywords():
    """Run check_arguments() on some made up keywords."""
    # Should succeed
    cluster.options.check_arguments(
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
        cluster.options.check_arguments({'nodes': 'bob'})
    with pytest.raises(ValueError):
        cluster.options.check_arguments({'mem': 'bob'})
    # Should succeed
    cluster.options.check_arguments({'nodes': '14'})

def test_string_formatting():
    """Test options_to_string."""
    cluster.queue.MODE = 'torque'
    outstr = cluster.options.options_to_string({'nodes': '2', 'cores': 5})
    assert outstr == '#PBS -l nodes=2:ppn=5'
    cluster.queue.MODE = 'slurm'
    outstr = cluster.options.options_to_string({'nodes': '2', 'cores': 5})
    assert outstr == '#SBATCH --ntasks 2\n#SBATCH --cpus-per-task 5'
    cluster.queue.MODE = 'local'
    outstr = cluster.options.options_to_string({'nodes': '2', 'cores': 5})
    assert outstr == ''
    with pytest.raises(cluster.options.OptionsError):
        cluster.options.option_to_string('nodes', 2)

def test_back_to_normal():
    """Return the queue to the normal setting."""
    cluster.queue.get_cluster_environment()
