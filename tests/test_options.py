"""Test options handling."""
import os
import sys
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
