"""Test options handling."""
import os
import sys
sys.path.append(os.path.abspath('.'))
import cluster

def test_help():
    """Check that the output of option_help() matches saved output."""
    if os.path.isfile('options_help.txt'):
        ofile = 'options_help.txt'
    elif os.path.isfile('tests/options_help.txt'):
        ofile = 'options_help.txt'
    else:
        raise Exception('Cannot find options_help.txt file')
    assert cluster.option_help(prnt=False) == open(ofile).read()
