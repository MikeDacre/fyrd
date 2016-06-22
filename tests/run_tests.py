#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Run all applicable tests.

============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
       CREATED: 2016-54-22 15:06
 Last modified: 2016-06-22 16:08

   DESCRIPTION: Run multiple kinds of tests, provide options to skip some.

============================================================================
"""
from __future__ import print_function
import os
import sys
import argparse
from subprocess import check_call

try:
    import pytest
except ImportError:
    print('Cannot run tests without py.test installed')
    sys.exit(1)


def main(argv=None):
    """Get arguments and run tests."""
    if not argv:
        argv = sys.argv[1:]

    parser  = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-l', '--local', action="store_true",
                        help="Skip remote tests")

    args = parser.parse_args(argv)

    # Move us up one if we are in the tests directory
    if os.path.basename(os.path.abspath('.')) == 'tests':
        os.chdir('..')

    # Run the tests
    print('Running py.test tests')
    if args.local:
        print('Skipping remote queue tests')
        pytest.main(['tests/test_options.py', 'tests/test_queue.py',
                     'tests/test_local.py'])
    else:
        pytest.main()

    print('py.test tests complete, running local queue test.')
    check_call([sys.executable, 'tests/local_queue.py'])

if __name__ == '__main__' and '__file__' in globals():
    sys.exit(main())
