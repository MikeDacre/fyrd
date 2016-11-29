#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Run all applicable tests.

============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
       CREATED: 2016-54-22 15:06
 Last modified: 2016-11-28 16:53

   DESCRIPTION: Run multiple kinds of tests, provide options to skip some.

============================================================================
"""
from __future__ import print_function
import os
import sys
import argparse
from subprocess import call


def main(argv=None):
    """Get arguments and run tests."""
    if not argv:
        argv = sys.argv[1:]

    parser  = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-l', '--local', action="store_true",
                        help="Skip remote tests")
    parser.add_argument('-c', '--coverage', action="store_true",
                        help="Generate coverage")
    parser.add_argument('-v', '--verbose', action="store_true",
                        help="Verbose")

    args = parser.parse_args(argv)

    # Move us up one if we are in the tests directory
    if os.path.basename(os.path.abspath('.')) == 'tests':
        os.chdir('..')

    if args.coverage:
        if os.path.exists('.coverage'):
            os.remove('.coverage')
        cmnd = ['coverage', 'run', '-a', '--source', 'fyrd']
        pytt = ['py.test', '--cov=fyrd']
    else:
        cmnd = [sys.executable]
        pytt = ['py.test']

    # Run the tests
    if args.local:
        print('Skipping remote queue tests')
        pytt += ['tests/test_options.py', 'tests/test_queue.py',
                 'tests/test_local.py', 'tests/test_config.py']
    outcode = call(pytt)
    print('py.test tests complete with code {}, running local queue test.'
          .format(outcode))

    local_args = cmnd + ['tests/local_queue.py']
    if args.verbose:
        local_args.append('-v')
    outcode += call(local_args)

    print('local test complete with outcode {}.'
          .format(outcode))

    return outcode

if __name__ == '__main__' and '__file__' in globals():
    sys.exit(main())
