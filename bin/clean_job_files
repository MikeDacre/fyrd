#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Clean all intermediate files created by the cluster module from this dir.

============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2016-34-15 15:06
 Last modified: 2016-06-16 10:42

   DESCRIPTION: Uses the cluster.job.clean_dir() function

       CAUTION: The clean() function will delete **EVERY** file with
                extensions matching those these::
                    .<suffix>.err
                    .<suffix>.out
                    .<suffix>.sbatch & .cluster.script for slurm mode
                    .<suffix>.qsub for torque mode
                    .<suffix> for local mode
                    _func.<suffix>.py
                    _func.<suffix>.py.pickle.in
                    _func.<suffix>.py.pickle.out

============================================================================
"""
import sys
import argparse
import cluster

def main(argv=None):
    """Command line parsing."""
    if not argv:
        argv = sys.argv[1:]

    parser  = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-d', '--dir',
                        help="Directory to clean")
    parser.add_argument('-s', '--suffix',
                        help="Directory to clean")
    parser.add_argument('-q', '--qtype', choices=('torque', 'slurm', 'local'),
                        help="Limit deletions to this qtype")
    # We store this as false as the question is a negative
    parser.add_argument('-n', '--no-confirm', action='store_false',
                        help="Do not confirm before deleting (for scripts)")
    parser.add_argument('-v', '--verbose',
                        help="Show debug information")

    args = parser.parse_args(argv)

    if args.verbose:
        cluster.logme.MIN_LEVEL = 'debug'

    files = cluster.job.clean_dir(directory=args.dir, suffix=args.suffix,
                                  qtype=args.qtype, confirm=args.no_confirm)

    # Print list of files if it wasn't done by the function
    if not args.no_confirm:
        sys.stdout.write('Deleted files:\n\t')
        sys.stdout.write('\n\t'.join(files))
        sys.stdout.write('\n')

if __name__ == '__main__' and '__file__' in globals():
    sys.exit(main())
