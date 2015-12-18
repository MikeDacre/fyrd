#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8 tabstop=4 expandtab shiftwidth=4 softtabstop=4
"""
#============================================================================#
#                                                                            #
#        AUTHOR: Michael D Dacre, mike.dacre@gmail.com                       #
#  ORGANIZATION: Stanford University                                         #
#       LICENSE: MIT License, property of Stanford, use as you wish          #
#       VERSION: 0.1                                                         #
#       CREATED: 2015-12-18 12:43                                            #
# Last modified: 2015-12-18 13:08                                            #
#                                                                            #
#   DESCRIPTION: Uses slurmpy and pyslurm to check the job queue for only    #
#                one user's jobs. Produces a very simple display, for full   #
#                job information, the regular tools can be used (e.g squeue) #
#                Output:                                                     #
#                    Job_number\\tJob_name                                   #
#                The output can be modified by the following flags:          #
#                    -c count only                                           #
#                    -l space separated job number list                      #
#                In addition, only running or queued jobs can be obtained:   #
#                    -r running jobs                                         #
#                    -q queued jobs                                          #
#                                                                            #
#============================================================================#
"""
import sys
import slurmy


def main(running=False, queued=False, count=False, list=False):
    """ Run everything """
    # Create queue object
    queue = slurmy.queue()
    if running:



if __name__ == '__main__' and '__file__' in globals():
    """Command Line Argument Parsing"""
    import argparse

    f_class = argparse.RawDescriptionHelpFormatter
    parser  = argparse.ArgumentParser(description=__doc__,
                                      formatter_class=f_class)

    j = parser.add_argument_group('Choose jobs to show, default is all')
    j.add_argument('-r', '--running', action='store_true',
                   help="Show running jobs only")
    j.add_argument('-q', '--queued', action='store_true',
                   help="Show queued jobs only")

    o = parser.add_argument_group('Choose alternate output style')
    o.add_argument('-c', '--count', action='store_true',
                   help="Display count only")
    o.add_argument('-l', '--list', action='store_true',
                   help="Print space separated list of job numbers")

    args = parser.parse_args()

    # Run the script
    main(running=args.running,
         queued=args.queued,
         count=args.count,
         list=args.list)
