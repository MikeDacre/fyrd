"""
#============================================================================#
#                                                                            #
#       PROJECT: slurmy                                                      #
#        AUTHOR: Michael D Dacre, mike.dacre@gmail.com                       #
#  ORGANIZATION: Stanford University                                         #
#       LICENSE: MIT License, property of Stanford, use as you wish          #
#       VERSION: 0.2                                                         #
#       CREATED: 2015-12-11 22:19                                            #
# Last modified: 2016-03-03 15:31                                            #
#                                                                            #
#   DESCRIPTION: Submit jobs easily to SLURM, monitor the queue, control     #
#                job dependencies.                                           @
#                                                                            #
#  DEPENDENCIES: pyslurm and cython                                          #
#                                                                            #
#           URL: https://github.com/MikeDacre/python_slurm                   #
#                                                                            #
#============================================================================#
"""
# Load config file
from .config_file import get_config

defaults = get_config()

# Regular functions
from .slurmy import submit_file
from .slurmy import run
from .slurmy import make_job_file

# Cython
from .queue import queue
from .queue import monitor_submit

__all__ = ['queue', 'monitor_submit', 'submit_file', 'run', 'make_job_file']
