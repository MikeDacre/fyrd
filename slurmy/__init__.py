"""
#============================================================================#
#                                                                            #
#       PROJECT: slurmy                                                      #
#        AUTHOR: Michael D Dacre, mike.dacre@gmail.com                       #
#  ORGANIZATION: Stanford University                                         #
#       LICENSE: MIT License, property of Stanford, use as you wish          #
#       VERSION: 0.2                                                         #
#       CREATED: 2015-12-11 22:19                                            #
# Last modified: 2015-12-11 22:27                                            #
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

##
# The End #
# Load config file
from ._get_config import _get_config

_defaults = _get_config()

# Regular functions
from .slurmy import *

# Cython
from .queue import *
