#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Write the current set of options to a file.

============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2016-31-16 23:06
 Last modified: 2016-06-17 17:41

============================================================================
"""
import os
import sys
sys.path.append(os.path.abspath('../'))
import cluster
cluster.jobqueue.THREADS = 5

with open('options_help.txt', 'w') as fout:
    fout.write(cluster.option_help(mode='string'))
