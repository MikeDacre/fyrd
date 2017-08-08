#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Write the current set of options to a file.

============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2016-31-16 23:06
 Last modified: 2017-08-05 23:55

============================================================================
"""
import os
import sys
sys.path.append(os.path.abspath('../'))
import fyrd

with open('options_help.txt', 'w') as fout:
    fout.write(fyrd.option_help(mode='string'))
