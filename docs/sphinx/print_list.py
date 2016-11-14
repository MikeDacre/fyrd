#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Print a table of keyword options.

Last modified: 2016-11-11 20:18
"""
from fyrd import options, run

kwds = run.indent(options.option_help('list'))
with open('keyword_list.rst', 'w') as fout:
    fout.write(kwds)
