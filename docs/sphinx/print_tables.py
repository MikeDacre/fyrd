#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Print a table of keyword options.

Last modified: 2016-11-11 16:42
"""
from fyrd import options

tables = options.option_help('table', tablefmt='grid')
with open('keyword_table.rst', 'w') as fout:
    fout.write(tables)
