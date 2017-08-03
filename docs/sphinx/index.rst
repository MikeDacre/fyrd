Fyrd's Documentation
====================

Python job submission on torque and slurm clusters with dependency tracking.

+---------+----------------------------------------------------+
| Author  | Michael D Dacre <mike.dacre@gmail.com>             |
+---------+----------------------------------------------------+
| License | MIT License, property of Stanford, use as you wish |
+---------+----------------------------------------------------+
| Version | 0.6.1b9                                            |
+---------+----------------------------------------------------+

.. only:: html

  .. image:: https://readthedocs.org/projects/fyrd/badge/?version=latest
     :target: https://fyrd.readthedocs.io/
  .. image:: https://travis-ci.org/MikeDacre/fyrd.svg?branch=master
     :target: https://travis-ci.org/MikeDacre/fyrd
  .. image:: https://api.codacy.com/project/badge/Grade/c163cff81a1941a18b2c5455901695a3
     :target: https://www.codacy.com/app/mike-dacre/fyrd?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=MikeDacre/fyrd&amp;utm_campaign=Badge_Grade
  .. image:: https://img.shields.io/badge/python%20versions-2.7%203.4%203.5%203.6-brightgreen.svg
  .. image:: https://badge.fury.io/py/fyrd.svg
     :target: https://badge.fury.io/py/fyrd
     :alt: PyPI Version

Allows simple job submission with *dependency tracking and queue waiting* on
either torque, slurm, or locally with the multiprocessing module. It uses simple
techniques to avoid overwhelming the queue and to catch bugs on the fly.

It is routinely tested on Mac OS and Linux with slurm and torque clusters, or in
the absence of a cluster, on Python versions 2.7.10, 2.7.11, 2.7.12, 3.3.0, 3.4.0,
3.5.2, 3.6.2, and 3.7-dev. The full test suite is available in the `tests` folder.

Fyrd is pronounced 'feared' (sort of), it is an Anglo-Saxon term for an army,
particularly an army of freemen (in this case an army of compute nodes). The
logo is based on a Saxon shield commonly used by these groups. This software
was formerly known as 'Python Cluster'.

The code is hosted at github:
`https://github.com/MikeDacre/fyrd <https://github.com/MikeDacre/fyrd>`_

To install, use `PyPI <https://pypi.python.org/pypi/fyrd>`_:

.. code:: shell

   pip install fyrd
   fyrd conf init

Contents:

.. toctree::
   :maxdepth: 3

   basic_usage
   configuration
   keywords
   console
   advanced_usage
   api
   indices
