Fyrd's Documentation
====================
 
Submit jobs to compute clusters with slurm, torque, or simple multiprocessing.

+---------+----------------------------------------------------+
| Author  | Michael D Dacre <mike.dacre@gmail.com>             |
+---------+----------------------------------------------------+
| License | MIT License, property of Stanford, use as you wish |
+---------+----------------------------------------------------+
| Version | 0.6.1-beta.4                                       |
+---------+----------------------------------------------------+

.. image:: https://readthedocs.org/projects/fyrd/badge/?version=latest
   :target: https://fyrd.readthedocs.io/
.. image:: https://travis-ci.org/MikeDacre/fyrd.svg?branch=master
   :target: https://travis-ci.org/MikeDacre/fyrd
.. image:: https://api.codacy.com/project/badge/Grade/c163cff81a1941a18b2c5455901695a3
   :target: https://www.codacy.com/app/mike-dacre/fyrd?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=MikeDacre/fyrd&amp;utm_campaign=Badge_Grade
.. image:: https://img.shields.io/badge/python%20versions-2.7%203.4%203.5%203.6-brightgreen.svg

Pronounced 'feared' (sort of), Anglo-Saxon for an army, particularly an army of
freemen (an army of nodes). Formerly known as 'Python Cluster'.

Allows simple job submission with *dependency tracking and queue waiting* with
either torque, slurm, or locally with the multiprocessing module. It uses simple
techiques to avoid overwhelming the queue and to catch bugs on the fly.

For complete documentation see `the documentation site <https://fyrd.readthedocs.io>`_
and the `Fyrd.pdf <Fyrd.pdf>`_ document in this repository.

NOTE: This software is still in beta, the scripts in bin/ do not all function
properly and the software has not been fully tested on slurm systems. Please
report any problems to the github issues page. Version 0.6.2 will resolve all
of these outstanding bugs.

NOTE: While this software is extremely powerful for pure python-based cluster job
submission, `snakemake <https://bitbucket.org/snakemake/snakemake/wiki/Home>`_ is
possibly a better choice for very large workflows.

In the future this code will work with Makefiles and will be more robust, but it
needs further development before that happens, for now it can just be used as a
simple python submission library.

Contents:

.. toctree::
   :maxdepth: 3
   
   usage
   scripts
   api
   indices
