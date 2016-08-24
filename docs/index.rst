Welcome to Python Cluster's documentation!
==========================================

Submit jobs to slurm or torque, or with multiprocessing.

+---------+----------------------------------------------------+
| Author  | Michael D Dacre <mike.dacre@gmail.com>             |
+---------+----------------------------------------------------+
| License | MIT License, property of Stanford, use as you wish |
+---------+----------------------------------------------------+
| Version | 0.6.1b                                             |
+---------+----------------------------------------------------+

Allows easy job submission with *dependency tracking and queue waiting* with
either torque, slurm, or locally with the multiprocessing module. It uses simple
techiques to avoid overwhelming the queue and to catch bugs (e.g. queue stalling)
on the fly.

NOTE: While this software is extremely powerful for pure python-based cluster job
submission, `snakemake <https://bitbucket.org/snakemake/snakemake/wiki/Home>`_ is
possibly a better choice for very large workflows.

Contents:

.. toctree::
   :maxdepth: 3
   
   usage
   scripts
   api
 

* :ref:`modindex`  
* :ref:`search`
