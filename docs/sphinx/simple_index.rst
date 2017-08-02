Fyrd's Documentation
====================
 
Python job submission on torque and slurm clusters with dependency tracking.
 
.. image:: https://i.imgur.com/ns7Wnzv.png
   :alt: fyrd cluster logo— a Saxon shield remeniscent of those used in fyrds
   :target: https://fyrd.readthedocs.org

Pronounced 'feared' (sort of), Anglo-Saxon for an army, particularly an army of
freemen (an army of nodes). The logo is based on a Saxon shield commonly used in
these fyrds. This library used to be known as 'Python Cluster'.

Allows simple job submission with *dependency tracking and queue waiting* on
either torque, slurm, or locally with the multiprocessing module. It uses simple
techniques to avoid overwhelming the queue and to catch bugs on the fly.

It is routinely tested on Mac OS and Linux with slurm and torque clusters, or in
the absence of a cluster, on Python versions 2.7.10, 2.7.11, 2.7.12, 3.3.0, 3.4.0,
3.5.2, 3.6.2, and 3.7-dev. The full test suite is available in the `tests` folder.

For complete documentation see `the documentation site <https://fyrd.readthedocs.io>`_
and the `Fyrd.pdf <Fyrd.pdf>`_ document in this repository.

Contents:

.. toctree::
   :maxdepth: 3
   
   basic_usage
   configuration
   keywords_no_table
   console
   advanced_usage
   api
