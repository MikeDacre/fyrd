####
Fyrd
####

One liner script and function to torque, slurm, or a local machine with
dependency tracking using python. Uses the same syntax irrespective of cluster
environment!

.. image:: http://i.imgur.com/NNbprZH.png
   :alt: fyrd cluster logo — a Saxon shield remeniscent of those used in fyrds
   :target: https://fyrd.readthedocs.org
   :height: 200
   :width: 200

+---------+----------------------------------------------------+
| Author  | Michael D Dacre <mike.dacre@gmail.com>             |
+---------+----------------------------------------------------+
| License | MIT License, property of Stanford, use as you wish |
+---------+----------------------------------------------------+
| Version | 0.6.1-beta.5                                       |
+---------+----------------------------------------------------+

.. image:: https://readthedocs.org/projects/fyrd/badge/?version=latest
   :target: https://fyrd.readthedocs.io/
.. image:: https://travis-ci.org/MikeDacre/fyrd.svg?branch=master
   :target: https://travis-ci.org/MikeDacre/python-cluster
.. image:: https://api.codacy.com/project/badge/Grade/c163cff81a1941a18b2c5455901695a3
   :target: https://www.codacy.com/app/mike-dacre/fyrd?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=MikeDacre/fyrd&amp;utm_campaign=Badge_Grade
.. image:: https://img.shields.io/badge/python%20versions-2.7%203.4%203.5%203.6-brightgreen.svg

Allows simple job submission with *dependency tracking and queue waiting* on
either torque, slurm, or locally with the multiprocessing module. It uses simple
techniques to avoid overwhelming the queue and to catch bugs on the fly.

It is routinely tested on Mac OS and Linux with slurm and torque clusters, or
in the absence of a cluster, on Python versions `2.7.10`, `2.7.11`, `2.7.12`,
`3.3.0`, `3.4.0`, `3.5.2`, `3.6-dev`, and `3.7-dev`. The full test suite is
available in the `tests` folder.

Fyrd is pronounced 'feared' (sort of), it is an Anglo-Saxon term for an army,
particularly an army of freemen (in this case an army of compute nodes). The
logo is based on a Saxon shield commonly used by these groups. This software
was formerly known as 'Python Cluster'.

For usage instructions and complete documentation see `the documentation site
<https://fyrd.readthedocs.io>`_ and the `fyrd_manual.pdf
</docs/fyrd_manual.pdf>`_ document in this repository.

.. contents:: **Contents**

Overview
========

This library was created to make working with torque or slurm clusters as easy
as working with the multiprocessing library. It aims to provide:

- Easy submission of either python functions or shell scripts to torque or slurm
  from within python.
- Simple dependency tracking for jobs.
- The ability to submit jobs with any of the torque or slurm keyword arguments.
- Easy customization.
- Very simple usage that scales to complex applications.
- A simple queue monitoring API that functions identically with torque and slurm
  queues.
- A fallback local mode that allows code to run locally using the multiprocessing
  module without needing any changes to syntax.

To do this, all major torque and slurm keyword arguments are encoded in dictionaries
in the `fyrd/options.py` file using synonyms so that all arguments are standardized
on the fly. Job management is handled by the `Job` class in `fyrd/job.py`, which
accepts any of the keyword arguments in the options file. To make submission as simple
as possible, the code makes used of profiles defined in the `~/.fyrd/profiles.txt`
config file. These allow simple grouping of keyword arguments into named profiles to
make submission even easier. Dependency tracking is handled by the `depends=`
argument to `Job`, which accepts job numbers or `Job` objects, either singularly or
as lists.

To allow simple queue management and job waiting, a `Queue` class is
implemented in `fyrd/queue.py`. It uses iterators, also defined in that file,
to parse torque or slurm queues transparently and allow access to their
attributes through the `Queue` class and the `Queue.jobs` dictionary. The `Job`
class uses this system to block until the job completes when either the
`wait()` or `get()` methods are called.

To allow similar functionality on a system not connected to a torque or slurm
queue, a local queue that behaves similarly, including allowing dependency
tracking, is implemented in the `fyrd/jobqueue.py` file. It is based on
multiprocessing but behaves like torque.  It is not a good idea to use this
module in place of multiprocessing due to the dependency tracking overhead, it
is primarily intended as a fallback, but it does work well enough to use
independently.

As all clusters are different, common alterable parameters are defined in a
config file located at `~/.fyrd/config.txt`. This includes an option for max
queue size, which makes job submission block until the queue has opened up,
preventing job submission failure on systems with queue limits (most clusters).

To make life easier, a bunch of simple wrapper functions are defined in
`fyrd/basic.py` that allow submission without having to worry about using the
class system, or to submit existing job files. Several helper function are also
created in `fyrd/helpers.py` that allow the automation of more complex tasks,
like running `apply` on a pandas dataframe in parallel on the cluster
(`fyrd.helpers.parapply()`).

The end result is that submitting 10 thousand very small jobs to a small cluster
can be done like this:

.. code:: python

   jobs = []
   for i in huge_list:
       jobs.append(fyrd.Job(my_function, (i,), profile='small').submit())
   results = []
   for i in jobs:
       results.append(i.get())

The results list in this example will contain the function outputs, even if those
outputs are integers, objects, or other Python types. Similarly, shell scripts can
be run like this:

.. code:: python

   script = r"""zcat {} | grep "#config" | awk '{split($1,a,"."); print a[2]"\t"$2}'"""
   jobs   = []
   for i in [i for i in os.listdir('.') if i.endswith('.gz')]:
       jobs.append(fyrd.Job(script.format(i), profile='long').submit())
   results = []
   for i in jobs:
       i.wait()
       results.append(i.stdout)

Results will contain the contents of STDOUT for the submitted script

Here is the same code with dependency tracking:

.. code:: python

   script = r"""zcat {} | grep "#config" | awk '{split($1,a,"."); print a[2]"\t"$2}'"""
   jobs   = []
   jobs2  = []
   for i in [i for i in os.listdir('.') if i.endswith('.gz')]:
       j = fyrd.Job(script.format(i), profile='long').submit()
       jobs.append(j)
       jobs2.append(fyrd.Job(my_function, depends=j).submit())
   results = []
   for i in jobs2:
       i.wait()
       results.append(i.out)

As you can see, the `profile` keyword is not required, if not supplied the
default profile is used. It is also important to note that `.out` will contain
the same contents as `.stdout` for all script submissions, but for function
submissions, `.out` contains the function output, not STDOUT.

Installation
-------------

This module will work with Python 2.7+ on Linux and Mac OS systems.

It is not on PyPI yet, but it will be as of version 0.6.2.

To install, do the following:

.. code:: shell

   pip install https://github.com/MikeDacre/fyrd/archive/v0.6.1-beta.5.tar.gz
   fyrd conf init

To get the latest version:

.. code:: shell

   pip install https://github.com/MikeDacre/fyrd/tarball/master
   fyrd conf init

The `fyrd conf init` command initializes your environment interactively by
asking questions about the local cluster system.

I recommend installing using pyenv in a pyenv anaconda environment, this will
make your life much simpler, but is not required.

In general you want either `pyenv <https://github.com/yyuu/pyenv>`_ or user
level install (`pip install --user`) even if you have sudo access, as most
cluster environments share /home/<user> across the cluster, making this module
available everywhere.

Importing is simple:

.. code:: python

  import fyrd

Prerequisites
.............

This software requires two external modules:
- `dill <https://pypi.python.org/pypi/dill>`_ —  which makes function submission more stable
- `tabulate <https://pypi.python.org/pypi/tabulate>`_ —  allows readable printing of help

Cluster Dependencies
....................

In order to submit functions to the cluster, this module must import them on the
compute node. This means that all of your python modules must be available on
every compute node.

By default, the same python executable used for submission is used on the
cluster to run functions, however, this can be overridden by the
'generic_python' option on the cluster. If using this option, you must install
all of your local modules on the cluster also.

To avoid pain and debugging, you can do this manually by running this on your
login node:

.. code:: shell

  freeze --local | grep -v '^\-e' | cut -d = -f 1 > module_list.txt

And then on the compute nodes:

.. code:: shell

  cat module_list.txt | xargs pip install --user

Alternately, if your pyenv is available on the cluster nodes, then all of
your modules are already available, so you don't need to worry about this!


Issues and Contributing
=======================

If you have any trouble with this software add an issue in
https://github.com/MikeDacre/python-cluster/issues

I am always looking for help testing the software, expanding the number of
keywords, and implementing new features. If you want to help out, contact
me or just fork the repo and send me a pull request when you are done.

Please look through the code and follow the style as closely as possible,
also, please either message me or add information to the issues page before
starting work so that I know what you are working on!


Why the Name?
=============

I gave this project the name 'Fyrd' in honour of my grandmother, Hélène
Sandolphen, who was a scholar of old English. It is the old Anglo-Saxon word
for 'army', and this code gives you an army of workers on any machine so it
seemed appropriate.

The project used to be called "Python Cluster", which is more descriptive but
frankly boring. Also, about half a dozen other projects have almost the same
name, so it made no sense to keep that name and put the project onto PyPI.


Documentation
=============

This software is much more powerful that this document gives it credit for,
to get the most out of it, read the docs at https://fyrd.readthedocs.org
or get the PDF version from the file in `docs/fyrd.pdf`.
