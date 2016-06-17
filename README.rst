##############
Python Cluster
##############

.. image:: https://travis-ci.org/MikeDacre/python-cluster.svg?branch=master

Submit jobs to slurm or torque, or with multiprocessing.

Author:  Michael D Dacre <mike.dacre@gmail.com>

License: MIT License, property of Stanford, use as you wish

Version: 0.6.1

Allows simple job submission with *dependency tracking and
queue waiting* with either torque, slurm, or locally with the
multiprocessing module. It uses simple techiques to avoid
overwhelming the queue and to catch bugs on the fly.

.. contents:: **Contents**

************
Installation
************

This module will work with Python 2.7+ on Linux systems.

To install, use the standard python method:

.. code:: shell

  git clone https://github.com/MikeDacre/python-cluster
  cd python-cluster
  python ./setup.py install --user

In general you want user level install even if you have sudo
access, as most cluster environments share /home/<user> across
the cluster, making this module available everywhere.

*Note:* While the name is `python-cluster` you will import it
just as `cluster`:

.. code:: python

  import cluster

Prerequisites
-------------

The only external module that I use in this software is
`dill <https://pypi.python.org/pypi/dill>`_. It isn't 100% required
but it makes function submission much more stable.

If you choose to use dill, it must be installed cluster wide.

Function Submission
-------------------

In order to submit functions to the cluster, this module must import
them on the compute node. This means that all of your python modules
must be available on every compute node. To avoid pain and debugging,
you can do this manually by running this on your loggin node:

.. code:: shell

  freeze --local | grep -v '^\-e' | cut -d = -f 1 > module_list.txt

And then on the compute nodes:

.. code:: shell

  cat module_list.txt | xargs pip install --user

This will ensure that all of your modules are installed globally.

In general it is a good idea to install modules as `--user` with pip
to avoid this issue.

************
Simple Usage
************

Setting Environment
-------------------

To set the environement, set queue.MODE to one of ['torque',
'slurm', 'local'], or run get_cluster_environment().

Simple Job Submission
---------------------

At its simplest, this module can be used by just executing
submit(<command>), where command is a function or system
command/shell script. The module will autodetect the cluster,
generate an intuitive name, run the job, and write all outputs
to files in the current directory. These can be cleaned with
clean_dir().

To run with dependency tracking, run:

.. code:: python

  import cluster
  job  = cluster.submit(<command1>)
  job2 = cluster.submit(<command2>, dependencies=job1)
  exitcode, stdout, stderr = job2.get()  # Will block until job completes

Functions
---------

The submit function works well with python functions as well as with
shell scripts and shell commands.

*However,* in order for this to work, `cluster` ends up importing your
original script file on the nodes. This means that all code in your
file will be executed, so anything that isn't a function or class must
be protected with an:

.. code:: python

  if __name__ == '__main__':

protecting statment.

If you do not do this you can end up with multi-submission and infinate
recursion, which could mess up your jobs or just crash the job, but either
way, it won't be good.

File Submission
---------------

If you want to just submit a file, that can be done like this:

.. code:: python

  from cluster import submit_file
  submit_file('/path/to/script', dependencies=[7, 9])

This will return the job number and will enter the job into the queue as dependant on jobs 007 and 009. The dependencies can be omitted.

****************
Queue Management
****************

This module provides simple queue management functions

To generate a queue object, do the following:

.. code:: python

  import cluster
  q = cluster.Queue(user='self')

This will give you a simple queue object containg a list of jobs that belong to you.
If you do not provide user, all jobs are included for all users. You can provide `qtype`
to explicitly force the queue object to contain jobs from one queing system (e.g. local
or torque).

To get a dictionary of all jobs, running jobs, queued jobs, and complete jobs, use:

.. code:: python

  q.jobs
  q.running
  q.complete
  q.queued

Every job has a number of attributes, including owner, nodes, cores, memory.

**************
Advanced Usage
**************

Profiles, Keywords, and the Config File
---------------------------------------

To make submission easier, this module defines a number of
keyword arguments in the options.py file that can be used
for all submission and Job() functions. These include things
like 'cores' and 'nodes' and 'mem'. To avoid having to set
these every time, the module sets a config file at
~/.python-cluster that defines profiles. These can be edited
directly in that file or through the config_file methods.

For example:

.. code:: python

  config_file.set_profile('small', {'nodes': 1, 'cores': 1,
                                    'mem': '2GB'})

To see all profiles run:

.. code:: python

  config_file.get_profile()

Other options are defined in the config file, including the
maximum number of jobs in the queue, the time to sleep between
submissions, and other options. To see these run:

.. code:: python

  config_file.get()

You can set options with:

.. code:: python

  config_file.set()

Feel free to alter the defaults in config_file.py and
options.py, they are clearly documented.

Job Files
---------

All jobs write out a job file before submission, even though
this is not necessary (or useful) with multiprocessing. In
local mode, this is a `.cluster` file, in slurm is is a
`.cluster.sbatch` and a `.cluster.script` file, in torque it is a
`.cluster.qsub` file. 'cluster' is set by the suffix keyword,
and can be overridden.

To change the directory these files are written to, use the
'filedir' keyword argument to Job or submit.

*NOTE:* This directory *must* be accessible to the compute nodes!!!

All jobs are assigned a name that is used to generate the
output files, including STDOUT and STDERR files. The default
name for the out files is STDOUT: name.cluster.out and
STDERR: name.cluster.err. These can be overwridden with
keyword arguments.

Dependecy Tracking
------------------

Dependency tracking is supported in all modes. Local mode uses
a unique queueing system that works similarly to torque and
slurm and which is defined in jobqueue.py.

To use dependency tracking in any mode pass a list of job ids
to submit or submit_file with the `dependencies` keyword
argument.

Logging
-------

I use a custion logging script called logme to log errors. To
get verbose output, set logme.MIN_LEVEL to 'debug'. To reduce
output, set logme.MIN_LEVEL to 'warn'.

*************
Code Overview
*************

There are two important classes for interaction with the batch
system: Job and Queue. The essential flow of a job submission
is:

.. code:: python

  job = Job(command/function, arguments, name)
  job.write()  # Writes the job submission files
  job.submit() # Submits the job
  job.wait()   # Waits for the job to complete
  job.stdout   # Prints the output from the job
  job.clean()  # Delete all of the files written

You can also wait for many jobs with the Queue class:

.. code:: python

  q = Queue(user='self')
  q.wait([job1, job2])

The jobs in this case can be either a Job class or a job
number.
