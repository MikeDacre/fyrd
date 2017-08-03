# -*- coding: utf-8 -*-
"""
Submit jobs to slurm or torque, or with multiprocessing.

 =============== ===================================================
         AUTHOR: Michael D Dacre, mike.dacre@gmail.com
   ORGANIZATION: Stanford University
        LICENSE: MIT License, property of Stanford, use as you wish
        VERSION: 0.6.1-beta.7
        CREATED: 2015-12-11 22:19
  Last modified: 2017-08-02 23:37
 =============== ===================================================

Allows simple job submission with *dependency tracking and queue waiting* with
either torque, slurm, or locally with the multiprocessing module. It uses
simple techiques to avoid overwhelming the queue and to catch bugs on the fly.

Setting Environment
-------------------

To set the environement, set queue.MODE to one of ['torque',
'slurm', 'local'], or run get_cluster_environment().

Simple Use
----------

At its simplest, this module can be used by just executing submit(<command>),
where command is a function or system command/shell script. The module will
autodetect the cluster, generate an intuitive name, run the job, and write all
outputs to files in the current directory. These can be cleaned with
clean_dir().

To run with dependency tracking, run::

    job  = submit(<command1>)
    job2 = submit(<command2>, dependencies=job1)

*NOTE:* To use this, the file that is calling submit must have all code inside
a function or protected by if __name__ == '__main__'.  In order to make
function submission work, the file that calls this script must be importable,
importing in python executes all code that isn't wrapped in an if __name__ ==
'__main__' clause. This can lead to infinite recursion!

Much more can be done though.

Class Overview
--------------

There are two important classes for interaction with the batch
system: Job and Queue. The essential flow of a job submission
is::

    job = Job(command/function, arguments, name)
    job.write()  # Writes the job submission files
    job.submit() # Submits the job
    job.wait()   # Waits for the job to complete
    job.stdout   # Prints the output from the job
    job.clean()  # Delete all of the files written

You can also wait for many jobs with the Queue class::

    q = Queue(user='self')
    q.wait([job1, job2])

The jobs in this case can be either a Job class or a job
number.

Profiles, Keywords, and the Config File
---------------------------------------

To make submission easier, this module defines a number of keyword arguments in
the options.py file that can be used for all submission and Job() functions.
These include things like 'cores' and 'nodes' and 'mem'. To avoid having to set
these every time, the module sets a config file at ~/.fyrd that
defines profiles. These can be edited directly in that file or through the
conf methods.

For example::

    conf.set_profile('small', {'nodes': 1, 'cores': 1,
                                      'mem': '2GB'})

To see all profiles run::

    conf.get_profiles()

Other options are defined in the config file, including the maximum number of
jobs in the queue, the time to sleep between submissions, and other options. To
see these run::

    conf.get_option()

You can set options with::

    conf.set_option()

Feel free to alter the defaults in conf.py and options.py, they are
clearly documented.

Job Files
---------

All jobs write out a job file before submission, even though this is not
necessary (or useful) with multiprocessing. In local mode, this is a .cluster
file, in slurm is is a .cluster.sbatch and a .cluster.script file, in torque it
is a .cluster.qsub file. 'cluster' is set by the suffix keyword, and can be
overridden.

To change the directory these files are written to, use the 'filedir' keyword
argument to Job or submit.  *NOTE:* This *must* be accessible to the compute
nodes!!!

All jobs are assigned a name that is used to generate the output files,
including STDOUT and STDERR files. The default name for the out files is
STDOUT: name.cluster.out and STDERR: name.cluster.err. These can be overwridden
with keyword arguments.

Dependecy Tracking
------------------

Dependency tracking is supported in all modes. Local mode uses a unique
queueing system that works similarly to torque and slurm and which is defined
in local.py.

To use dependency tracking in any mode pass a list of job ids to submit or
submit_file with the `dependencies` keyword argument.

Logging
-------

I use a custion logging script called logme to log errors. To get verbose
output, set logme.MIN_LEVEL to 'debug'. To reduce output, set logme.MIN_LEVEL
to 'warn'.

Help
----

Full help is available at::
    github.com/MikeDacre/fyrd
"""
import os as _os
import signal as _signal
import atexit as _atexit

# Version Number
__version__ = '0.6.1-beta.8'

#################################################
#  Currently configured job submission systems  #
#################################################

ALLOWED_MODES = ['local', 'torque', 'slurm']
# Current mode held in queue.MODE

###################
#  House Keeping  #
###################


class ClusterError(Exception):

    """A custom exception for cluster errors."""

    pass

#########################################
#  Make our functions easily available  #
#########################################

from . import local
from . import queue
from . import job
from . import conf
from . import options
from . import helpers
from .run import check_pid as _check_pid

from .queue import Queue
from .queue import wait
from .queue import check_queue
from .queue import get_cluster_environment

from .job import Job
from .basic import submit
from .basic import submit_file
from .basic import make_job_file
from .basic import clean
from .basic import clean_dir

from .conf import set_profile
from .conf import get_profile
from .conf import get_profiles

from .options import option_help

__all__ = ['Job', 'Queue', 'wait', 'submit', 'submit_file', 'make_job_file',
           'clean', 'clean_dir', 'check_queue', 'option_help', 'set_profile',
           'get_profile', 'get_profiles', 'helpers']

##########################
#  Set the cluster type  #
##########################

queue.MODE = get_cluster_environment()
check_queue()


###############################
#  Kill the JobQueue on exit  #
###############################
#  def _kill_local():
    #  if local.JQUEUE and _check_pid(local.JQUEUE.pid):
        #  local.JQUEUE.terminate()
    #  #  del(local.JQUEUE)

#  _atexit.register(_kill_local)
