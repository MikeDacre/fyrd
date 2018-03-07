API Reference
=============

fyrd.queue
----------

The core class in this file is the ``Queue()`` class which does most of the queue
management. In addition, ``get_cluster_environment()`` attempts to autodetect the
cluster type (*torque*, *slurm*, *normal*) and sets the global cluster type for
the whole file. Finally, the ``wait()`` function accepts a list of jobs and will
block until those jobs are complete.

The Queue class relies on a few simple queue parsers defined by the
``torque_queue_parser`` and ``slurm_queue_parser`` functions. These call ``qstat -x``
or ``squeue`` and ``sacct`` to get job information, and yield a simple tuple of
that data with the following members::

  job_id, name, userid, partition, state, node-list, node-count, cpu-per-node, exit-code

The Queue class then converts this information into a ``Queue.QueueJob`` object and
adds it to the internal ``jobs`` dictionary within the Queue class. This list is
now the basis for all of the other functionality encoded by the Queue class. It
can be accessed directly, or sliced by accessing the ``completed``, ``queued``, and
``running`` attributes of the Queue class, these are used to simply divide up the
jobs dictionary to make finding information easy.

fyrd.queue.Queue
................

.. autoclass:: fyrd.queue.Queue
   :show-inheritance:

Methods
~~~~~~~

.. automethod:: fyrd.queue.Queue.wait

.. automethod:: fyrd.queue.Queue.get

.. automethod:: fyrd.queue.Queue.wait_to_submit

.. automethod:: fyrd.queue.Queue.test_job_in_queue

.. automethod:: fyrd.queue.Queue.get_jobs

.. automethod:: fyrd.queue.Queue.get_user_jobs

.. automethod:: fyrd.queue.Queue.update

.. automethod:: fyrd.queue.Queue.check_dependencies

fyrd.queue Jobs
................

Hold information about individual jobs, ``QueueJob`` about primary jobs,
``QueueChild`` about individual array jobs (which are stored in the ``children``
attribute of ``QueueJob`` objects.

.. autoclass:: fyrd.queue.QueueJob

.. autoclass:: fyrd.queue.QueueChild

fyrd.queue.QueueError
.....................

.. autoexception:: fyrd.queue.QueueError


fyrd.job
--------

Job management is handled by the ``Job()`` class. This is a very large class
that defines all the methods required to build and submit a job to the cluster.

It accepts keyword arguments defined in `fyrd.options <#fyrd.options>`_ on
initialization, which are then fleshed out using profile information from the
config files defined by `fyrd.conf <#fyrd.conf>`_.

The primary argument on initialization is the function or script to submit.

Examples:

.. code:: python

   Job('ls -lah | grep myfile')
   Job(print, ('hi',))
   Job('echo hostname', profile='tiny')
   Job(huge_function, args=(1,2) kwargs={'hi': 'there'},
       profile='long', cores=28, mem='200GB')

fyrd.job.Job
............

.. autoclass:: fyrd.Job
   :show-inheritance:

Methods
~~~~~~~

.. automethod:: fyrd.job.Job.initialize

.. automethod:: fyrd.job.Job.gen_scripts

.. automethod:: fyrd.job.Job.write

.. automethod:: fyrd.job.Job.clean

.. automethod:: fyrd.job.Job.scrub

.. automethod:: fyrd.job.Job.submit

.. automethod:: fyrd.job.Job.resubmit

.. automethod:: fyrd.job.Job.get_keywords

.. automethod:: fyrd.job.Job.set_keywords

.. automethod:: fyrd.job.Job.wait

.. automethod:: fyrd.job.Job.get

.. automethod:: fyrd.job.Job.get_output

.. automethod:: fyrd.job.Job.get_stdout

.. automethod:: fyrd.job.Job.get_stderr

.. automethod:: fyrd.job.Job.get_times

.. automethod:: fyrd.job.Job.get_exitcode

.. automethod:: fyrd.job.Job.update

.. automethod:: fyrd.job.Job.update_queue_info

.. automethod:: fyrd.job.Job.fetch_outputs


fyrd.submission_scripts
-----------------------

This module defines to classes that are used to build the actual jobs for submission,
including writing the files. ``Function`` is actually a child class of ``Script``.

.. autoclass:: fyrd.submission_scripts.Script
   :members:
   :show-inheritance:

.. autoclass:: fyrd.submission_scripts.Function
   :members:
   :show-inheritance:

fyrd.batch_systems
------------------

All batch systems are defined here.

fyrd.batch_systems functions
............................

.. autofunction:: fyrd.batch_systems.get_cluster_environment

.. autofunction:: fyrd.batch_systems.check_queue

.. autofunction:: fyrd.batch_systems.get_batch_system

fyrd.batch_systems.options
..........................

All `keyword arguments </keywords.html>`_ are defined in dictionaries in the
``options.py`` file, alongside function to manage those dictionaries. Of
particular importance is ``option_help()``, which can display all of the keyword
arguments as a string or a table. ``check_arguments()`` checks a dictionary to
make sure that the arguments are allowed (i.e. defined), it is called on all
keyword arguments in the package.

To see keywords, run ``fyrd keywords`` from the console or ``fyrd.option_help()``
from a python session.

The way that option handling works in general, is that all hard-coded keyword
arguments must contain a dictionary entry for 'torque' and 'slurm', as well as a
type declaration. If the type is NoneType, then the option is assumed to be a
boolean option. If it has a type though, ``check_argument()`` attempts to cast the
type and specific idiosyncrasies are handled in this step, e.g. memory is converted
into an integer of MB. Once the arguments are sanitized ``format()`` is called on
the string held in either the 'torque' or the 'slurm' values, and the formatted
string is then used as an option. If the type is a list/tuple, the 'sjoin' and
'tjoin' dictionary keys must exist, and are used to handle joining.

The following two functions are used to manage this formatting step.

``option_to_string()`` will take an option/value pair and return an appropriate
string that can be used in the current queue mode. If the option is not
implemented in the current mode, a debug message is printed to the console and
an empty string is returned.

``options_to_string()`` is a wrapper around ``option_to_string()`` and can handle a
whole dictionary of arguments, it explicitly handle arguments that cannot be
managed using a simple string format.

.. autofunction:: fyrd.batch_systems.options.option_help

.. autofunction:: fyrd.batch_systems.options.sanitize_arguments

.. autofunction:: fyrd.batch_systems.options.split_keywords

.. autofunction:: fyrd.batch_systems.options.check_arguments

.. autofunction:: fyrd.batch_systems.options.options_to_string

.. autofunction:: fyrd.batch_systems.options.option_to_string

fyrd.conf
---------

``fyrd.conf`` handles the config (``~/.fyrd/config.txt``) file and the profiles
(``~/.fyrd/profiles.txt``) file.

`Profiles </basic_usage.html#profiles>`_ are combinations of keyword arguments
that can be called in any of the submission functions. Both the config and profiles
are just `ConfigParser <https://docs.python.org/3/library/configparser.html>`_
objects, ``conf.py`` merely adds an abstraction layer on top of this to maintain
the integrity of the files.

config
......

The config has three sections (and no defaults):

- queue    —  sets options for handling the queue
- jobs     —  sets options for submitting jobs
- jobqueue —  local option handling, will be removed in the future

For a complete reference, see the config documentation :
`Configuration </configuration.html>`_

Options can be managed with the ``get_option()`` and ``set_option()`` functions, but
it is actually easier to use the console script::

    fyrd conf list
    fyrd conf edit max_jobs 3000

.. autofunction:: fyrd.conf.get_option

.. autofunction:: fyrd.conf.set_option

.. autofunction:: fyrd.conf.delete

.. autofunction:: fyrd.conf.load_config

.. autofunction:: fyrd.conf.write_config

.. autofunction:: fyrd.conf.create_config

.. autofunction:: fyrd.conf.create_config_interactive

profiles
........

Profiles are wrapped in a ``Profile()`` class to make attribute access easy, but
they are fundamentally just dictionaries of keyword arguments. They can be
created with ``cluster.conf.Profile(name, {keywds})`` and then written to a file
with the ``write()`` method.

The easiest way to interact with profiles is not with class but with the
``get_profile()``, ``set_profile()``, and ``del_profile()`` functions. These make it
very easy to go from a dictionary of keywords to a profile.

Profiles can then be called with the ``profile=`` keyword in any submission
function or Job class.

As with the config, profile management is the easiest and most stable when using
the console script::

  fyrd profile list
  fyrd profile add very_long walltime:120:00:00
  fyrd profile edit default partition:normal cores:4 mem:10GB
  fyrd profile delete small

fyrd.conf.Profile
~~~~~~~~~~~~~~~~~

.. autoclass:: fyrd.conf.Profile
   :members:
   :show-inheritance:

.. autofunction:: fyrd.conf.set_profile

.. autofunction:: fyrd.conf.get_profile

fyrd.helpers
------------

The helpers are all high level functions that are not required for the library
but make difficult jobs easy to assist in the goal of trivially easy cluster
submission.

The functions in `fyrd.basic <#fyrd.basic>`_ below are different in that they
provide simple job submission and management, while the functions in
``fyrd.helpers`` allow the submission of many jobs.

.. autofunction:: fyrd.helpers.jobify

.. autofunction:: fyrd.helpers.parapply

.. autofunction:: fyrd.helpers.parapply_summary

.. autofunction:: fyrd.helpers.splitrun

fyrd.basic
----------

This module holds high level functions to make job submission easy, allowing the user
to skip multiple steps and to avoid using the ``Job`` class directly.

``submit()``, ``make_job()``, and ``make_job_file()`` all create ``Job`` objects in the
background and allow users to submit jobs. All of these functions accept the exact
same arguments as the ``Job`` class does, and all of them return a ``Job`` object.

``submit_file()`` is different, it simply submits a pre-formed job file, either one that
has been written by this software or by any other method. The function makes no attempt
to fix arguments to allow submission on multiple clusters, it just submits the file.

``clean()`` takes a list of job objects and runs the ``clean()`` method on all of them,
``clean_dir()`` uses known directory and suffix information to clean out all job files
from any directory.

.. autofunction:: fyrd.basic.submit()

.. autofunction:: fyrd.basic.make_job()

.. autofunction:: fyrd.basic.make_job_file()

.. autofunction:: fyrd.basic.submit_file()

.. autofunction:: fyrd.basic.clean()

.. autofunction:: fyrd.basic.clean_dir()


fyrd.run
--------

.. automodule:: fyrd.run
   :members:
   :show-inheritance:

fyrd.logme
----------

This is a package I wrote myself and keep using because I like it. It provides
syslog style leveled logging (e.g. 'debug'->'info'->'warn'->'error'->'critical')
and it implements colors and timestamped messages.

The minimum print level can be set module wide at runtime by changing
``cluster.logme.MIN_LEVEL``.

.. autofunction:: fyrd.logme.log

.. automodule:: fyrd.logme
   :members:
   :show-inheritance:
