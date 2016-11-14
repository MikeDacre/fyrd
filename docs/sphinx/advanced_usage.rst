Advanced Usage
==============

Most of the important functionality is covered in the
`Getting Started </basic_usage.html>`_ section, and full details on the library
are available in the `API Reference </api.html>`_ section. This section just
provides some extra information on Job and Queue management, and importantly
introduces some of the higher-level options available through the
`helpers </api.html#fyrd-helpers>`_.

The Job Class
-------------

The core of this submission system is the `Job` class, this class builds a job
using keyword arguments and profile parsing. The bulk of this is done at class
initialization and is covered in the getting started section of this
documentation and on job submission with the `submit()` method. There are
several other features of this class to be aware of though.

Script File Handling
....................

Torque and slurm both require submission scripts to work. In the future these
will be stored by fyrd in a database and submitted from memory, but for now
they are written to disk.

The creation and writing of these scripts is handled by the
`Script <api.html#fyrd.submission_scripts.Script>`_ and
`Function <api.html#fyrd.submission_scripts.Function>`_ classes in the
`fyrd.submission_scripts <api.html#fyrd-submission-scripts>`_ module.
These classes pass keywords to the
`options_to_string() </api.html#fyrd.options.options_to_string>`_ function
of the options method, which converts them into a submission string compatible
with the active cluster. These are then written to a script for submission
to the cluster.

The `Function` class has some additional functionality to allow easy submission
of functions to the cluster. It tries to build a list of all possible modules
that the function could need and adds import statements to all of them to the
function submission script. It then pickles the submitted function and
arguments to a pickle file on the disk, and writes a python script to the same
directory.

This python script unpickles the function and arguments and runs them, pickling
either the result or and exception, if one is raised, to the disc on completion.
The submission script calls this python script on the cluster nodes.

The script and output files are written to the path defined by the `.filepath`
attribute of the `Job` class, which is set using the 'filepath' keyword
argument. If not set, this directory defaults to the directory set in the
filepath section of the `config </configuration.html>`_ file or the current working
directory. Note that this path is independent of the `.runpath` attibute, which
is where the code will actually run, and also defaults to the current working
directory.

Job Output Handling and Retrieval
.................................

The correct way to get outputs from within a python session is to call the
`.get()` method of the `Job` class. This first calls the `.wait()` method, which
blocks until job completion, and then the `.fetch_outputs()` method which
*calls get_output, get_stdout, and get_stderr, which save all function outputs,
STDOUT, and STDERR to the class*. This means that outputs can be accessed using
the following `Job` class attributes:

- `.output` —  the function output for functions or STDOUT for scripts
- `.stdout` —  the STDOUT for the script submission (always present)
- `.stderr` —  the STDERR for the script submission (always present)

This makes job output retrieval very easy, but it is sometimes not what you want,
particularly if outputs are very large (they get loaded into memory).

The `wait()` method will not save any outputs. In addition `get()` can be
with the `save=False` argument, which means it will fetch the output (or STDOUT)
only, but will not write them to the class itself.

**Note**: By default, `get()` also deletes all script and output files. This
is generally a good thing as it keeps the working directory clean, but it isn't
always what you want. To prevent outputs from being deleted, pass
`delete_outfiles=False` to `get()`, or alternatively set the `.clean_outputs`
attribute to `False` prior to running `get()`. To prevent the cleaning of
any files, including the script files, pass `cleanup=False` or set
`.clean_files` to `False`.

`clean_files` and `clean_outputs` can also be set globally in the config file.


Job Files
---------

All jobs write out a job file before submission, even though this is not
necessary (or useful) with multiprocessing. This will change in a future
version.

To ensure files are obviously produced by this package and that files are unique
the file format is name.number.random_string.suffix.extension. These are:

name:          Defined by the `name=` argument or guessed from the function/script
number:        A number count of the total jobs with the same name already queued
random_string: An 8-character random string
suffix:        A string defined in the config file, default 'cluster'
extension:     An obvious extension such as '.sbatch' or '.qsub'

To change the directory these files are written to, set the filedir item in the
config file or use the 'filedir' keyword argument to Job or submit.

*NOTE:* This directory *must* be accessible to the compute nodes!!!

It is sometimes useful to set the filedir setting in the config to a single directory
accessible cluster-wide. This avoids cluttering the current directory, particularly
as outputs can be retrieved so easily from within python. If you are going to do
this set the 'clean_files' and 'clean_outfiles' arguments in the config file to
avoid cluttering the directory.

All Job objects have a ``clean()`` method that will delete any left over files.
In addition there is a clean_job_files script that will delete all files made by
this package in any given directory. Be very careful with the script though, it
can clobber a lot of work all at once if it is used wrong.

Helpers
-------

The `fyrd.helpers </api.html#fyrd-helpers>`_ module defines several simple
functions that allow more complex job handling.

The helpers are all high level functions that are not required for the library
but make difficult jobs easy to assist in the goal of trivially easy cluster
submission.

The most important function in `fyrd.helpers` is `parapply()`, which allows the
user to submit a `pandas.DataFrame.apply` method to the cluster in parallel by
splitting the DataFrame, submitting jobs, and then recombining the DataFrame at
the end, all without leaving any temp files behind. e.g.:

.. code:: python

   df = pandas.read_csv('my_huge_file.txt')
   df = fyrd.helpers.parapply(100, df, long_running_function, profile='fast')

That command will split the dataframe into 100 pieces, submit each to the
cluster as a different job with the profile 'fast', and then recombine them
into a single DataFrame again at the end.

`parapply_summary` behaves similarly but assumes that the function summarizes the data
rather than returning a DataFrame of the same size. It thus runs the function on the
resulting DataFrame also, allowing all dfs to be merged. e.g.:

.. code:: python

   df = fyrd.helpers.parapply_summary(df, numpy.mean)

This will return just the mean of all the numeric columns, `parapply` would return a
DataFrame with duplicates for every submitted job.


Queue Management
----------------

Queue handling is done by the `Queue </api.html#fyrd-queue-queue>`_ class in
the `fyrd.queue </api.html#fyrd-queue>`_ module. This class calls the
`fyrd.queue.queue_parser </api.html#fyrd.queue.queue_parser>`_ iterator which
in turn calls either
`fyrd.queue.torque_queue_parser </api.html#fyrd.queue.torque_queue_parser>`_ or
`fyrd.queue.slurm_queue_parser </api.html#fyrd.queue.slurm_queue_parser>`_
depending on the detected cluster environment (set by `fyrd.queue.QUEUE_MODE`
and overridden by the 'queue_type' config option if desired (not necessary,
queue type is auto-detected)).

These iterators return the following information from the queue::

  job_id, name, userid, partition, state, node-list, node-count, cpu-per-node, exit-code

These pieces of information are used to create QueueJob objects for every
job, which are stored in the `Queue.jobs` attribute (a dictionary). The `Queue`
class provides several properties, attributes, and methods to allow easy
filtering of these jobs.

Most important is the `QueueJob.state` attribute, which holds information on
the current state of that job. To get a list of all states in the queue, call
the `Queue.job_states` property, which will return a list of states in the queue.
All of these states are also attributes of the `Queue` class, for example::

  fyrd.Queue.completed

returns all completed jobs in the queue as a dictionary (a filtered copy of the
`.jobs` attribute).

**Note**: torque states are auto-converted to slurm states, as slurm states
are easier to read. e.g. 'C' becomes 'completed'.

The most useful method of `Queue` is `wait()`, it will take a list of job numbers
or `Job` objects and wait until all of them are complete. This method is called
by the `Job.wait()` method, and can be called directly to wait for an arbitrary
number of jobs.

To wait for all jobs from a given user, you can do this:

.. code:: python

   q = fyrd.Queue()
   q.wait(q.get_user_jobs(['bob', 'fred']))

This task can also be accomplished with the console application:

.. code:: shell

   fyrd wait <job> [<job>...]
   fyrd wait -u bob fred

The method can actually be simply accessed as a function instead of needing
the `Queue` class:

.. code:: python
   fyrd.wait([1,2,3])

To generate a `Queue` object, do the following:

.. code:: python

  import fyrd
  q = fyrd.Queue(user='self')

This will give you a simple queue object containg a list of jobs that belong to
you.  If you do not provide user, all jobs are included for all users. You can
provide `qtype` to explicitly force the queue object to contain jobs from one
queing system (e.g. local or torque).

To get a dictionary of all jobs, running jobs, queued jobs, and complete jobs,
use:

.. code:: python

  q.jobs
  q.running
  q.complete
  q.queued

Every job is a `QueueJob` class and has a number of attributes, including
owner, nodes, cores, memory.

Config
------

Many of the important options used by this software are set in a config file
and can be managed on the console with `fyrd conf ...`.

For full information see the `Configuration </configuration.html>`_ section of
this documentation.


Logging
-------

I use a custion logging script called `logme </api.html#fyrd-logme>`_ to log
errors. To get verbose output, set `fyrd.logme.MIN_LEVEL` to 'debug' or
'verbose'. To reduce output, set logme.MIN_LEVEL to 'warn'.
