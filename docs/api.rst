API Documentation
=================

The following documentation is primarily built from the docstrings of the actual
source code and can be considered an API reference.

Queueing
--------

The most import thing is the `Queue()` class which does most of the queue
mangement. In addition, `get_cluster_environment()` attempts to autodetect the
cluster type (torque, slurm, normal) and sets the global cluster type for the
whole file. Finally, the `wait()` function accepts a list of jobs and will block
until those jobs are complete.

.. autoclass:: cluster.Queue
   :members:

.. autofunction:: cluster.queue.get_cluster_environment

.. autofunction:: cluster.queue.wait 

.. autofunction:: cluster.queue.check_queue 

Job Management
--------------

Job management is handeled by the `Job()` class, full instructions on using this
class are above, in particular review the 'Keyword Arguments' section above.

The methods of this class are exposed by a few functions that aim to make job
submission easier. The foremost of these is `submit()` which can take as little
as a single command and execute it. `make_job()` and `make_job_file()` work
similarly but just return a Job object, or write the file and then return the
Job object respectively. `clean()` takes a list of Job objects and runs their
internal `clean()` methods, deleting all written files.

There are two additional functions that are completely independent of the Job
object: `submit_file()` and `clean_dir()`. `submit_file()` uses similar methods
to the Job class to submit a job to the cluster, but it does not involve the job
class at all, instead just submitting an already created job file. It can do
dependency tracking in the same way as a job file, but that is all.
`clean_dir()` uses the file naming convention established in the Job class (and
defined separately here) to delete all files in a directory that look like they
could be made by this module. It has an autoconfirm feature that can be
activated to avoid accidental clobbering.

.. autoclass:: cluster.Job
   :members:

.. autofunction:: cluster.submit

.. autofunction:: cluster.job.submit_file

.. autofunction:: cluster.job.make_job

.. autofunction:: cluster.job.make_job_file

.. autofunction:: cluster.job.clean

.. autofunction:: cluster.job.clean_dir

Options
-------

All keyword arguments are defined in dictionaries in the `options.py` file,
alongside function to manage those dictionaries. Of particular importance is
`option_help()`, which can display all of the keyword arguments as a string or a
table. `check_arguments()` checks a dictionary to make sure that the arguments
are allowed (i.e. definied), it is called on all keyword arguments in the
package.

The way that option handling works in general, is that all hardcoded keyword
arguments must contain a dictionary entry for 'torque' and 'slurm', as well as a
type declaration. If the type is NoneType, then the option is assumed to be a
boolean option. If it has a type though, `check_argument()` attmepts to cast the
type and specific idiosyncracies are handled in this step, e.g. memory is converted
into an integer of MB. Once the arguments are sanitized `format()` is called on
the string held in either the 'torque' or the 'slurm' values, and the formatted
string is then used as an option. If the type is a list/tuple, the 'sjoin' and
'tjoin' dictionary keys must exist, and are used to handle joining.

The following two functions are used to manage this formatting step.

`option_to_string()` will take an option/value pair and return an appropriate
string that can be used in the current queue mode. If the option is not
implemented in the current mode, a debug message is printed to the console and
an empty string is returned.

`options_to_string()` is a wrapper around `option_to_string()` and can handle a
whole dictionary of arguments, it explicitly handle arguments that cannot be
managed using a simple string format.

.. autofunction:: cluster.options.option_help

.. autofunction:: cluster.options.check_arguments

.. autofunction:: cluster.options.options_to_string

.. autofunction:: cluster.options.option_to_string

Config File
-----------

Profiles are combinations of keyword arguments that can be called in any of the
submission functions. They are handled in the `config_file.py` file which just
adds an abstraction layer on top of the builtin python ConfigParser script.

The config file also contains other options that can be managed with the `get()`
and `set()` functions. Profiles are wrapped in a `Profile()` class to make
attribute access easy, but they are fundamentally just dictionaries of keyword
arguments. They can be created with `cluster.config_file.Profile({kewywds})` and
then written to a file with that class' `write()` method. The easiest way to
interact with profiles is with the `get_profile()` and `set_profile()`
functions. These make it very easy to go from a dictionary of keywords to a
profile.

Profiles can then be called with the `profile=` keyword in any submission
function or Job class.

.. autoclass:: cluster.config_file.Profile
   :members:

.. autofunction:: cluster.config_file.get

.. autofunction:: cluster.config_file.set

.. autofunction:: cluster.config_file.set_profile

.. autofunction:: cluster.config_file.get_profile

.. autofunction:: cluster.config_file.delete

.. autofunction:: cluster.config_file.get_config


Local Queue Implementation
--------------------------

The local queue implementation is based on the multiprocessing library and is
not intended to be used directly, it should always be used via the Job class
because it is somewhat tempramental. The essential idea behind it is that we can
have one JobQueue class that is bound to the parent process, it exclusively
manages a single child thread that runs the `job_runner()` function. The two
process communicate using a `multiprocessing.Queue` object, and pass
`cluster.jobqueue.Job` objects back and forth between them.

The Job objects (different from the Job objects in `job.py`) contain information
about the task to run, including the number of cores required. The job runner
manages a pool of `multiprocessing.Pool` tasks directly, and keeps the total
running cores below the total allowed (default is the system max, can be set
with the threads keyword). It backfills smaller jobs and holds on to larger jobs
until there is enough space free.

This is close to what torque and slurm do, but vastly more crude. It serves as a
stopgap to allow parallel software written for compute clusters to run on a
single machine in a similar fashion, without the need for a pipeline alteration.
The reason I have reimplemented a process pool is that I need dependency
tracking and I need to allow some processes to run on multiple cores (e.g. 6 of
the available 24 on the machine).

The `job_runner()` and `Job` objects should never be accessed except by the
JobQueue. Only one JobQueue should run at a time (not enforced), and by default
it is bound to `cluster.jobqueue.JQUEUE`. That is the interface used by all
other parts of this package.

.. autoclass:: cluster.jobqueue.JobQueue
   :members:

.. autoclass:: cluster.jobqueue.Job
   :members:

.. autofunction:: cluster.jobqueue.job_runner

Logme
-----

This is a package I wrote myself and keep using because I like it. It provides
syslog style leveled logging (e.g. 'debug'->'info'->'warn'->'error'->'critical')
and it implements colors and timestamped messages.

The minimum print level can be set module wide at runtime by changing
`cluster.logme.MIN_LEVEL`.

.. autofunction:: cluster.logme.log

Other Functions
---------------

Some other wrapper functions are defined in `run.py`, these are just little
useful knick-knacks that make function submission and queue management possible.

.. autofunction:: cluster.run.cmd

.. autofunction:: cluster.run.which

.. autofunction:: cluster.run.open_zipped

.. autofunction:: cluster.run.split_file

Indices and tables
------------------

* :ref:`genindex`
* :ref:`search`
