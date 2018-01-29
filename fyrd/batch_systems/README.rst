Adding Batch Systems
====================

Fyrd is intended to be fully modular, meaning anyone should be able to
implement support for any batch system, even other remote submission systems
like DistributedPython *if* they are able to define the following functions and
options.

To add a new batch system, you will need to:

1. Edit `__init__.py` to:

   1. Update `DEFINED_SYSTEMS` to include your batch system
   2. Edit `get_cluster_environment()` to detect your batch system, this function
      is ordered, meaning that it checks for slurm before torque, as slurm
      implements torque aliases. You should add a sensible way of detecting your
      batch system here.

2. Create a file in this directory with the name of your batch system (must match
   the name in `DEFINED_SYSTEMS`). This file must contain all constants and functions
   described below in the `Batch Script <#Batch_Script>`_ section.
3. Edit `options.py` as described below in the `Options <#Options>`_ section.
4. Run the pyenv test suite on your cluster system and make sure all tests pass
   on all versions of python supported by fyrd on your cluster system.
5. Optionally add a buildkite script on your cluster to allow CI testing. Note,
   this will technically give anyone with push privileges (i.e. me) the ability
   to execute code on your server. I promise to do no evil, but I can understand
   a degree of uncertainty regarding that. However, using buildkite will allow us
   to make sure that future updates don't break support for your batch system.
6. Become a fyrd maintainer! I always need help, if you want to contribute more,
   please do :-)

Options
-------

Fyrd works primarily by converting batch system arguments (e.g. `--queue`
for torque and `--partition` for slurm) into python keyword arguments. This is
done by creating dictionaries in the `fyrd/batch_systems/options.py` file.

Option parsing is done on job creation by calling the
`options.options_to_string()` function on the user provided keyword arguments.
The primary point of this function is to convert all keyword arguments to
string forms that can go at the top of your batch file prior to cluster
submission. Therefore you *must* edit the dictionaries in `options.py` to
include your batch system definitions. The most important section to edit is
`CLUSTER_CORE`, this dictionary has sections for each batch system, e.g. for
walltime::

    ('time',
     {'help': 'Walltime in HH:MM:SS',
      'default': '12:00:00', 'type': str,
      'slurm': '--time={}', 'torque': '-l walltime={}'}),

This auto-converts the time argument provided by the user into `--time` for slurm
and `-l walltime=` for torque.

As all systems are a little different, `options.options_to_string()` first
calls the `parse_strange_options()` function in the batch system definition
script to allow you the option to manually parse all options that cannot be
handled so simply. Hopefully this function will do nothing, but return the input,
but in some cases it makes sense for this function to handle every argument, an
obvious example is when running using something like `multiprocessing` instead
of a true batch system.

Batch Script
............

The defined batch script must have the name of your system and must define the
following constants and functions in exactly the way described below. Your
functions can do anything you want, and you can have extra functions in your
file (maybe make them private with a leading `_` in the name), but the primary
functions must take exactly the same arguments as those described below, and
provide exactly the same return values.

Constants
.........

- `PREFIX`: The string that will go before options at the top of a script file,
  could be blank for simple shell scripts, for slurm is is `'#SBATCH'`

Functions
.........

queue_test(warn=True)
~~~~~~~~~~~~~~~~~~~~~

Input:

- warn: bool, warn on failure, optional

Output:

- functional: bool, True if this system can be used

Description:

Use this function to write code to test that your system can function. If you are
using a specific command line tool in your code, consider adding it to the config
file to allow users to specify an absolute path or alternate name.

Use a combination of `_run.which()` (which returns a full path to an executable if
the executable is in the user's `PATH` and is executable) and `_run.is_exe()` (which
tests if a file is executable) to check your command line tools.

Use the warn parameter with `_logme.log()` to set a log level, e.g.:

.. code:: python

    log_level = 'error' if warn else 'debug'
    _logme.log('Cannot use me :-(', log_level)

Try not to raise any `Exceptions`, instead try to just log the problem and return
`False`.

This code is run very frequently to test that the queue is usable, so make your code
as simple and efficient as possible.


normalize_job_id(job_id)
~~~~~~~~~~~~~~~~~~~~~~~~

Input:

- job_id: string, return value from job submission

Output:

- job_id: string, a normalized job id
- array_id: string or None, a normalized array job id

Description:

Take a string returned by your job submission script (e.g. `qsub`) and turn it
into a normalized (hopefully string version of an int) job ID or process ID and
an array_id, if that is implemented by your system. The array_id can be None if
not implemented and should be None if not present (i.e. the job is not an array
job).

normalize_state(state)
~~~~~~~~~~~~~~~~~~~~~~

Input:

- state: string, a state description from the queue, e.g. 'running', or 'R'

Output:

- state: string, a state normalized into one of:
  -  'completed',
  -  'completing'
  -  'held'
  -  'pending'
  -  'running'
  -  'suspended'
  -  'running'
  -  'suspended'

gen_scripts(job_object, command, args, precmd, modstr)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Input:

- job_object: Job, a `fyrd.job.Job` object for the current job
- command: string, a string of the command to be run
- args: any additional arguments that are to be submitted, generally not used
- precmd: string, the batch system directives created by `options_to_string`,
  you can edit this or overwrite it if necessary
- modstr: string, a string of module imports (e.g. module load samtools) set by
  the user

Output:

- submission_script: `fyrd.submission_scripts.Script` object with the script to
  run
- exec_script: `fyrd.submission_scripts.Script` object with an additional script
  called by submission script if necessary, can be None

Description:

This is one of the more complex functions, but essentially you are going to just
format the `fyrd.script_runners.CMND_RUNNER_TRACK` script using the objects in the
inputs. This just makes an executable submission script, so you can build this
anyway you want, you don't have to use the `CMND_RUNNER_TRACK` script. However,
if you make your own script, the STDOUT must include timestamps like this::

    date +'%y-%m-%d-%H:%M:%S'
    echo "Running {name}"
    {command}
    exitcode=$?
    echo Done
    date +'%y-%m-%d-%H:%M:%S'
    if [[ $exitcode != 0 ]]; then
        echo Exited with code: $exitcode >&2
    fi
    exit $exitcode

This is because we parse the first two and last 2/3 lines of the file to get the
job runtimes and exit codes.

Here is an example function:

.. code:: python

   def gen_scripts(job_object, command, args, precmd, modstr):
   """Create script object for job, does not create a sep. exec script."""
   scrpt = _os.path.join(job_object.scriptpath,
                         '{}.cluster.qsub'.format(job_object.name))

   sub_script = _scrpts.CMND_RUNNER_TRACK.format(
       precmd=precmd, usedir=job_object.runpath, name=job_object.name,
       command=command
   )
   return _Script(script=sub_script, file_name=scrpt), None
 
submit(file_name, dependencies=None, job=None, args=None, kwds=None)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Input:

- file_name: string, The path to the file to execute [required]
- dependencies: list, A list of dependencies (job objects or job numbers)
  [optional]
- job: fyrd.job.Job, A job object of the calling job (not always passed)
  [optional]
- args: list, A list of additional arguments (currently unused) [optional]
- kwargs: dict or str, A dictionary or string of 'arg:val,arg,arg:val,...'
  (currently unused) [optional]

Output:

- job_id: string, A job number

Description:

This function must actually submit the job file, however you want it to. If
possible, include dependency tracking, if that isn't possible, raise a
NotImplemented Exception. You can make use of `fyrd.run.cmd`, which allows you
to execute code directly on the terminal and can catch errors and retry submission
however many times you choose (5 is a good number). It also returns the exit_code,
STDOUT, and STDERR for the execution.

The job object is passed whenever a job is submitted using the normal
submission process, and will contain all keyword arguments. If your batch
system requires command line arguments, you can parse the keyword arguments
with the `parse_strange_options` function and store them in the `submit_args`
attribute of the Job object. You can then access that attribute in this
submission function and pass them to `fyrd.run.cmd` (or any other method you
choose) as command line arguments.

Note, this submit function can also be called on existing scripts without a job
object, so your function *should not require* the job object. The args and kwds
arguments exist to allow additional parsing, although they are currently
unused; right now args gets the contents of Job.submit_args and kwds gets the
contents of the `additional_keywords` argument to Job.submit(). This argument
is currently ignored by all batch scripts.

Please add as much error catching code as possible in the submit function, the
`torque.py` example is a good one.

kill(job_ids)
~~~~~~~~~~~~~

Input:

- job_ids: list, A list of job numbers

Output:

- bool: True on success, False on failure

Immediately terminate the running jobs

queue_parser(user=None, partition=None)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Input:

- user: string, optional username to limit to
- partition: string, optional partition/queue to limit to

(Fine to ignore these arguments if they are not implemented on your system)

Yields (must be an iterator):

- job_id: string
- array_id: string, optional array job number
- name: string, a name for the job
- userid: string, user of the job (can be None)
- partition: string, partition running in (can be None)
- state: string a slurm-style string representation of the state
- nodelist: list, the nodes the job is running on
- numnodes: int, a count of the number of nodes
- threads_per_node: int, a count of the number of cores being used on each node
- exit_code: int, an exit_code (can be None if not exited yet) **Must** be an int
  if state == 'completed'. **must** be 0 if job completed successfully.

Description:

This is the iterator that is the core of the batch system definition. You must
somehow be able to parse all of the currently running jobs and return the above
information about every job. *If your batch system implements array jobs, this
generator must yield one entry per array child, not parent job*.

parse_strange_options(option_dict)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Inputs:

- option_dict: dictionary, a dictionary of keywords from the `options.py` file
  prior to interpretation with `option_to_string`, allowing parsing of all
  unusual keywords.

Outputs:

- outlist: list, A list of **strings** that will be added to the top of the submit
  file
- option_dict: dictionary, A parsed version of option_dict with **all options not
  defined in the appropriate dictionaries in `options.py` removed**.
- other_args: a list of parsed arguments to be passed at submit time, this will
  be added to the `submit_args` attribute of the Job or passed as the `args`
  argument to `submit`.

Summary
-------

The modularity of this system is intended to make it easy to support any batch
system, however it is possible that some systems won't fit into the mold defined
here. If that is the case, feel free to alter other parts of the code to make it
work, but **be sure that all tests run successfully on every defined cluster on
every supported version of python**. Feel free to reach out to me to request
testing if you do not have access to any system.

