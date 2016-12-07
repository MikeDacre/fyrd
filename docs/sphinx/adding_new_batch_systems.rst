Adding New Batch Systems
========================

To add a new batch system, a script describing that system must be added to the
`/batch` package, and keyword options must be added to the `options.py`
dictionaries. The new batch system must have entries for every record in the
`CLUSTER_CORE` and `CLUSTER_OPTS` dictionaries. Additionally, it is possible to
add options to additional dictionaries in that file, but as much as possible
all options should be added to just those two dictionaries.

If your batch system uses a different and non-compatible system for job submission
such the updating `options.py` is impossible, it is also fine to not alter `options.py` 
at all. `options.py` is called by the `format_script()` method of the `BatchSystem`
class you create anyway, you can just skip that. However, if it is possible to
use the `options.py` dictionaries, please do so. I will refuse pull requests that
do not use the `options.py` system if doing so would be possible.

Batch Queue Script
------------------

Every new batch system must have a script in `/batch` with a name that matches
an entry in `fyrd.batch.CLUSTERS`. This script must define the following constant:

- QUEUE_OUTPUT: A sample output of the queue command (e.g. `qstat`) to use for
                testing and documentation. It should contain just a few jobs
                with output representative of multiple job types (e.g. running,
                queued, cancelled, array jobs and simple jobs).
- STATUS_DICT:  

This script must also define a `BatchSystem` class that inherits from
`fyrd.batch.BatchSystem`. This class should define the following features (some
are optional, these are noted where appropriate).

Required Attributes
...................

- name:                Must match the name of the script and the name in `CLUSTERS`
- submit_cmnd:         A command that is used to submit scripts, such as `qsub`
                       or `sbatch`
- queue_cmnd:          A command that is used to get queue output
- arg_prefix:          A string to add before options in the script file, such
                       as `#SBATCH` for slurm or `#PBS` for torque
- identifying_scripts: A list of scripts that, when present on the PATH,
                       identify this queue. All must be present.

Optional Attributes
...................

- status_dict: A dictionary mapping all possible outputs of the queue command
               to the accepted states outlined below.
- suffix:      A string to add in the suffix of written submit script
               files (e.g. 'slurm'). Defaults to the name.

Required Methods
................

queue_parser(user=None, partition=None)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Args::

  user (list):      Filter by user ID
  partition (list): Filter by partition
 
Use fyrd.run.listify() on both user and partition to allow folks to submit
either as a string also.

This function should be an iterator and should yield a tuple of exactly 9 items:

+-------------+-----------+-------------------------------------------------------------------------------------------------+
| Attribute   | Type      | Description                                                                                     |
+=============+===========+=================================================================================================+
| job_id      | int/str   | An integer or string representation of the job ID                                               |
+-------------+-----------+-------------------------------------------------------------------------------------------------+
| array_index | int/None  | An array job index, no required but if present should be an int                                 |
+-------------+-----------+-------------------------------------------------------------------------------------------------+
| name        | str       | The complete name of the job from the queue                                                     |
+-------------+-----------+-------------------------------------------------------------------------------------------------+
| userid      | str       | The job owners user ID as a string, not an ID number                                            |
+-------------+-----------+-------------------------------------------------------------------------------------------------+
| partition   | str       | The partition/queue the job is running in                                                       |
+-------------+-----------+-------------------------------------------------------------------------------------------------+
| state       | str       | The job state, must be standardized to one of the allowed states                                |
+-------------+-----------+-------------------------------------------------------------------------------------------------+
| nodes       | list/None | A list of nodes the  job is assigned to, should be None if job has not yet started running.     |
+-------------+-----------+-------------------------------------------------------------------------------------------------+
| numnodes    | int/None  | An integer count of the number of nodes the job is assigned, should be None if not yet running. |
+-------------+-----------+-------------------------------------------------------------------------------------------------+
| threads     | int/None  | The number of threads the job has on each node.                                                 |
+-------------+-----------+-------------------------------------------------------------------------------------------------+
| exitcode    | int/None  | The exitcode of the job on completion                                                           |
+-------------+-----------+-------------------------------------------------------------------------------------------------+

You can safely get the queue output by calling the self.fetch_queue attribute,
it will return the contents of the queue.

Note: if your batch system allows array jobs, you should try to support them.

The allowed states are:

Good states (success assumed):
- complete
- completed
- special_exit

Active states (assume job is running):
- running
- pending
- completing
- configuring

Bad states (assume job failed):
- failed
- cancelled
- timeout
- boot_fail
- node_fail

Uncertain states (delay for a period to see if job resolves, otherwise mark as bad):
- hold
- stopped
- suspended
- preempted

id_from_stdout(stdout)
~~~~~~~~~~~~~~~~~~~~~~

This method will be called once per submitted job on the `STDOUT` returned by
the submission script. It must parse the output of the submission script and
return a job ID, preferably as an int, but a string is also fine.

Example:

.. code:: python

   def id_from_stdout(stdout):
       return int(stdout.split('.')[0])

Optional Methods
................

format_script(kwds)
~~~~~~~~~~~~~~~~~~~

This method must return a string containing a submitable script minus any call
to `module` or `cd` and minus the command to run itself, these will be added
later.

To build it, build a script by manually parsing any keyword arguments from kwds
that cannot be handled by the `options.options_to_string()` function and then
passing the remaining keywords to `options.options_to_string()`. The
`options_to_string()` command should definitely be included somewhere in the
function, this function is what adds the `#QUEUE <command>` type arguments to
the batch file.

The default method (used if not defined here) is:

.. code:: python

   def format_script(kwds):
       script  = '#!/bin/bash\n'
       script += options.options_to_string(kwds)
       return script

To control where commands to module are added, add a '{modules}' string to the
script, otherwise modules will be added immediately following the script, prior
to the execution command.

Note: `cd <rundir>` will be added automatically before the command also.

submit_args(kwds=None, dependencies=None)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function allows you to alter the command used for submission, if present it
can be used to add additional arguments to the submit command.

It must return a string of command line arguments that will be added between the
`SUBMIT_CMND` and the submit script (note that the submit script does not have to
be written, if it isn't written it will be passed as STDIN, in which case there will
be nothing after the args returned by this function).
