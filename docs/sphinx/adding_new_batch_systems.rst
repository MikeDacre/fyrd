Adding New Batch Systems
========================

To add a new batch system, a script describing that system must be added to the `/batch` package, and keyword
options must be added to the `options.py` dictionaries. The new batch system must have entries for every
record in the `CLUSTER_CORE` and `CLUSTER_OPTS` dictionaries. Additionally, it is possible to add options to
additional dictionaries in that file, but as much as possible all options should be added to just those two
dictionaries.

Batch Queue Script
------------------

Every new batch system must have a script in `/batch` that contains the following features.

Required Attributes
...................

- SUBMISSION_CMND:     A command that is used to submit scripts, such as `qsub` or `sbatch`
- PREFIX:              A string to add before options in the script file, such as `#SBATCH` for slurm or `#PBS` for torque
- IDENTIFYING_SCRIPTS: A list of scripts that, when present on the PATH, identify this queue. All must be present.
- QUEUE_OUTPUT:        A sample output of the queue command (e.g. `qstat`) to use for testing and documentation. It should
                       contain just a few jobs with output representative of multiple job types (e.g. running, queued,
                       cancelled, array jobs and simple jobs).

Required Functions
..................

queue_parser(user=None, partition=None)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function should be an iterator and should yield a tuple of exactly 9 items:

+-------------+-----------+-------------------------------------------------------------------------------------------------+
| Attribute   | Type      | Description                                                                                     |
+=============+===========+=================================================================================================+
| job_id      | int/str   | An integer or string representation of the job ID                                               |
+-------------+-----------+-------------------------------------------------------------------------------------------------+
| array_index | int/None  | An array job index, no required but if present should be an int                                 |
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

format_script(kwds)
~~~~~~~~~~~~~~~~~~~

This function must return a string containing a submitable script minus any
call to `module` or `cd` and minus the command to run itself, these will be
added later.

To build it, build a script by manually parsing any keyword arguments from kwds
that cannot be handled by the `options.options_to_string()` function and then
passing the remaining keywords to `options.options_to_string()`.

In the simplest case this function could look like this:

.. code:: python

   def format_script(kwds):
       script  = '#!/bin/bash'
       script += options.options_to_string(kwds)
       return script
