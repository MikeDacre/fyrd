Adding New Batch Systems
========================

Required Attributes
-------------------

- IDENTIFYING_SCRIPTS: A list of scripts that, when present on the PATH, identify this queue. All must be present.

Required Functions
------------------

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
