############
Python Slurm
############

Depends on `pyslurm <https://github.com/gingergeeks/pyslurm>`_ by `Ginger Geeks <gingergeeks.co.uk>`_.

Allows the user to submit slurm jobs from a python script and monitor progress.

Useful if your cluster limits total queue submissions, as your personal queue length can be monitored and jobs only submitted when queue length drops below a certain count.

Allows simple dependency tracking with SLURM's built in dependency tracking commands, or script level dependency tracking by monitoring job state and output files.

NOTE: Under active development. Unstable. Do not use.

*********************
Simple Job Submission
*********************

If you want to just submit a file, that can be done like this::

    from slurmy import submit_file
    submit_file('/path/to/script', dependencies=007:009)

This will return the job number and will enter the job into the queue as dependant on jobs 007 and 009. The dependencies can be omitted.

Alternatively, if your queue has a job limit on it, you can define that limit in ~/.slurmy under [queue] and max_jobs. By default this is 1000 jobs at a time. To submit jobs and block if the job limit is hit, run::

    from slurm import monitor_submit_file
    monitor_submit_file('/path/to/script', dependencies=007:009)

All other syntax is the same as for submit_file.

****************
Queue Management
****************

Queue management can be directly run through pyslurm. Queue management in slurmy focusses on single user queue management and works with pyslurm object. All queue management functions are written in cython.

To generate a queue object, do the following::

    import slurmy
    q = slurmy.queue()

This will give you a simple queue object named q that you can query. It contains a ful pyslurm job object named full_queue, as well as a dictionary of extensive job information for your user only, named queue. It also contains other data and funtions, for full information see the documentation on that file with help(slurmy.queue).
