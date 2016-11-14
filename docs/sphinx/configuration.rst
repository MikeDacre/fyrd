Configuration
=============

Many program parameters can be set in the config file, found by default at
`~/.fyrd/config.txt`.

This file has three sections with the following defaults:

[queue]::

    max_jobs (int):     sets the maximum number of running jobs before
                        submission will pause and wait for the queue to empty
    sleep_len (int):    sets the amount of time the program will wait between
                        submission attempts
    queue_update (int): sets the amount of time between refreshes of the queue.
    res_time (int):     Time in seconds to wait if a job is in an uncertain
                        state, usually preempted or suspended. These jobs often
                        resolve into running or completed again after some time
                        so it makes sense to wait a bit, but not forever. The
                        default is 45 minutes: 2700 seconds.
    queue_type (str):   the type of queue to use, one of 'torque', 'slurm',
                        'local', 'auto'. Default is auto to auto-detect the
                        queue.

[jobs]::

    clean_files (bool):    means that by default files will be deleted when job
                           completes
    clean_outputs (bool):  is the same but for output files (they are saved
                           first)
    file_block_time (int): Max amount of time to block after job completes in
                           the queue while waiting for output files to appear.
                           Some queues can take a long time to copy files under
                           load, so it is worth setting this high, it won't
                           block unless the files do not appear.
    filepath (str):        Path to write all temp and output files by default,
                           must be globally cluster accessible. Note: this is
                           *not* the runtime path, just where files are written
                           to.
    suffix (str):          The suffix to use when writing scripts and output
                           files
    auto_submit (bool):    If wait() or get() are called prior to submission,
                           auto-submit the job. Otherwise throws an error and
                           returns None
    generic_python (bool): Use /usr/bin/env python instead of the current
                           executable, not advised, but sometimes necessary.
    profile_file (str):    the config file where profiles are defined.

[jobqueue]::

    Sets options for the local queue system, will be removed in the future in
    favor of database.

    jobno (int):  The current job number for the local queue, auto-increments
                  with every submission.

Example file::
 
    [queue]
    res_time = 2700
    queue_type = auto
    sleep_len = 1
    queue_update = 2
    max_jobs = 1000
    bool = True
     
    [jobs]
    suffix = cluster
    file_block_time = 12
    filepath = None
    clean_outputs = False
    auto_submit = True
    profile_file = /Users/dacre/.fyrd/profiles.txt
    clean_files = True
    generic_python = False
    
    [jobqueue]
    jobno = 9


The config is managed by `fyrd/conf.py </api.html#fyrd-conf>`_ and enforces a
minimum set of entries. If the config does not exist or any entries are
missing, they will be created on the fly using the defaults defined in the
defaults.
 
