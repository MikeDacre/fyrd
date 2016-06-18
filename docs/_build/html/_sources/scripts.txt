Scripts
=======

This package contains a few little helper scripts to make your life easier.
These are not required in order to use the cluster library.

clean_job_files
---------------

::

  usage: clean_job_files [-h] [-d DIR] [-s SUFFIX] [-q {torque,slurm,local}]
                         [-n] [-v VERBOSE]

  Clean all intermediate files created by the cluster module from this dir.

  ============================================================================

          AUTHOR: Michael D Dacre, mike.dacre@gmail.com
    ORGANIZATION: Stanford University
         LICENSE: MIT License, property of Stanford, use as you wish
         CREATED: 2016-34-15 15:06
   Last modified: 2016-06-16 10:42

     DESCRIPTION: Uses the cluster.job.clean_dir() function

         CAUTION: The clean() function will delete **EVERY** file with
                  extensions matching those these::
                      .<suffix>.err
                      .<suffix>.out
                      .<suffix>.sbatch & .cluster.script for slurm mode
                      .<suffix>.qsub for torque mode
                      .<suffix> for local mode
                      _func.<suffix>.py
                      _func.<suffix>.py.pickle.in
                      _func.<suffix>.py.pickle.out

  ============================================================================

  optional arguments:
    -h, --help            show this help message and exit
    -d DIR, --dir DIR     Directory to clean
    -s SUFFIX, --suffix SUFFIX
                          Directory to clean
    -q {torque,slurm,local}, --qtype {torque,slurm,local}
                          Limit deletions to this qtype
    -n, --no-confirm      Do not confirm before deleting (for scripts)
    -v VERBOSE, --verbose VERBOSE
                          Show debug information

* :ref:`search`
