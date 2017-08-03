Console Scripts
===============

This software is primarily intended to be a library, however some management tasks are just
easier from the console. For that reason, *fyrd* has a frontend console script that makes 
tasks such as managing the local config and profiles trivial, it also has modes to inspect
the queue easily, and to wait for jobs from the console, as well as to clean the working
directory.

fyrd
----

This software has uses a subcommand system to separate modes, and has six modes:

- `config`   — show and edit the contents of the config file
- `profile`  - inspect and manage cluster profiles
- `keywords` - print a list of current keyword arguments with descriptions for each
- `queue`    - show running jobs, makes filtering jobs very easy
- `wait`     - wait for a list of jobs
- `clean`    - clean all script and output files in the given directory

Several of the commands have aliases (`conf` and `prof` being the two main ones)

Examples
........

.. code:: shell

   fyrd prof list 
   fyrd prof add large cores:92 mem:200GB partition:high_mem time:00:06:00

.. code:: shell

   fyrd queue  # Shows all of your current jobs
   fyrd queue -a # Shows all users jobs
   fyrd queue -p long -u bob dylan # Show all jobs owned by bob and dylan in the long queue

.. code:: shell

   fyrd wait 19872 19876
   fyrd wait -u john

   # Will block until all of bob's jobs in the long queue finish
   fyrd queue -p long -u bob -l | xargs fyrd wait 

.. code:: shell

   fyrd clean

All Options
...........

`fyrd`::

    usage: fyrd [-h] [-v] {conf,prof,keywords,queue,wait,clean} ...

    Manage fyrd config, profiles, and queue.

    ============   ======================================
    Author         Michael D Dacre <mike.dacre@gmail.com>
    Organization   Stanford University
    License        MIT License, use as you wish
    Version        0.6.2b9
    ============   ======================================

    positional arguments:
      {conf,prof,keywords,queue,wait,clean}
        conf (config)       View and manage the config
        prof (profile)      Manage profiles
        keywords (keys, options)
                            Print available keyword arguments.
        queue (q)           Search the queue
        wait                Wait for jobs
        clean               Clean up a job directory

    optional arguments:
      -h, --help            show this help message and exit
      -v, --verbose         Show debug outputs

`fyrd conf`::

    usage: fyrd conf [-h] {show,list,help,update,alter,init} ...

    This script allows display and management of the fyrd config file found
    here: /home/dacre/.fyrd/config.txt.

    positional arguments:
      {show,list,help,update,alter,init}
        show (list)         Show current config
        help                Show info on every config option
        update (alter)      Update the config
        init                Interactively initialize the config

    optional arguments:
      -h, --help            show this help message and exit

    Show usage::
        fyrd conf show [-s <section>]

    Update usage::
        fyrd conf update <section> <option> <value>

    *Values can only be altered one at a time*

    To create a new config from scratch interactively::
        fyrd conf init [--defaults]

`fyrd prof`::

    usage: fyrd prof [-h]
                     {show,list,add,new,update,alter,edit,remove-option,del-option,delete,del}
                     ...

    Fyrd jobs use keyword arguments to run (for a complete list run this script
    with the keywords command). These keywords can be bundled into profiles, which
    are kept in /home/dacre/.fyrd/profiles.txt. This file can be edited directly or manipulated here.

    positional arguments:
      {show,list,add,new,update,alter,edit,remove-option,del-option,delete,del}
        show (list)         Print current profiles
        add (new)           Add a new profile
        update (alter, edit)
                            Update an existing profile
        remove-option (del-option)
                            Remove a profile option
        delete (del)        Delete an existing profile

    optional arguments:
      -h, --help            show this help message and exit

    Show::
        fyrd prof show

    Delete::
        fyrd prof delete <name>

    Update::
        fyrd prof update <name> <options>

    Add::
        fyrd prof add <name> <options>

    <options>:
        The options arguments must be in the following format::
            opt:val opt2:val2 opt3:val3

    Note: the DEFAULT profile is special and cannot be deleted, deleting it will
    cause it to be instantly recreated with the default values. Values from this
    profile will be available in EVERY other profile if they are not overriden
    there. i.e. if DEFAULT contains `partition=normal`, if 'long' does not have
    a 'partition' option, it will default to 'normal'.

    To reset the profile to defaults, just delete the file and run this script
    again.

`fyrd keywords`::

    usage: fyrd keywords [-h] [-t | -s | -l]

    optional arguments:
      -h, --help          show this help message and exit
      -t, --table         Print keywords as a table
      -s, --split-tables  Print keywords as multiple tables
      -l, --list          Print a list of keywords only

`fyrd queue`::

    usage: fyrd queue [-h] [-u  [...] | -a] [-p  [...]] [-r | -q | -d | -b]
                      [-l | -c]

    Check the local queue, similar to squeue or qstat but simpler, good for
    quickly checking the queue.

    By default it searches only your own jobs, pass '--all-users' or
    '--users <user> [<user2>...]' to change that behavior.

    To just list jobs with some basic info, run with no arguments.

    optional arguments:
      -h, --help            show this help message and exit

    queue filtering:
      -u  [ ...], --users  [ ...]
                            Limit to these users
      -a, --all-users       Display jobs for all users
      -p  [ ...], --partitions  [ ...]
                            Limit to these partitions (queues)

    queue state filtering:
      -r, --running         Show only running jobs
      -q, --queued          Show only queued jobs
      -d, --done            Show only completed jobs
      -b, --bad             Show only completed jobs

    display options:
      -l, --list            Print job numbers only, works well with xargs
      -c, --count           Print job count only

`fyrd wait`::
  
    usage: fyrd wait [-h] [-u USERS] [jobs [jobs ...]]

    Wait on a list of jobs, block until they complete.

    positional arguments:
      jobs                  Job list to wait for

    optional arguments:
      -h, --help            show this help message and exit
      -u USERS, --users USERS
                            A comma-separated list of users to wait for

`fyrd clean`::

    usage: fyrd clean [-h] [-o] [-s SUFFIX] [-q {torque,slurm,local}] [-n] [dir]

    Clean all intermediate files created by the cluster module.

    If not directory is passed, the default if either scriptpath or outpath are
    set in the config is to clean files in those locations is to clean those
    directories. If they are not set, the default is the current directory.

    By default, outputs are not cleaned, to clean them too, pass '-o'

    Caution:
        The clean() function will delete **EVERY** file with
        extensions matching those these::

            .<suffix>.err
            .<suffix>.out
            .<suffix>.sbatch & .fyrd.script for slurm mode
            .<suffix>.qsub for torque mode
            .<suffix> for local mode
            _func.<suffix>.py
            _func.<suffix>.py.pickle.in
            _func.<suffix>.py.pickle.out

    positional arguments:
      dir                   Directory to clean (optional)

    optional arguments:
      -h, --help            show this help message and exit
      -o, --outputs         Clean output files too
      -s SUFFIX, --suffix SUFFIX
                            Suffix to use for cleaning
      -q {torque,slurm,local}, --qtype {torque,slurm,local}
                            Limit deletions to this qtype
      -n, --no-confirm      Do not confirm before deleting (for scripts)

Aliases
-------

Several shell scripts are provided in `bin/` to provide shortcuts to the *fyrd*
subcommands:

- `my-queue` (or `myq`): `fyrd queue`
- `clean-job-files`: `fyrd clean`
- `monitor-jobs`: `fyrd wait`
- `cluster-keywords`: `fyrd keywords`
