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

- ``run``      - run an arbitrary shell script on the cluster
- ``run-job``  - run existing cluster script(s)
- ``wait``     - wait for a list of jobs
- ``queue``    - show running jobs, makes filtering jobs very easy
- ``config``   — show and edit the contents of the config file
- ``profile``  - inspect and manage cluster profiles
- ``keywords`` - print a list of current keyword arguments with descriptions for each
- ``clean``    - clean all script and output files in the given directory

Several of the commands have aliases (``conf`` and ``prof`` being the two main ones)

Emailing
........

The ``run``, ``run-job``, and ``wait`` commands can all email you when they are done. To use
this you need to configure the sending in the ``~/fyrd/config.txt`` file::

    [notify]
    mode = linux  # Can be linux or smtp, linux uses the mail command
    notify_address = your.address@gmail.com
    # The following are only needed for smtp mode
    smtp_host = smtp.gmail.com
    smtp_port = 587
    smtp_tls = True
    smtp_from = your.server@gmail.com
    smtp_user = None  # Defaults to smtp_from
    # This is insecure, so use an application specific password. This should
    # be a read-only file with the SMTP password. After making it run:
    # chmod 400 ~/.fyrd/smtp_pass
    smtp_passfile = ~/.fyrd/smtp_pass

To enable emailing, pass ``-n`` (notify) to ``wait``, or ``-w -n`` to the other two commands.
You can also manually specify the address with ``-e your.address@gmail.com``.

Examples
........

.. code:: shell

   fyrd run 'samtools display big_file.bam | python $HOME/bin/my_parser.py > outfile'
   fyrd run --profile long --args walltime=24:00:00,mem=20G --wait -n \
            'samtools display big_file.bam | python $HOME/bin/my_parser.py > outfile'

.. code:: shell

   fyrd submit --wait -n ./jobs/*.sh

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

``fyrd``::

    usage: fyrd [-h] [-v] {run,submit,wait,queue,conf,prof,keywords,clean} ...

    Manage fyrd config, profiles, and queue.

    ============   ======================================
    Author         Michael D Dacre <mike.dacre@gmail.com>
    Organization   Stanford University
    License        MIT License, use as you wish
    Version        0.6.2a1
    ============   ======================================

    positional arguments:
      {run,submit,wait,queue,conf,prof,keywords,clean}
        run (r)             Run simple shell scripts
        submit (sub, s)     Submit existing job files
        wait (w)            Wait for jobs
        queue (q)           Search the queue
        conf (config)       View and manage the config
        prof (profile)      Manage profiles
        keywords (keys, options)
                            Print available keyword arguments.
        clean               Clean up a job directory

    optional arguments:
      -h, --help            show this help message and exit
      -v, --verbose         Show debug outputs

``fyrd run``::

    usage: fyrd run [-h] [-p PROFILE] [-c CORES] [-m MEM] [-t TIME] [-a ARGS] [-w]
                    [-k] [-l] [-n] [-e EMAIL] [-s] [-x EXTRA_VARS] [-d]
                    [shell_script] [file_parsing [file_parsing ...]]

    Run a shell script on the cluster and optionally wait for completion.

    Allows the running of a single simple shell script, or the same shell script on
    many files, or more complex file interpretation.

    positional arguments:
      shell_script          The script to run
      file_parsing          The script to run

    optional arguments:
      -h, --help            show this help message and exit
      -s, --simple          The amount of walltime to request
      -x EXTRA_VARS, --extra-vars EXTRA_VARS
                            Regex in form "new_var:orig_var:regex:sub,..."
      -d, --dry-run         Print commands instead of running them

    Run Options:
      -p PROFILE, --profile PROFILE
                            The profile to use to run
      -c CORES, --cores CORES
                            The number of cores to request
      -m MEM, --mem MEM     The amount of memory to request
      -t TIME, --time TIME  The amount of walltime to request
      -a ARGS, --args ARGS  Submission args, e.g.:
                            'time=00:20:00,mem=20G,cores=10'
      -w, --wait            Wait for the job to complete
      -k, --keep            Keep submission scripts
      -l, --clean           Delete STDOUT and STDERR files when done

    Notification Options:
      -n, --notify          Send notification email when done
      -e EMAIL, --email EMAIL
                            Email address to send notification to, default set in
                            ~/.fyrd/config.txt

``fyrd submit``::

    usage: fyrd submit [-h] [-p PROFILE] [-c CORES] [-m MEM] [-t TIME] [-a ARGS]
                       [-w] [-k] [-l] [-n] [-e EMAIL]
                       job_files [job_files ...]

    Run a shell script on the cluster and optionally wait for completion.

    Allows the running of a single simple shell script, or the same shell script on
    many files, or more complex file interpretation.

    positional arguments:
      job_files             The script to run

    optional arguments:
      -h, --help            show this help message and exit

    Run Options:
      -p PROFILE, --profile PROFILE
                            The profile to use to run
      -c CORES, --cores CORES
                            The number of cores to request
      -m MEM, --mem MEM     The amount of memory to request
      -t TIME, --time TIME  The amount of walltime to request
      -a ARGS, --args ARGS  Submission args, e.g.:
                            'time=00:20:00,mem=20G,cores=10'
      -w, --wait            Wait for the job to complete
      -k, --keep            Keep submission scripts
      -l, --clean           Delete STDOUT and STDERR files when done

    Notification Options:
      -n, --notify          Send notification email when done
      -e EMAIL, --email EMAIL
                            Email address to send notification to, default set in
                            ~/.fyrd/config.txt

``fyrd wait``::

    usage: fyrd wait [-h] [-n] [-e EMAIL] [-u USERS] [jobs [jobs ...]]

    Wait on a list of jobs, block until they complete.

    positional arguments:
      jobs                  Job list to wait for

    optional arguments:
      -h, --help            show this help message and exit
      -u USERS, --users USERS
                            A comma-separated list of users to wait for

    Notification Options:
      -n, --notify          Send notification email when done
      -e EMAIL, --email EMAIL
                            Email address to send notification to, default set in
                            ~/.fyrd/config.txt

``fyrd queue``::

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

``fyrd conf``::

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

``fyrd prof``::

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
    there. i.e. if DEFAULT contains ``partition=normal``, if 'long' does not have
    a 'partition' option, it will default to 'normal'.

    To reset the profile to defaults, just delete the file and run this script
    again.

``fyrd keywords``::

    usage: fyrd keywords [-h] [-t | -s | -l]

    optional arguments:
      -h, --help          show this help message and exit
      -t, --table         Print keywords as a table
      -s, --split-tables  Print keywords as multiple tables
      -l, --list          Print a list of keywords only

``fyrd clean``::

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

Several shell scripts are provided in ``bin/`` to provide shortcuts to the *fyrd*
subcommands:

- ``frun``: ``fyrd run``
- ``fsub``: ``fyrd submit``
- ``my-queue`` (or ``myq``): ``fyrd queue``
- ``clean-job-files``: ``fyrd clean``
- ``monitor-jobs``: ``fyrd wait``
- ``cluster-keywords``: ``fyrd keywords``
