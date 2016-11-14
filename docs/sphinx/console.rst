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

.. code:: shell

   fyrd clean

All Options
...........

.. argparse::
  :module: fyrd.__main__
  :func: command_line_parser
  :prog: fyrd


aliases
-------

Several shell scripts are provided in `bin/` to provide shortcuts to the *fyrd*
subcommands:

- `my-queue` (or `myq`): `fyrd queue`
- `clean-job-files`: `fyrd clean`
- `monitor-jobs`: `fyrd wait`
- `cluster-keywords`: `fyrd keywords`
