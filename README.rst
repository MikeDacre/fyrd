####
Fyrd
####

One liner script and function submission to torque or slurm clusters with
dependency tracking using python. Uses the same syntax irrespective of cluster
environment!

Learn more at https://fyrd.science, https://fyrd.rtfd.com, and
https://github.com/MikeDacre/fyrd

.. image:: http://i.imgur.com/NNbprZH.png
   :alt: fyrd cluster logo — a Saxon shield remeniscent of those used in fyrds
   :target: https://fyrd.readthedocs.org
   :height: 200
   :width: 200

+---------+----------------------------------------------------+
| Author  | Michael D Dacre <mike.dacre@gmail.com>             |
+---------+----------------------------------------------------+
| License | MIT License, property of Stanford, use as you wish |
+---------+----------------------------------------------------+
| Version | 0.6.2a1                                            |
+---------+----------------------------------------------------+


.. image:: https://badge.buildkite.com/b6659b460caf5205919916c4e9d212c4e04d4301fa55a51180.svg?branch=master
   :target: https://buildkite.com/mikedacre/fyrd-cluster-tests
.. image:: https://travis-ci.org/MikeDacre/fyrd.svg?branch=master
   :target: https://travis-ci.org/MikeDacre/fyrd
.. image:: https://api.codacy.com/project/badge/Grade/c163cff81a1941a18b2c5455901695a3
   :target: https://www.codacy.com/app/mike-dacre/fyrd?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=MikeDacre/fyrd&amp;utm_campaign=Badge_Grade

.. image:: https://readthedocs.org/projects/fyrd/badge/?version=latest
   :target: https://fyrd.readthedocs.io/

.. image:: https://badge.fury.io/py/fyrd.svg
   :target: https://badge.fury.io/py/fyrd
   :alt: PyPI Version
.. image:: https://img.shields.io/badge/python%20versions-2.7%203.4%203.5%203.6%203.7-brightgreen.svg
   :target: https://fyrd.science
.. image:: https://requires.io/github/MikeDacre/fyrd/requirements.svg?branch=master
   :target: https://requires.io/github/MikeDacre/fyrd/requirements/?branch=master
   :alt: Requirements Status


Allows simple job submission with *dependency tracking and queue waiting* on
either torque, slurm, or locally with the multiprocessing module. It uses simple
techniques to avoid overwhelming the queue and to catch bugs on the fly.

It is routinely tested on Mac OS and Linux with slurm and torque clusters, or
in the absence of a cluster, on Python versions `2.7.10`, `2.7.11`, `2.7.12`,
`3.3.0`, `3.4.0`, `3.5.2`, `3.6.2`, and `3.7-dev`. The full test suite is
available in the `tests` folder.

Fyrd is pronounced 'feared' (sort of), it is an Anglo-Saxon term for an army,
particularly an army of freemen (in this case an army of compute nodes). The
logo is based on a Saxon shield commonly used by these groups. This software
was formerly known as 'Python Cluster'.

For usage instructions and complete documentation see `the documentation site
<https://fyrd.readthedocs.io>`_ and the `fyrd_manual.pdf
<https://github.com/MikeDacre/fyrd/blob/master/docs/fyrd_manual.pdf>`_ document
in this repository.

.. contents:: **Contents**

Overview
========

This library was created to make working with torque or slurm clusters as easy
as working with the multiprocessing library. It aims to provide:

- Easy submission of either python functions or shell scripts to torque or slurm
  from within python.
- Simple dependency tracking for jobs.
- The ability to submit jobs with any of the torque or slurm keyword arguments.
- Easy customization.
- Very simple usage that scales to complex applications.
- A simple queue monitoring API that functions identically with torque and slurm
  queues.
- A fallback local mode that allows code to run locally using the multiprocessing
  module without needing any changes to syntax.

To do this, all major torque and slurm keyword arguments are encoded in
dictionaries in the `fyrd/options.py` file using synonyms so that all arguments
are standardized on the fly. Job management is handled by the `Job` class in
`fyrd/job.py`, which accepts any of the keyword arguments in the options file.
To make submission as simple as possible, the code makes used of profiles
defined in the `~/.fyrd/profiles.txt` config file. These allow simple grouping
of keyword arguments into named profiles to make submission even easier.
Dependency tracking is handled by the `depends=` argument to `Job`, which
accepts job numbers or `Job` objects, either singularly or as lists.

To allow simple queue management and job waiting, a `Queue` class is
implemented in `fyrd/queue.py`. It uses iterators, also defined in that file,
to parse torque or slurm queues transparently and allow access to their
attributes through the `Queue` class and the `Queue.jobs` dictionary. The `Job`
class uses this system to block until the job completes when either the
`wait()` or `get()` methods are called.

Note, waiting can email you when it is done, but you need to enable it in the
config file (`~/.fyrd/config.txt`)::

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

To allow similar functionality on a system not connected to a torque or slurm
queue, a local queue that behaves similarly, including allowing dependency
tracking, is implemented in the `fyrd/jobqueue.py` file. It is based on
multiprocessing but behaves like torque.  It is not a good idea to use this
module in place of multiprocessing due to the dependency tracking overhead, it
is primarily intended as a fallback, but it does work well enough to use
independently. **Note: the local mode currently is quite slow, as the overhead
for job management means that 100% of each available CPU is not used, only
around 80% is. The local mode still works fine as a fallback or for testing
code, but it is important to remember that fyrd is meant primarily for large
cluster use.**

As all clusters are different, common alterable parameters are defined in a
config file located at `~/.fyrd/config.txt`. This includes an option for max
queue size, which makes job submission block until the queue has opened up,
preventing job submission failure on systems with queue limits (most clusters).

To make life easier, a bunch of simple wrapper functions are defined in
`fyrd/basic.py` that allow submission without having to worry about using the
class system, or to submit existing job files. Several helper function are also
created in `fyrd/helpers.py` that allow the automation of more complex tasks,
like running `apply` on a pandas dataframe in parallel on the cluster
(`fyrd.helpers.parapply()`).

Basic Usage
-----------

The end result is that submitting 10 thousand very small jobs to a small cluster
can be done like this:

.. code:: python

   jobs = []
   for i in huge_list:
       jobs.append(fyrd.Job(my_function, (i,), profile='small').submit())
   results = fyrd.get(jobs)

The results list in this example will contain the function outputs, even if
those outputs are integers, objects, or other Python types. Similarly, shell
scripts can be run like this:

.. code:: python

   script = r"""zcat {} | grep "#config" | awk '{{split($1,a,"."); print a[2]"\t"$2}}'"""
   jobs   = []
   for i in [i for i in os.listdir('.') if i.endswith('.gz')]:
       jobs.append(fyrd.Job(script.format(i), profile='long').submit())
   results = fyrd.get(jobs)
   for i in results:
       print(i.stdout)

Results will contain the contents of STDOUT for the submitted script

Here is the same code with dependency tracking:

.. code:: python

   script = r"""zcat {} | grep "#config" | awk '{{split($1,a,"."); print a[2]"\t"$2}}'"""
   jobs   = []
   jobs2  = []
   for i in [i for i in os.listdir('.') if i.endswith('.gz')]:
       j = fyrd.Job(script.format(i), profile='long').submit()
       jobs.append(j)
       jobs2.append(fyrd.Job(my_function, depends=j).submit())
   results = []
   for i in jobs2:
       i.wait()
       results.append(i.out)

As you can see, the `profile` keyword is not required, if not supplied the
default profile is used. It is also important to note that `.out` will contain
the same contents as `.stdout` for all script submissions, but for function
submissions, `.out` contains the function output, not STDOUT.

Note, to submit simple functions, I recommend that you use the `jobify`
decorator instead:

.. code:: python

   >>> import fyrd
   >>> @fyrd.jobify(name='test_job', mem='1GB')
   ... def test(string, iterations=4):
   ...     """This does basically nothing!"""
   ...     outstring = ""
   ...     for i in range(iterations):
   ...         outstring += "Version {0}: {1}".format(i, string)
   ...     return outstring
   ... 
   >>> test?
   Signature: test(*args, **kwargs)
   Docstring:
   This is a fyrd.job.Job decorated function.

   When you call it it will return a Job object from which you can get
   the results with the `.get()` method.

   Original Docstring:

   This does basically nothing!
   File:      ~/code/fyrd/fyrd/helpers.py
   Type:      function
   >>> j = test('hi')
   >>> j.get()
   'Version 0: hiVersion 1: hiVersion 2: hiVersion 3: hi'


Command Line Tools
------------------

Fyrd provides a few command line tools to make little jobs easier. The main
tool is `fyrd`. Running `fyrd --help` will give instructions on use, something
like this::

    usage: fyrd [-h] [-v] [-V]
            {run,submit,wait,queue,conf,prof,keywords,clean,local} ...

    Manage fyrd config, profiles, and queue.

    ============   ======================================
    Author         Michael D Dacre <mike.dacre@gmail.com>
    Organization   Stanford University
    License        MIT License, use as you wish
    Version        0.6.2-beta2
    ============   ======================================

    positional arguments:
      {run,submit,wait,queue,conf,prof,keywords,clean,local}
        run (r)             Run simple shell scripts
        submit (sub, s)     Submit existing job files
        wait (w)            Wait for jobs
        queue (q)           Search the queue
        conf (config)       View and manage the config
        prof (profile)      Manage profiles
        keywords (keys, options)
                            Print available keyword arguments.
        clean               Clean up a job directory
        local (server)      Manage the local queue server

    optional arguments:
      -h, --help            show this help message and exit
      -v, --verbose         Show debug outputs
      -V, --version         Print version string

The keywords each have their own help menus and are fairly self-explanatory.
The `conf` and `profile` arguments allow you to edit the fyrd config and
cluster profiles without having to directly edit the config files in the
`~/.fyrd/` directory.

The `keywords` argument is a help function only, it prints all possible keyword
arguments that can be used in cluster submissions.

`queue` allows you to query the queue in the same way that `squeue` or `qstat`
would, with a few extra functions to make it easy to see only your jobs, or
only your running jobs.

There is another command line tool provided `myqueue` or `myq` (both are the
same), these tools are just wrappers for `fyrd queue` and they make it really
fast to query a torque or slurm queue on any machine. e.g. `myq -r` will show
you all your currently running jobs, `myq -r -c` will display a count of all
currently running jobs, and `myq -r -l` will dump a list of job numbers only to
the console, really useful when combined with `xargs`, e.g. `myq -r -l | xargs
qdel`.

The `wait` command just blocks until the provided job numbers complete, and
can send you an email when it completes, see the config info above.

And the `clean` command provides options to clean out a job directory that
contains leftover files from a fyrd session.

Installation
-------------

This module will work with Python 2.7+ on Linux and Mac OS systems.

The betas are on PyPI, and can be installed directly from there:

.. code:: shell

   pip install fyrd
   fyrd conf init

To install a specific tag from github, do the following:

.. code:: shell

   pip install https://github.com/MikeDacre/fyrd/archive/v0.6.1b9.tar.gz
   fyrd conf init

To get the latest version:

.. code:: shell

   pip install https://github.com/MikeDacre/fyrd/tarball/master
   fyrd conf init

To get the development version (still pretty stable):

.. code:: shell

   pip install https://github.com/MikeDacre/fyrd/tarball/dev
   fyrd conf init
 
The `fyrd conf init` command initializes your environment interactively by
asking questions about the local cluster system.

I recommend installing using anaconda or pyenv, this will make your life much
simpler, but is not required.

In general you want either `pyenv <https://github.com/yyuu/pyenv>`_ or user
level install (`pip install --user`) even if you have `sudo` access, as most
cluster environments share /home/<user> across the cluster, making this module
available everywhere. Anaconda will work if it is installed in a cross-cluster
capacity, usually as a module (with lmod, e.g. `module load anaconda`). An
install to the system python will usually fail as cluster nodes need to have
access to the module also.

Importing is simple:

.. code:: python

  import fyrd

Requirements
............

This software requires the following external modules:

- `dill <https://pypi.python.org/pypi/dill>`_ —  which makes function submission more stable
- `tabulate <https://pypi.python.org/pypi/tabulate>`_ —  allows readable printing of help
- `six <https://pypi.python.org/pypi/six>`_ —  makes python2/3 cross-compatibility easier
- `tblib <https://pypi.python.org/pypi/tblib>`_ —  allows me to pass Tracebacks between nodes
- `tqdm <https://pypi.python.org/pypi/tqdm>`_ — pretty progress bars for multi-job get and wait

Cluster Dependencies
....................

In order to submit functions to the cluster, this module must import them on the
compute node. This means that all of your python modules must be available on
every compute node.

By default, the same python executable used for submission is used on the
cluster to run functions, however, this can be overridden by the
'generic_python' option on the cluster. If using this option, you must install
all of your local modules on the cluster also.

To avoid pain and debugging, you can do this manually by running this on your
login node:

.. code:: shell

  freeze --local | grep -v '^\-e' | cut -d = -f 1 > module_list.txt

And then on the compute nodes:

.. code:: shell

  cat module_list.txt | xargs pip install --user

Alternately, if your pyenv is available on the cluster nodes, then all of
your modules are already available, so you don't need to worry about this!


Testing
=======

To fully test this software, I use `py.test` tests written in the tests folder.
Unfortunately, local queue tests do not work with `py.test`, so I have separated
them out into the `local_queue.py` script. To run all tests, run `python
tests/run_tests.py`.

To ensure sensible testing always, I use `buildkite <https://buildkite.com>`_,
which is an amazing piece of software. It integrates into this repository and
runs tests on all python versions I support on my two clusters (a slurm cluster
and a torque cluster) every day and on every push or pull request. I also use
`travis ci <travis-ci.org>`_ to run local queue tests, and
`codacy <https://www.codacy.com/>`_ to monitor code style.

All code in the master branch must pass the travis-ci and buildkite tests, code
in dev also *usually* passes those test, but it is not guaranteed. All other
branches are unstable and will often fail the tests.

Releases
========

I use the following work-flow to release versions of fyrd:

1. Develop new features and fix new bugs in a feature branch
2. Write tests for the new feature
3. When all tests are passing, merge into dev
4. Do more extensive manual testing in dev, possibly add additional
   commits.
5. Repeat the above for other related features and bugs
6. When a related set of fixes and features are done and well tested,
   merge into master with a pull request through github, all travis and 
   buildkite tests must pass for the merge to work.
7. At some point after the new features are in master, add a new tagged
   beta release.
8. After the beta is determined to be stable and all issues attached to
   that version milestone are resolved, create a non-beta tag

New releases are added when enough features and fixes have accumulated to
justify it, new minor version are added only when there are very large changes
in the code and are always tracked by milestones.
   
While this project is still in its infancy, the API cannot be considered stable
and the major version will remain 0. once version 1.0 is reached, any API
changes will result in a major version change.

As such, and non-beta release can be considered stable, beta releases and the
master branch are very likely to be stable, dev is usually but not always
stable, all other branches are very unstable.

Issues and Contributing
=======================

If you have any trouble with this software add an issue in
https://github.com/MikeDacre/fyrd/issues

For peculiar technical questions or help getting the code installed, email
me at `mike.dacre@gmail.com <mailto:mike.dacre@gmail.com>`_.

I am always looking for help with this software, and I will gladly accept
pull requests. In particular, I am looking for help with:

- Testing the code in different cluster environments
- Expanding the list of keyword options
- Adding new clusters other than torque and slurm
- Implementing new features in the issues section

If you are interested in helping out with any of those things, or if you would
be willing to give me access to your cluster to allow me to run tests and port
fyrd to your environment, please contact me.

If you are planning on contributing and submitting a pull request, please
follow these rules:

- Follow the code style as closely as possible, I am a little obsessive about
  that
- If you add new functions or features:
  - Add some tests to the test suite that fully test your new feature
  - Add notes to the documentation on what your feature does and how it works
- Make sure your code passes the full test suite, which means you need to run
  `python tests/run_tests.py` from the root of the repository at a bare
  minimum. Ideally, you will install pyenv and run `bash tests/pyenv_tests.py`
- Squash all of your commits into a single commit with a well written and
  informative commit message.
- Send me a pull request to either the `dev` or `master` branches.

It may take a few days for me to fully review your pull request, as I will test
it extensively. If it is a big new feature implementation I may request that
you send the pull request to the `dev` branch instead of to `master`.

Why the Name?
=============

I gave this project the name 'Fyrd' in honour of my grandmother, Hélène
Sandolphen, who was a scholar of old English. It is the old Anglo-Saxon word
for 'army', and this code gives you an army of workers on any machine so it
seemed appropriate.

The project used to be called "Python Cluster", which is more descriptive but
frankly boring. Also, about half a dozen other projects have almost the same
name, so it made no sense to keep that name and put the project onto PyPI.


Documentation
=============

This software is much more powerful that this document gives it credit for,
to get the most out of it, read the docs at https://fyrd.readthedocs.org
or get the PDF version from the file in
`docs/fyrd_manual.pdf <https://github.com/MikeDacre/fyrd/blob/master/docs/fyrd_manual.pdf>`_.
