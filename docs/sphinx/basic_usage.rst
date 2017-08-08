Getting Started
===============

Simple Job Submission
---------------------

At its simplest, this module can be used by just executing `submit(<command>)`,
where command is a function or system command/shell script. The module will
autodetect the cluster, generate an intuitive name, run the job, and write all
outputs to files in the current directory. These can be cleaned with
`clean_dir()`.

To run with dependency tracking, run:

.. code:: python

  import fyrd
  job  = fyrd.submit(<command1>)
  job2 = fyrd.submit(<command2>, depends=job1)
  out1, out2 = fyrd.get([job, job2])  # Will block until job completes

The `submit()` function is actually just a wrapper for the
`Job </api.html#fyrd-job-job>`_ class. The same behavior as above can be
obtained by initializing a `Job` object directly:
                                                
.. code:: python

  import fyrd
  job  = fyrd.Job(<command1>)
  job.submit()
  job2 = fyrd.Job(<command2>, depends=job1).submit()
  out  = job2.get()  # Will block until job completes

Note that as shown above, the submit method returns the `Job` object, so it
can be called on job initialization. Also note that the object returned by
calling the `submit()` function (as in the first example) is also a `Job`
object, so these two examples can be used fully interchangeably.

Similar wrappers allow you to submit and monitor existing job files, such
as those made by other pipelines:

.. code:: python

   import os
   import fyrd
   jobs = []
   job_dir = os.path.abspath('./jobs/')
   for job in [os.path.join(job_dir, i) for i in os.listdir(job_dir) if i.endswith('sh')]:
       jobs.append(fyrd.submit_file(job))
   fyrd.wait(jobs)  # Will block until every job is completed

This type of thing can also be accomplished using the `console script </console.html>`_:

.. code:: shell

   fyrd run --wait ./jobs/*.sh

Functions
---------

The submit function works well with python functions as well as with shell
scripts and shell commands, in fact, this is the most powerful feature of this
package. For example:

.. code:: python

   import fyrd
   def raise_me(something, power=2):
       return something**power
   outs = []
   if __name__ == '__main__':
       for i in range(80):
           outs.append(fyrd.submit(my_function, (i,), {'power': 2},
                                   mem='10MB', time='00:00:30'))
       final_sum = 0
       for i in outs:
           final_sum += i.get()
       print(final_sum)

By default this will submit every instance as a job on the cluster, then get the
results and clean up all intermediate files, and the code will work identically
on a Mac with no cluster access, a slurm cluster, or a torque cluster, with no
need to change syntax.

This is very powerful when combined with simple methods that split files or
large python classes, to make this kind of work easier, a number of simple
functions are provided in `the helpers module </advanced_usage.html#helpers>`_,
to learn more about that, review the Advanced Usage section of this documentation.

Function submission works equally well for submitting methods, however the original
class object will not be updated, the method return value will be accurate, but any
changes the method makes to `self` will not be returned from the cluster and will be
lost.

Possible Infinate Recursion Error
.................................

**Warning**: in order for function submission to work, *fyrd* ends up importing
your original script file on the nodes. This means that all code in your file
will be executed, so anything that isn't a function or class must be protected
with an `if __name__ == '__main__':` protecting statement.

If you do not do this you can end up with multi-submission and infinite
recursion, which could mess up your jobs or just crash the job, but either way,
it won't be good.

This isn't true when submitting from an interactive session such as ipython
or jupyter.

File Submission
---------------

If you want to just submit a job file that has already been created, either by
this software or any other method, that can be done like this:

.. code:: python

  from fyrd import submit_file
  submit_file('/path/to/script', dependencies=[7, 9])

This will return the job number and will enter the job into the queue as
dependant on jobs 7 and 9. The dependencies can be omitted.

Keywords
--------

The `Job` class, and therefore every submission script, accepts a large number of
keyword arguments and synonyms to make job submission easy. Some good examples:

- cores
- mem (or memory)
- time (or walltime)
- partition (or queue)

The synonyms are provided to make submission easy for anyone familiar with
the arguments used by either torque or slurm. For example:

.. code:: python

   job = Job('zcat huge_file | parse_file', cores=1, mem='30GB', time='24:00:00')
   job = Job(my_parallel_function, cores=28, mem=12000, queue='high_mem')
   for i in huge_list:
       out.append(submit(parser_function, i, cores=1, mem='1GB', partition='small'))
   job = Job('ls /etc')

As you can see, optional keywords make submission very easy and flexible. The
whole point of this software it to make working with a remote cluster in python
as easy as possible.

For a full list of keyword arguments see the
`Keyword Arguments </keywords.html>`_ section of the documentation.

All options are defined in the `fyrd.options </api.html#fyrd-options>`_ module.
If you want extra options, just submit an issue or add them yourself and send
me a pull request.

Profiles
--------

One of the issues with using keyword options is the nuisance of having to type
them every time. More importantly, when writing code to work on any cluster one
has to deal with heterogeneity between the clusters, such as the number of cores
available on each node, or the name of the submission queue.

Because of this, *fyrd* makes use of profiles that bundle keyword arguments and
give them a name, so that cluster submission can look like this:

.. code:: python

   job = Job('zcat huge_file | parse_file', profile='large')
   job = Job(my_parallel_function, cores=28, profile='high_mem')

These profiles are defined in `~/.fyrd/profiles.txt` by default and have the
following syntax::

  [large]
  partition = normal
  cores = 16
  nodes = 1
  time = 24:00:00
  mem = 32000

This means that you can now do this:

.. code:: python

   Job(my_function, profile='large')

You can create as many of these as you like.

While you can edit the profile file directly to add and edit profile, it is
easier and more stable to use the console script:

..code:: shell

  fyrd profile list
  fyrd profile edit large time:02-00:00:00 mem=64GB
  fyrd profile edit DEFAULT partition:normal
  fyrd profile remove-option DEFAULT cores
  fyrd profile add silly cores:92 mem:1MB
  fyrd profile delete silly

The advantage of using the console script is that argument parsing is done on
editing the profiles, so any errors are caught at that time. If you edit the
file manually, then any mistakes will cause an Exception to be raised when you
try to submit a job.

If no arguments are given the default profile (called 'DEFAULT' in the
`config </configuration.html>`_ file) is used.

**Note**: any arguments in the DEFAULT profile are available in all profiles if
the are not manually overridden there. The DEFAULT profile cannot be deleted. It
is a good place to put the name of the default queue.
