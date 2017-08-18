---
layout: default
title:  Fyrd
---

{% include badges.html %}

### What is *Fyrd*?

*Fyrd* is a library for Python 2.7+/3.3+ that allows easy multiprocessing-style parallelization using a batch system back end. Right now it supports [torque](http://www.adaptivecomputing.com/products/open-source/torque/) and [slurm](https://slurm.schedmd.com/), with a local, multiprocessing based, fallback. Because it is written in a modular way, however, any batch system can be added. To request your batch system be added, please email requests@fyrd.science or [submit an issue on the github page]({{ site.github.repository_url }}/issues).

The local fallback mode is a miniature torque/slurm style job managing clone. It starts a server on your machine and that server will accept and run jobs in the same way that torque or slurm would. It allows dependency tracking to work, but does not implement partitions or allow the viewing of other users jobs. Note that fyrd's local mode is not a fantastic substitute for multiprocessing, because it uses a file-submission model that is unnecessary for local jobs, this means that there is a slight (about 0.5 second) overhead for every job that is submitted.

### A Quick Example

Say you have some piece of code that you got from somewhere, lets call it `really_slow_algorithm`, and you want to run it on 200 files and then work on then examine the results of `STDOUT`, but you need to parse the results first to get just the first two columns. Here is how you would do that:

```python
import os
import math
import fyrd

SCRIPT = """really_slow_algorithm {} | awk '{print $1, "\\t", $2}'"""

# This is the same as running fyrd.Job(filter_results, (x,), profile='small', time='00:01:00').submit()
# It is best used when dependency tracking is not needed and every function submission requires
# the same resources
@fyrd.jobify(profile='small', time='00:01:00')
def filter_results(x):
    """If first column contains 'tomato', return the log of the second column."""
    output = []
    for i in x.split('\n'):
        name, data = i.split('\t')
    if 'tomato' in name:
        output.append(math.log10(int(data)))
      return output

script_jobs = []
for i in os.listdir('input_files'):
    script_jobs.append(fyrd.Job(SCRIPT.format(i), cores=1, mem='10GB', time='48:00:00').submit())

# Wait for jobs to complete
fyrd.wait(script_jobs)
    
func_jobs = []
for result in script_job:
    func_jobs.append(filter_results(result.stdout))

filtered = fyrd.basic.get(func_jobs)
```

### Dependency tracking

Fyrd implements a simple dependency tracking system. Most batch systems allow dependency tracking, but they require somewhat complex syntax and a prior knowledge of the job ID, something that you do not have when you are building a pipeline. *Fyrd* therefore allows tracking of dependencies using [Job objects](https://fyrd.readthedocs.io/en/latest/api.html#fyrd.Job), as well as by job IDs, it you happen to have those:

```python
job = fyrd.Job('really_slow_algorithm big_file').submit()

job.wait()

if job.completed:
    job2 = fyrd.Job(completed_function)
elif job.failed:
    job2 = fyrd.Job(failed_function)

job3 = fyrd.Job(wrapup_function, depends=job2)

for job in [job2, job3]:
    job.submit()

result = job3.get()
```

### Keyword Arguments and Profiles

Batch systems have many possible submission options, to allow these options to work the same irrespective of cluster syntax, they are all implemented as keyword arguments to the `fyrd.job.Job` class. A complete list is available in the [documentation](https://fyrd.readthedocs.io/en/latest/keywords.html).

Because there can be many possible arguments to the batch system, and because most jobs use the same few arguments, *Fyrd* also has a [profile system](https://fyrd.readthedocs.io/en/latest/basic_usage.html#profiles), which allows common arguments to be bundled together. This system implements a `DEFAULT` profile, which allows job submission with no arguments at all:

```python
fyrd.Job(my_function).submit().get()
fyrd.Job(long_function, profile='long').submit().get()
```

### Installation

```bash
pip install fyrd
```

To get a specific version:
```bash
pip install {{ site.github.repository_url }}/archive/{{ site.version }}.tar.gz --user
fyrd conf init
```

To get the latest stable version:

```bash
pip install {{ site.github.repository_url }}/tarball/master --user
fyrd conf init
```

To get the latest development version (still pretty stable):

```bash
pip install {{ site.github.repository_url }}/tarball/dev --user
fyrd conf init
```
 

The `fyrd conf init` command initializes your environment interactively by asking questions about the local cluster system.

### Documentation

Fyrd has very comprehensive documentation on [Read the Docs](https://fyrd.readthedocs.org), you should definitely read it. A good place to start is the [getting started documentation](https://fyrd.readthedocs.io/en/latest/basic_usage.html#).

### Issues and Contributing

If you have any trouble with this software add an issue on [the issues page]({{ site.github.repository_url }}/issues)

I am always looking for help testing the software and implementing new keyword arguments. I would also very must like to add new batch system environments, but I need access to those clusters in order to test the new keyword arguments and implement queue parsing. If you would like to help with that or give me access to your cluster (for development of this package only), please email me a mike@fyrd.science.

### Why the Name?

I gave this project the name 'Fyrd' in honor of my grandmother, Hélène Sandolphen, who was a scholar of old English. It is the old Anglo-Saxon word for 'army', particularly an army of freemen, and this code gives you an army of workers on any machine or cluster so it seemed appropriate. The logo is an Anglo Saxon shield of the kind used by the Fyrds, with a graphic of a cluster superimposed on the top.

The project used to be called "Python Cluster", which is more descriptive but frankly boring. Also, about half a dozen other projects have almost the same name, so it made no sense to keep that name and put the project onto PyPI.
