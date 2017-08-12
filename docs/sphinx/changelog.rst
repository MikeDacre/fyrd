Change Log
==========

Version 0.6.2a1
---------------

This version brings a major overhaul to the structure of the code, while leaving the
API *mostly* intact.

Major Changes
.............

- Batch system definitions now fully modular and are contained in the `fyrd.batch_systems`
  package. `options.py` has also been moved into this package, which allows any programmer
  to add a new batch system definition to fyrd by just editing the contents of that small
  subpackaged
- Updated console script to allow running arbitrary shell scripts on the console with
  `fyrd run` or submitting any number of existing job files using `fyrd sub`. Added the
  new alias scripts `frun` and `fsub` for those new modes also. Both new modes will accept
  the `--wait` argument, meaning that they will block until the jobs complete.
- Documentation overhauled to update API and add instructions on creating a new batch system,
  these instructions are duplicated in the README within the `batch_systems` package folder.
- **Local support temporarily removed**. It didn't work very well, and it broke the new
  batch system structure, I hope to add it back again shortly.
- Full support for array job parsing for both torque and slurm. We now create on job entry
  for each array job child, instead of for each array job. To manage this, the
  `fyrd.queue.Queue.QueueJob` class was moved to `fyrd.queue.QueueJob` and split to add a 
  child class, `fyrd.queue.QueueChild`. All array jobs not have one `fyrd.queue.QueueJob`
  job, plus one `fyrd.queue.QueueChild` job for each of their children, which are stored
  in the `children` dictionary in the `fyrd.queue.QueueJob` class.
- Added a `get` method to the `fyrd.queue.Queue` class to allow a user to get outputs from
  a list of jobs, loops continuously through the jobs so that jobs are not lost.
- Added `tqdm <https://pypi.python.org/pypi/tqdm>`_ as a requirement and enabled progressbars
  in multi-job wait and get

Minor Changes
.............

- Updated the documentation to include this changelog, which will only contain change information
  for version 0.6.2a1 onwards.
- Added additional tests to cover the new changes as well as generally increase test suite
  coverage.
- Several small bug fixes
