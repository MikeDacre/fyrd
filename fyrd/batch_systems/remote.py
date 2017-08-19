# -*- coding: utf-8 -*-
"""
Submit jobs with fyrd from remote calls.

Implements server and clients that will monitor `~/.fyrd/messages` for messages
in the format:

    `to_<uuid>.fyrd` and `from_<uuid>.fyrd`

The job server shoul be started on the host by `fyrd job_server start` and the
client by `fyrd job_client start`.

The server does not care about communication method, it will start a
fyrd.queue_daemon process to monitor the server and then monitor the messages
folder. The client can currently only use ssh for communication, but any
method that puts a file would work.

The server loops once per LOOP_DELAY (defaut is 0.01 seconds) and checks for
any files beginning with `to_`, it opens and deletes those files after
extracting the timestamp. It then writes a response to a corresponding
`from_` file with the same UUID with a response.

All files use [dill](https://pypi.python.org/pypi/dill) to serialize python
data and use the form::

    (subject, message)

subject is a string that makes a request of the server, message is an optional
corresponding object. For example, to submit a job, send an initialized job
object::

    ('submit', fyrd.job.Job)

The client process will write job info to a database and then periodically
(set by the CLIENT_CHECK config item, usually once a second) for the server
response by UUID. In this case, the server response would be in the
corresponding `from_<uuid>.fyrd` file and would have the information:

    ('job', fyrd.job.Job)

NOTE: A UUID is used once only, so the client will use a unique UUID for every
message and the server will respond only once with that UUID. It is up to the
client to request an update from the server on the running jon.

The Job object will be munged to use the queue mode of the running server and
then submitted. To get job information::

    ('get', job_id)

The server will then return:

    ('job_info', fyrd.queue.QueueJob)

This could also be a list::

    ('get', [job_id, job_id])

Response::

    ('job_info', {job_id: fyrd.queue.QueueJob, job_id: 'not_found'})

The server is intentionally written to not crash wherever possible, however
if it does crash, as long as the queue monitoring daemon remains running, it
should be able to be restarted with no problems.
"""

