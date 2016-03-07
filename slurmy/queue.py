
"""
Description:   Submit job when the total number of jobs in the queue drops below the max
               provided by max= or defined in ~/.slurmy

Created:       2015-12-11
Last modified: 2016-03-07 12:12
"""
from time import time
from time import sleep
from pwd import getpwnam
from os import environ
from sys import stderr

# pyslurm is required - https://github.com/gingergeeks/pyslurm
from pyslurm import job

# Our imports
from .slurmy import submit_file
from . import defaults

# We only need the defaults for this section
_defaults = defaults['queue']

# Funtions to import if requested
__all__ = ['queue', 'monitor_submit']


class queue(object):
    """ Functions that need to access the slurm queue """

    def wait(self, job_list):
        """ Block until all jobs in job_list are complete.

        Note: update time is dependant upon the queue_update parameter in
              your ~/.slurmy file.

              In addition, wait() will not return until between 1 and 3
              seconds after a job has completed, irrespective of queue_update
              time. This allows time for any copy operations to complete after
              the job exits.

        :job_list: int, string, list, or tuple of job ids.
        :returns:  True on success False or nothing on failure.
        """
        if not isinstance(job_list, (list, tuple)):
            job_list = [job_list]
        job_list = [int(i) for i in job_list]
        for jb in job_list:
            while True:
                self.load()
                not_found = 0
                # Allow two seconds to elapse before job is found in queue,
                # if it is not in the queue by then, raise exception.
                if jb not in self:
                    sleep(1)
                    not_found += 1
                    if not_found == 3:
                        raise self.QueueError('{} not in queue'.format(jb))
                    continue
                # Actually look for job in running/queued queues
                if jb in self.running.keys() or jb in self.queued.keys():
                    sleep(2)
                else:
                    break
        sleep(1)  # Sleep an extra second to allow post-run scripts to run.
        return True

    def get_job_count(self):
        """ If job count not updated recently, update it """
        self.load()
        return self.job_count

    def __init__(self):
        """ Create self. """
        self.uid = getpwnam(environ['USER']).pw_uid
        self.full_queue = job()
        self._load()

    ######################
    # Internal Functions #
    ######################
    def load(self):
        if int(time()) - self.full_queue.lastUpdate() > int(_defaults['queue_update']):
            self._load()

    def _get_running_jobs(self):
        """ Return only queued jobs """
        self.load()
        j = 'RUNNING'
        running = {}
        for k, v in self.queue.items():
            if v['job_state'].decode() == j:
                running[k] = v
        return running

    def _get_queued_jobs(self):
        """ Return only queued jobs """
        self.load()
        j = 'PENDING'
        queued = {}
        for k, v in self.queue.items():
            if isinstance(v['job_state'], bytes):
                v['job_state'] = v['job_state'].decode()
            if v['job_state'] == j:
                queued[k] = v
        return queued

    def _load(self):
        """ Refresh the list of jobs from the server. """
        try:
            self.current_job_ids = self.full_queue.find('user_id', self.uid)
        except ValueError:
            sleep(5)
            self._load()
        self.job_count = len(self.current_job_ids)
        self.queue = {}
        try:
            for k, v in self.full_queue.get().items():
                if k in self.current_job_ids:
                    self.queue[k] = v
        except ValueError:
            sleep(5)
            self._load()

    def __iter__(self):
        """ Loop through jobs. """
        for jb in self.queue.keys():
            yield jb

    def __getattr__(self, key):
        """ Make running and queued attributes dynamic. """
        if key == 'running':
            return self._get_running_jobs()
        if key == 'queued':
            return self._get_queued_jobs()

    def __getitem__(self, key):
        """ Allow direct accessing of jobs by key. """
        self.load()
        try:
            return self.queue[key]
        except KeyError:
            return None

    def __len__(self):
        """ Length is the total job count. """
        self.load()
        return self.get_job_count()

    def __repr__(self):
        """ For debugging. """
        self.load()
        return 'queue<{}>'.format(self.queue.keys())

    def __str__(self):
        """ A list of keys. """
        self.load()
        return str(self.queue.keys())

    ################
    #  Exceptions  #
    ################
    class QueueError(Exception):

        """ Simple Exception wrapper. """

        pass


def monitor_submit(script_file, dependency=None, max_count=int(_defaults['max_jobs'])):
    """ Check length of queue and submit if possible """
    q = queue()
    notify = True
    while True:
        if q.get_job_count() < int(max_count):
            return submit_file(script_file, dependency)
        else:
            if notify:
                stderr.write('INFO --> Queue length is ' + str(q.job_count) +
                             '. Max queue length is ' + str(max_count) +
                             ' Will attempt to resubmit every ' + str(_defaults['sleep_len']) +
                             ' seconds\n')
                notify = False
            sleep(int(_defaults['sleep_len']))

##
# The End #
