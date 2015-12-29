
"""
Description:   Submit job when the total number of jobs in the queue drops below the max
               provided by max= or defined in ~/.slurmy

Created:       2015-12-11
Last modified: 2015-12-29 12:27
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

    def load(self):
        if int(time()) - self.full_queue.lastUpdate() > int(_defaults['queue_update']):
            self._load()

    def get_running_jobs(self):
        """ Create a self.running dictionary from the self.queue dictionary """
        self._update_running_jobs()
        return self.running

    def get_queued_jobs(self):
        """ Create a self.running dictionary from the self.queue dictionary """
        self._update_queued_jobs()
        return self.queued

    def get_job_count(self):
        """ If job count not updated recently, update it """
        self.load()
        return self.job_count

    #####################
    # Private Functions #
    #####################
    def _update_running_jobs(self):
        """ Update the self.running dictionary with only queued jobs """
        self.load()
        self.running = {}
        j = b'RUNNING'
        for k, v in self.queue.items():
            if v['job_state'] == j:
                self.running[k] = v

    def _update_queued_jobs(self):
        """ Update the self.queued dictionary with only queued jobs """
        self.load()
        self.queued = {}
        j = b'PENDING'
        for k, v in self.queue.items():
            if v['job_state'] == j:
                self.queued[k] = v

    ###################
    # Class Functions #
    ###################
    def _load(self):
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

    def __init__(self):
        self.uid = getpwnam(environ['USER']).pw_uid
        self.full_queue = job()
        self._load()

    def __getattr__(self, key):
        if key == 'running':
            return self.get_running_jobs()
        if key == 'queued':
            return self.get_queued_jobs()

    def __getitem__(self, key):
        self.load()
        try:
            return self.queue[key]
        except KeyError:
            return None

    def __len__(self):
        self.load()
        return self.get_job_count()

    def __repr__(self):
        self.load()
        r = []
        for k, v in self.queue.items():
            r.append(str(k) + ':')
            for i, j in v.items():
                r.append('\t{0:>20}:\t{1:<}'.format(repr(i), repr(j)))
        return '\n'.join(r)


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
