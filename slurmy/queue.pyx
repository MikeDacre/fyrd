"""
Description:   Submit job when the total number of jobs in the queue drops below the max
               provided by max= or defined in ~/.slurmy

Created:       2015-12-11
Last modified: 2015-12-11 23:14
"""
from time import time
from time import sleep
from pwd import getpwnam
from os import environ
from sys import stderr

# Our imports
from pyslurm import job
from .slurmy import submit_file
from . import _defaults

# We only need the defaults for this section
_defaults = _defaults['queue']


class queue():
    """ Functions that need to access the slurm queue """
    def __init__(self):
        super(queue, self).__init__()
        self.uid = getpwnam(environ['USER']).pw_uid
        self.full_queue = job()
        self._load()

    def _load(self):
        self.current_job_ids = self.full_queue.find('user_id', self.uid)
        self.job_count = len(self.current_job_ids)
        self.queue = {}
        for k, v in self.full_queue.get().items():
            if k in self.current_job_ids:
                self.queue[k] = v

    def load(self):
        if int(time()) - self.full_queue.lastUpdate() > int(_defaults['queue_update']):
            self._load()

    def get_job_count(self):
        """ If job count not updated recently, update it """
        self.load()
        return self.job_count


def monitor_submit_file(script_file, dependency=None, max_count=int(_defaults['max_jobs'])):
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
