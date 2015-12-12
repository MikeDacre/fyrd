"""
Submit job when the total number of jobs in the queue drops below the max
provided by max= or defined in ~/.slurmy
"""
from pyslurm import job
from .slurmy import submit_job
from pwd import getpwnam
from os import environ
from time import time
from time import sleep
from sys import stderr

default_max_jobs     = 1000     # Max number of jobs in queue
default_sleep_len    = 5        # Between submission attempts (in seconds)
default_queue_update = 20       # Amount of time between getting fresh queue info (seconds)


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
        if int(time()) - self.full_queue.lastUpdate() > default_queue_update:
            self._load()

    def get_job_count(self):
        """ If job count not updated recently, update it """
        self.load()
        return self.job_count


def monitor_submit(script_file, dependency=None, max_count=default_max_jobs):
    """ Check length of queue and submit if possible """
    q = queue()
    notify = True
    while True:
        if q.get_job_count() < int(max_count):
            return submit_job(script_file, dependency)
        else:
            if notify:
                stderr.write('INFO --> Queue length is ' + str(q.job_count) +
                             '. Max queue length is ' + str(max_count) +
                             ' Will attempt to resubmit every ' + str(default_sleep_len) +
                             ' seconds\n')
                notify = False
            sleep(default_sleep_len)

##
# The End #
