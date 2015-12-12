"""
Submit job when the total number of jobs in the queue drops below the max
provided by max= or defined in ~/.slurmy
"""
from pyslurm import job
from .slurmy import submit_job
from pwd import getpwnam
from os import environ
from time import time

default_max_jobs = 1000


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
        if int(time()) - self.full_queue.lastUpdate() > 60:
            self._load()


def monitor_submit(script_file, dependency=None, max_count=default_max_jobs):
    """ Check length of queue and submit if possible """
    q = queue()
    if q.job_count < max_count:
        submit_job(script_file, dependency)

##
# The End #
