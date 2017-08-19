# -*- coding: utf-8 -*-
"""
Run Queue objects as a daemon with an SQLAlchemy back-end.

Implements an SQLAlchemy database that holds queue information and a child
class of fyrd.queue.Queue—QueueManager—that runs as a daemon and periodically
updates the database.
"""
from itertools import product as _prod

import Pyro4

from . import queue
from . import run as _run
from . import batch_systems as _batch


@Pyro4.expose
class QueueManager(object):

    """A server wrapper of fyrd.queue.Queue that uses Pyro4.

    Initializes a bunch of queue.Queue objects and stores them in self.queues
    and then updates all of them. Runs as a Pyro4 daemon, on initialization
    spawns a thread that continuously loops through all queues and updates the
    database.

    Serves job information to any asking process from the database, will be
    called by a Queue object initialized by `fyrd.queue.Queue.with_server()`.
    Called by the `_update()` method to return a list of jobs.

    Proper useage is to first start the daemon with `fyrd queue_manager start`
    and then create a queue object. If a queue object is initialize while the
    server is running, it will automatically use the backed server, unless it
    is initializes by `fyrd.queue.Queue.no_server()`.
    """


    def __init__(self, users=None, partitions=None, qtypes=None):
        """Can filter by user, queue type or partition on initialization.

        .. note:: creates a Queue object for every combination of the above,
            so do not do too many.

        Parameters
        ----------
        users : list of str, optional
            Optional username to filter the queue with.  If user='self' or
            'current', the current user will be used.
        partitions : list of str, optional
            Optional partition to filter the queue with.
        qtypes : list of str
            one of the defined batch queues (e.g. 'slurm')
            Default is auto detected
        """
        combinations = []
        if users:
            combinations.append(_run.listify(users))
        if partitions:
            combinations.append(_run.listify(partitions))
        if qtypes:
            combinations.append(_run.listify(qtypes))
        self.queues = []
        for comb in _prod(*combinations):
            q = queue.Queue(*comb)
            q.show_pb = False
            self.queues.append(q)
        super(QueueManager, self).__init__()

    @Pyro4.expose
    def update():
        pass



