"""
Functions and attributes to allow torque queue functionality.
"""

IDENTIFYING_SCRIPTS = ['squeue', 'sbatch']

###############################################################################
#                                    Queue                                    #
###############################################################################


def queue_parser(user=None, partition=None):
    """Iterator for slurm queues.

    Use the `squeue -O` command to get standard data across implementation,
    supplement this data with the results of `sacct`. sacct returns data only
    for the current user but retains a much longer job history. Only jobs not
    returned by squeue are added with sacct, and they are added to *the end* of
    the returned queue, i.e. *out of order with respect to the actual queue*.

    Args:
        user:      optional user name to filter queue with
        partition: optional partition to filter queue with

    Yields:
        tuple: job_id, name, userid, partition, state, nodelist, numnodes,
               ntpernode, exit_code
    """
    nodequery = re.compile(r'([^\[,]+)(\[[^\[]+\])?')
    qargs = ['squeue', '-h', '-O',
             'jobid:400,name:400,userid:400,partition:400,state:400,' +
             'nodelist:400,numnodes:400,numcpus:400,exit_code:400']
    # Parse queue info by length
    squeue = [
        tuple(
            [k[i:i+200].rstrip() for i in range(0, 3600, 400)]
        ) for k in run.cmd(qargs)[1].split('\n')
    ]
    # SLURM sometimes clears the queue extremely fast, so we use sacct
    # to get old jobs by the current user
    qargs = ['sacct', '-p',
             '--format=jobid,jobname,user,partition,state,' +
             'nodelist,reqnodes,ncpus,exitcode']
    try:
        sacct = [tuple(i.strip(' |').split('|')) for i in
                 run.cmd(qargs)[1].split('\n')]
        sacct = sacct[1:]
    # This command isn't super stable and we don't care that much, so I will
    # just let it die no matter what
    except Exception as e:
        if logme.MIN_LEVEL == 'debug':
            raise e
        else:
            sacct = []

    if sacct:
        if len(sacct[0]) != 9:
            logme.log('sacct parsing failed unexpectedly as there are not ' +
                      '9 columns, aborting.', 'critical')
            raise ValueError('sacct output does not have 9 columns. Has:' +
                             '{}: {}'.format(len(sacct[0]), sacct[0]))
        jobids = [i[0] for i in squeue]
        for sinfo in sacct:
            # Skip job steps, only index whole jobs
            if '.' in sinfo[0]:
                logme.log('Skipping {} '.format(sinfo[0]) +
                          "in sacct processing as it is a job part.",
                          'verbose')
                continue
            # These are the values I expect
            try:
                [sid, sname, suser, spartition, sstate,
                 snodelist, snodes, scpus, scode] = sinfo
            except ValueError as err:
                logme.log('sacct parsing failed with error {} '.format(err) +
                          'due to an incorrect number of entries.\n' +
                          'Contents of sinfo:\n{}\n'.format(sinfo) +
                          'Expected 9 values\n:' +
                          '[sid, sname, suser, spartition, sstate, ' +
                          'snodelist, snodes, scpus, scode]',
                          'critical')
                raise
            # Skip jobs that were already in squeue
            if sid in jobids:
                logme.log('{} still in squeue output'.format(sid), 'verbose')
                continue
            scode = int(scode.split(':')[-1])
            squeue.append((sid, sname, suser, spartition, sstate,
                           snodelist, snodes, scpus, scode))
    else:
        logme.log('No job info in sacct', 'debug')

    # Sanitize data
    for sinfo in squeue:
        if len(sinfo) == 9:
            [sid, sname, suser, spartition, sstate, sndlst,
             snodes, scpus, scode] = sinfo
        else:
            sys.stderr.write('{}'.format(repr(sinfo)))
            raise ClusterError('Queue parsing error, expected 8 or 9 items '
                               'in output of squeue and sacct, got {}\n'
                               .format(len(sinfo)))
        if partition and spartition != partition:
            continue
        if not isinstance(sid, int):
            sid = int(sid) if sid else None
        if not isinstance(snodes, int):
            snodes = int(snodes) if snodes else None
        if not isinstance(scpus, int):
            scpus = int(scpus) if snodes else None
        if not isinstance(scode, int):
            scode = int(scode) if scode else None
        # Convert user from ID to name
        if suser.isdigit():
            suser = pwd.getpwuid(int(suser)).pw_name
        if user and suser != user:
            continue
        # Attempt to parse nodelist
        snodelist = []
        if sndlst:
            if nodequery.search(sndlst):
                nsplit = nodequery.findall(sndlst)
                for nrg in nsplit:
                    node, rge = nrg
                    if not rge:
                        snodelist.append(node)
                    else:
                        for reg in rge.strip('[]').split(','):
                            # Node range
                            if '-' in reg:
                                start, end = [int(i) for i in reg.split('-')]
                                for i in range(start, end):
                                    snodelist.append('{}{}'.format(node, i))
                            else:
                                snodelist.append('{}{}'.format(node, reg))
            else:
                snodelist = sndlst.split(',')

        yield (sid, sname, suser, spartition, sstate, snodelist,
               snodes, scpus, scode)
