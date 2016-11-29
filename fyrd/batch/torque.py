"""
Functions and attributes to allow torque queue functionality.
"""

IDENTIFYING_SCRIPTS = ['qsub', 'qstat']

###############################################################################
#                                    Queue                                    #
###############################################################################


def queue_parser(user=None, partition=None):
    """Iterator for torque queues.

    Use the `qstat -x` command to get an XML queue for compatibility.

    Args:
        user:     optional user name to pass to qstat to filter queue with
        partiton: optional partition to filter the queue with

    Yields:
        tuple: job_id, name, userid, partition, state, nodelist, numnodes,
               ntpernode, exit_code

    numcpus is currently always 1 as most torque queues treat every core as a
    node.
    """
    # I am not using run.cmd because I want to catch XML errors also
    try_count = 0
    qargs = ['qstat', '-x']
    while True:
        try:
            xmlqueue = ET.fromstring(check_output(qargs))
        except CalledProcessError:
            sleep(1)
            if try_count == 5:
                raise
            else:
                try_count += 1
        except ET.ParseError:
            # ElementTree throws error when string is empty
            sleep(1)
            if try_count == 1:
                xmlqueue = None
                break
            else:
                try_count += 1
        else:
            break

    # Create QueueJob objects for all entries that match user
    if xmlqueue is not None:
        for xmljob in xmlqueue:
            job_id    = xmljob.find('Job_Id').text.split('.')[0]
            if '[' in job_id:
                job_id, array_id = job_id.split('[')
                array_id = array_id.strip('[]')
                if array_id:
                    array_id = int(array_id)
                else:
                    array_id = 0
            else:
                array_id = None
            try:
                job_id = int(job_id)
            except ValueError:
                pass
            job_owner = xmljob.find('Job_Owner').text.split('@')[0]
            if user and job_owner != user:
                continue
            job_name  = xmljob.find('Job_Name').text
            job_queue = xmljob.find('queue').text
            job_state = xmljob.find('job_state').text
            job_state = TORQUE_SLURM_STATES[job_state]
            logme.log('Job {} state: {}'.format(job_id, job_state),
                      'debug')
            ndsx = xmljob.find('exec_host')
            if ndsx:
                nds = ndsx.text.split('+')
            else:
                nds = []
            nodes = []
            for node in nds:
                if '-' in node:
                    nm, num = node.split('/')
                    for i in range(*[int(i) for i in num.split('-')]):
                        nodes.append(nm + '/' + str(i).zfill(2))
                else:
                    nodes.append(node)
            # I assume that every 'node' is a core, as that is the
            # default for torque, but it isn't always true
            job_threads  = len(nodes)
            exitcode     = xmljob.find('exit_status')
            if hasattr(exitcode, 'text'):
                exitcode = int(exitcode.text)

            if partition and job_queue != partition:
                continue
            yield (job_id, array_id, job_name, job_owner, job_queue, job_state,
                   nodes, job_threads, 1, exitcode)
