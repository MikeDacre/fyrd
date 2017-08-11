# -*- coding: utf-8 -*-
"""
SLURM parsing functions.
"""
import os as _os
import re as _re
import sys as _sys
import pwd as _pwd     # Used to get usernames for queue
from subprocess import CalledProcessError as _CalledProcessError

from .. import run as _run
from .. import logme as _logme
from .. import ClusterError as _ClusterError
from .. import script_runners as _scrpts
from .. import submission_scripts as _sscrpt
_Script = _sscrpt.Script


PREFIX = '#SBATCH'


###############################################################################
#                           Normalization Functions                           #
###############################################################################


def normalize_job_id(job_id):
    """Convert the job id into job_id, array_id."""
    if '_' in job_id:
        job_id, array_id = job_id.split('_')
        job_id = job_id.strip()
        array_id = array_id.strip()
    else:
        array_id = None
    return job_id, array_id


def normalize_state(state):
    """Convert state into standadized (slurm style) state."""
    return state


###############################################################################
#                               Job Submission                                #
###############################################################################


def gen_scripts(job_object, command, args, precmd, modstr):
    """Build the submission script objects.

    Creates an exec script as well as a submission script.
    """
    scrpt = _os.path.join(
        job_object.scriptpath, '{}.{}.sbatch'.format(
            job_object.name, job_object.suffix
        )
    )

    # We use a separate script and a single srun command to avoid
    # issues with multiple threads running at once
    exec_script  = _os.path.join(
        job_object.scriptpath, '{}.{}.script'.format(
            job_object.name, job_object.suffix
        )
    )
    exe_script   = _scrpts.CMND_RUNNER_TRACK.format(
        precmd=modstr, usedir=job_object.runpath, name=job_object.name,
        command=command
    )

    # Create the exec_script Script object
    exec_script_obj = _Script(
        script=exe_script, file_name=exec_script
    )

    ecmnd = 'srun bash {}'.format(exec_script)
    sub_script = _scrpts.SCRP_RUNNER.format(
        precmd=precmd, script=exec_script, command=ecmnd
    )

    submission_script = _Script(script=sub_script, file_name=scrpt)

    return submission_script, exec_script_obj


def submit(file_name, dependencies=None, job=None, args=None, kwds=None):
    """Submit any file with dependencies to Torque.

    Parameters
    ----------
    file_name : str
        Path to an existing file
    dependencies : list
        List of dependencies
    job : fyrd.job.Job, not implemented
        A job object for the calling job, not used by this functions
    args : list, not implemented
        A list of additional command line arguments to pass when submitting,
        not used by this function
    kwds : dict or str, not implemented
        A dictionary of keyword arguments to parse with options_to_string, or
        a string of option:value,option,option:value,.... Not used by this
        function.

    Returns
    -------
    job_id : str
    """
    _logme.log('Submitting to slurm', 'debug')
    if dependencies:
        deps = '--dependency=afterok:{}'.format(
            ':'.join([str(d) for d in dependencies]))
        args = ['sbatch', deps, file_name]
    else:
        args = ['sbatch', file_name]

    # Try to submit job 5 times
    code, stdout, stderr = _run.cmd(args, tries=5)
    if code == 0:
        job_id, _ = normalize_job_id(stdout.split(' ')[-1])
    else:
        _logme.log('sbatch failed with code {}\n'.format(code) +
                   'stdout: {}\nstderr: {}'.format(stdout, stderr),
                   'critical')
        raise _CalledProcessError(code, args, stdout, stderr)

    return job_id


###############################################################################
#                               Job Management                                #
###############################################################################


def kill(job_ids):
    """Terminate all jobs in job_ids.

    Parameters
    ----------
    job_ids : list or str
        A list of valid job ids or a single valid job id

    Returns
    -------
    success : bool
    """
    o = _run.cmd('scancel {0}'.format(' '.join(_run.listify(job_ids))),
                 tries=5)
    if o[0] == 0:
        return True
    else:
        return False


###############################################################################
#                                Queue Parsing                                #
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
    nodequery = _re.compile(r'([^\[,]+)(\[[^\[]+\])?')
    qargs = ['squeue', '-h', '-O',
             'jobid:400,arraytaskid:400,name:400,userid:400,partition:400,' +
             'state:400,nodelist:400,numnodes:400,numcpus:400,exit_code:400']
    # Parse queue info by length
    squeue = [
        tuple(
            [k[i:i+200].rstrip() for i in range(0, 4000, 400)]
        ) for k in _run.cmd(qargs)[1].split('\n')
    ]
    # SLURM sometimes clears the queue extremely fast, so we use sacct
    # to get old jobs by the current user
    qargs = ['sacct', '-p',
             '--format=jobid,jobname,user,partition,state,' +
             'nodelist,reqnodes,ncpus,exitcode']
    try:
        sacct = [tuple(i.strip(' |').split('|')) for i in
                 _run.cmd(qargs)[1].split('\n')]
        sacct = sacct[1:]
    # This command isn't super stable and we don't care that much, so I will
    # just let it die no matter what
    except Exception as e:
        if _logme.MIN_LEVEL == 'debug':
            raise e
        else:
            sacct = []

    if sacct:
        if len(sacct[0]) != 9:
            _logme.log('sacct parsing failed unexpectedly as there are not ' +
                       '9 columns, aborting.', 'critical')
            raise ValueError('sacct output does not have 9 columns. Has:' +
                             '{}: {}'.format(len(sacct[0]), sacct[0]))
        jobids = [i[0] for i in squeue]
        for sinfo in sacct:
            # Skip job steps, only index whole jobs
            if '.' in sinfo[0]:
                _logme.log('Skipping {} '.format(sinfo[0]) +
                           "in sacct processing as it is a job part.",
                           'verbose')
                continue
            # These are the values I expect
            try:
                [sid, sname, suser, spartition, sstate,
                 snodelist, snodes, scpus, scode] = sinfo
                sid, sarr = normalize_job_id(sid)
            except ValueError as err:
                _logme.log('sacct parsing failed with error {} '.format(err) +
                           'due to an incorrect number of entries.\n' +
                           'Contents of sinfo:\n{}\n'.format(sinfo) +
                           'Expected 10 values\n:' +
                           '[sid, sarr, sname, suser, spartition, sstate, ' +
                           'snodelist, snodes, scpus, scode]',
                           'critical')
                raise
            # Skip jobs that were already in squeue
            if sid in jobids:
                _logme.log('{} still in squeue output'.format(sid),
                           'verbose')
                continue
            scode = int(scode.split(':')[-1])
            squeue.append((sid, sarr, sname, suser, spartition, sstate,
                           snodelist, snodes, scpus, scode))
    else:
        _logme.log('No job info in sacct', 'debug')

    # Sanitize data
    for sinfo in squeue:
        if len(sinfo) == 10:
            [sid, sarr, sname, suser, spartition, sstate, sndlst,
             snodes, scpus, scode] = sinfo
        else:
            _sys.stderr.write('{}'.format(repr(sinfo)))
            raise _ClusterError('Queue parsing error, expected 10 items '
                                'in output of squeue and sacct, got {}\n'
                                .format(len(sinfo)))
        if partition and spartition != partition:
            continue
        if not isinstance(sid, str):
            sid = str(sid) if sid else None
        else:
            sarr = None
        if not isinstance(snodes, int):
            snodes = int(snodes) if snodes else None
        if not isinstance(scpus, int):
            scpus = int(scpus) if snodes else None
        if not isinstance(scode, int):
            scode = int(scode) if scode else None
        sstate = sstate.lower()
        # Convert user from ID to name
        if suser.isdigit():
            suser = _pwd.getpwuid(int(suser)).pw_name
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

        yield (sid, sarr, sname, suser, spartition, sstate, snodelist,
               snodes, scpus, scode)


def parse_strange_options(option_dict):
    """Parse all options that cannot be handled by the regular function.

    Parameters
    ----------
    option_dict : dict
        All keyword arguments passed by the user that are not already defined
        in the Job object

    Returns
    -------
    list
        A list of strings to be added at the top of the script file
    dict
        Altered version of option_dict with all options that can't be handled
        by `fyrd.batch_systems.options.option_to_string()` removed.
    None
        Would contain additional arguments to pass to sbatch, but these are not
        needed so we just return None
    """
    outlist = []
    # Handle cores separately
    nodes = int(option_dict.pop('nodes')) if 'nodes' in option_dict else 1
    cores = int(option_dict.pop('cores')) if 'cores' in option_dict else 1

    outlist.append('#SBATCH --ntasks {}'.format(nodes))
    outlist.append('#SBATCH --cpus-per-task {}'.format(cores))

    return outlist, option_dict, None
