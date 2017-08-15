# -*- coding: utf-8 -*-
"""
Define functions for using the Torque batch system
"""
import os as _os
import re as _re
from time import sleep as _sleep
import xml.etree.ElementTree as _ET
from subprocess import check_output as _check_output
from subprocess import CalledProcessError as _CalledProcessError

from .. import run as _run
from .. import conf as _conf
from .. import logme as _logme
from .. import ClusterError as _ClusterError
from .. import script_runners as _scrpts
from .. import submission_scripts as _sscrpt
_Script = _sscrpt.Script


PREFIX = '#PBS'
SUFFIX = '.qsub'
PATH = _os.path.dirname(_conf.get_executable('queue', 'qsub', 'qsub'))


# Define torque-to-slurm mappings
TORQUE_SLURM_STATES = {
    'C': 'completed',
    'E': 'completing',
    'H': 'held',  # Not a SLURM state
    'Q': 'pending',
    'R': 'running',
    'T': 'suspended',
    'W': 'running',
    'S': 'suspended',
}


###############################################################################
#                             Functionality Test                              #
###############################################################################


def queue_test(warn=True):
    """Check that torque can be used.

    Just looks for qsub and qstat.

    Parameters
    ----------
    warn : bool
        log a warning on fail

    Returns
    -------
    batch_system_functional : bool
    """
    log_level = 'error' if warn else 'debug'
    qsub = _conf.get_option('queue', 'qsub')
    if qsub is not None and _os.path.dirname(qsub) and not _run.is_exe(qsub):
        _logme.log(
            'Cannot use torque as qsub path set in conf to {0}'
            .format(qsub) + ' but that path is not an executable',
            log_level
        )
        return False
    qsub = qsub if qsub else 'qsub'
    qsub = _run.which(qsub) if not _os.path.dirname(qsub) else qsub
    if not qsub:
        _logme.log(
            'Cannot use torque as cannot find qsub', log_level
        )
        return False
    qpath = _os.path.dirname(qsub)
    qstat = _os.path.join(qpath, 'qstat')
    return _run.is_exe(qstat)


###############################################################################
#                           Normalization Functions                           #
###############################################################################


def normalize_job_id(job_id):
    """Convert the job id into job_id, array_id."""
    job_id = job_id.split('.')[0]
    if '[' in job_id:
        job_id, array_id = job_id.split('[')
        job_id = job_id.strip('[]')
        array_id = array_id.strip('[]')
        if not array_id:
            array_id = None
    else:
        array_id = None
    return job_id, array_id


def normalize_state(state):
    """Convert state into standadized (slurm style) state."""
    if state.upper() in TORQUE_SLURM_STATES:
        state = TORQUE_SLURM_STATES[state.upper()]
    return state


###############################################################################
#                           Job Sumission Functions                           #
###############################################################################


def gen_scripts(job_object, command, args, precmd, modstr):
    """Create script object for job, does not create a sep. exec script.

    Parameters
    ---------
    job_object : fyrd.job.Job
    command : str
        Command to execute
    args : list
        List of additional arguments, not used in this script.
    precmd : str
        String from options_to_string() to add at the top of the file, should
        contain batch system directives
    modstr : str
        String to add after precmd, should contain module directives.

    Returns
    -------
    fyrd.script_runners.Script
        The submission script
    None
        Would be the exec_script, not used here.
    """
    scrpt = _os.path.join(
        job_object.scriptpath,
        '{0}.{1}.{2}'.format(job_object.name, job_object.suffix, SUFFIX)
    )

    sub_script = _scrpts.CMND_RUNNER_TRACK.format(
        precmd=precmd, usedir=job_object.runpath, name=job_object.name,
        command=command
    )
    return _Script(script=sub_script, file_name=scrpt), None


def submit(file_name, dependencies=None, job=None, args=None, kwds=None):
    """Submit any file with dependencies to Torque.

    Parameters
    ----------
    file_name : str
        Path to an existing torque submission file
    dependencies : list, optional
        List of torque job IDs as dependencies
    job : fyrd.job.Job, not implemented
        A job object for the calling job, not used by this functions
    args : list, not implemented
        A list of additional command line arguments to pass when submitting,
        not used by this function
    kwds : dict or str, not implemented
        A dictionary of keyword arguments to parse with options_to_string, or
        a string of option:value,option,option:value,.... Not used by this
        function.

    Returns:
        job_id (str)
    """
    _logme.log('Submitting to torque', 'debug')
    if dependencies:
        deps = '-W depend={}'.format(
            ','.join(['afterok:' + str(d) for d in dependencies]))
        args = ['qsub', deps, file_name]
    else:
        args = ['qsub', file_name]

    # Try to submit job 5 times
    code, stdout, stderr = _run.cmd(args, tries=5)
    if code == 0:
        job_id, _ = normalize_job_id(stdout.split('.')[0])
    elif code == 17 and 'Unable to open script file' in stderr:
        _logme.log('qsub submission failed due to an already existing '
                   'script file, attempting to rename file and try '
                   'again.\nstderr: {}, stdout: {}, cmnd: {}'
                   .format(stderr, stdout, args), 'error')
        new_name = args[1] + '.resub'
        _os.rename(args[1], new_name)
        _logme.log('renamed script {} to {}, resubmitting'
                   .format(args[1], new_name), 'info')
        args[1] = new_name
        code, stdout, stderr = _run.cmd(args, tries=5)
        if code == 0:
            job_id, _ = normalize_job_id(stdout.split('.')[0])
        else:
            _logme.log('Resubmission still failed, aborting',
                       'critical')
            raise _CalledProcessError(code, args, stdout, stderr)
    else:
        if stderr.startswith('qsub: submit error ('):
            raise _ClusterError('qsub submission failed with error: ' +
                                '{}, command: {}'.format(stderr, args))
        else:
            _logme.log(
                'qsub failed with code {}\nstdout: {}\nstderr: {}'
                .format(code, stdout, stderr), 'critical'
            )
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
    o = _run.cmd('qdel {0}'.format(' '.join(_run.listify(job_ids))), tries=5)
    return o[0] == 0:


###############################################################################
#                           Queue Parsing Functions                           #
###############################################################################


def queue_parser(user=None, partition=None):
    """Iterator for torque queues.

    Use the `qstat -x -t` command to get an XML queue for compatibility.

    Parameters
    ----------
    user : str, optional
        User name to pass to qstat to filter queue with
    partiton : str, optional
        Partition to filter the queue with

    Yields
    ------
    job_id : str
    array_id : str or None
    name : str
    userid : str
    partition : str
    state :str
    nodelist : list
    numnodes : int
    cntpernode : int or None
    exit_code : int or Nonw

    cntpernode is currently always 1 as most torque queues treat every core as
    a node.
    """
    # I am not using run.cmd because I want to catch XML errors also
    try_count = 0
    qargs = ['qstat', '-x', '-t']
    r = _re.compile('<Variable_List>.*?</Variable_List>')
    while True:
        try:
            xmlstr = _check_output(qargs)
            try:
                xmlstr = xmlstr.decode()
            except AttributeError:
                pass
            # Get rid of the Variable_List as it is just the environment
            # and can sometimes have nonsensical characters.
            xmlstr = xmlstr.replace('\x1b', '')
            xmlstr = r.sub('', xmlstr)
            xmlqueue = _ET.fromstring(xmlstr)
        except _CalledProcessError:
            _sleep(1)
            if try_count == 5:
                raise
            else:
                try_count += 1
        except _ET.ParseError:
            # ElementTree throws error when string is empty
            _sleep(1)
            if try_count == 1:
                xmlqueue = None
                break
            else:
                try_count += 1
        else:
            break

    if xmlqueue is not None:
        for xmljob in xmlqueue:
            job_id, array_id = normalize_job_id(xmljob.find('Job_Id').text)
            job_owner = xmljob.find('Job_Owner').text.split('@')[0]
            if user and job_owner != user:
                continue
            job_name  = xmljob.find('Job_Name').text
            job_queue = xmljob.find('queue').text
            job_state = xmljob.find('job_state').text
            job_state = TORQUE_SLURM_STATES[job_state]
            _logme.log('Job {} state: {}'.format(job_id, job_state),
                       'debug')
            ndsx = xmljob.find('exec_host')
            if hasattr(ndsx, 'text') and ndsx.text:
                nds = ndsx.text.split('+')
            else:
                nds = []
            nodes = []
            # Convert node range to individual nodes in list
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
            else:
                exitcode = None

            if partition and job_queue != partition:
                continue

            # Torque doesn't have a variable scpu
            scpus = 1
            yield (job_id, array_id, job_name, job_owner, job_queue, job_state,
                   nodes, job_threads, scpus, exitcode)


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
        Would contain additional arguments to pass to qsub, but these are not
        needed so we just return None
    """
    outlist = []
    # Handle cores separately
    nodes = int(option_dict.pop('nodes')) if 'nodes' in option_dict else 1
    cores = int(option_dict.pop('cores')) if 'cores' in option_dict else 1

    outstring = '#PBS -l nodes={}:ppn={}'.format(nodes, cores)
    if 'features' in option_dict:
        outstring += ':' + ':'.join(
            _run.opt_split(option_dict.pop('features'), (',', ':')))
    if 'qos' in option_dict:
        outstring += ',qos={}'.format(option_dict.pop('qos'))
    outlist.append(outstring)

    return outlist, option_dict, None
