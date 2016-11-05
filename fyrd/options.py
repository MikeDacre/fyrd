# -*- coding: utf-8 -*-
"""
Available options for job submission.

Last modified: 2016-11-04 14:33

All keyword arguments that can be used with Job() objects are defined in this
file. These can be editted by the end user to increase functionality.

Options are defined in dictionaries with the syntax:
    'name': {'slurm': The command to be used for slurm
             'torque': The command to be used for torque
             'default': The default to use if not set
             'type': The python object type for the option
             'help': A string with help information}

All of these fields are required except in the case that:
    1. The option is managed in options_to_string explicitly
    2. The option is in NORMAL, TORQUE, or SLURM dictionaries, in which case
       flags used by other queue systems can be skipped.
"""
import os
import sys
from textwrap import wrap
from itertools import groupby
from collections import OrderedDict

from . import run
from . import logme
from . import ClusterError

__all__ = ['option_help']

###############################################################################
#                       Possible Job Submission Options                       #
###############################################################################

# Defined by dictionaries:
#      type: python type to convert option into
#     slurm: the string to format args into for slurm
#    torque: the string to format args into for torque
# [s|t]join: used to join list types for slurm or torque
#      help: Info for the user on the option

# Options available in all modes
COMMON  = OrderedDict([
    ('depends',
     {'help': 'A job or list of jobs to depend on',
      'default': None, 'type': list}),
    ('clean_files',
     {'help': 'Auto clean script files when fetching outputs',
      'default': None, 'type': bool}),
    ('clean_outputs',
     {'help': 'Auto clean output files when fetching outputs',
      'default': None, 'type': bool}),
    ('cores',
     {'help': 'Number of cores to use for the job',
      'default': 1, 'type': int}),
    ('modules',
     {'help': 'Modules to load with the `module load` command',
      'default': None, 'type': list}),
    ('imports',
     {'help': 'Imports to be used in function calls (e.g. sys, os) '
              'if not provided, defaults to all current imports, which '
              'may not work if you use complex imports. The list can include '
              'the import call, or just be a name, e.g. '
              "['from os import path', 'sys']",
      'default': None, 'type': list}),
    ('filedir',
     {'help': 'Folder to write cluster files to, must be accessible ' +
              'to the compute nodes.',
      'default': '.', 'type': str}),
    ('dir',
     {'help': 'The working directory for the job',
      'default': 'path argument', 'type': str,
      'slurm': '--workdir={}', 'torque': '-d {}'}),
    ('suffix',
     {'help': 'A suffix to append to job files (e.g. job.suffix.qsub)',
      'default': 'cluster', 'type': str}),
    ('outfile',
     {'help': 'File to write STDOUT to',
      'default': None, 'type': str,
      'slurm': '-o {}', 'torque': '-o {}'}),
    ('errfile',
     {'help': 'File to write STDERR to',
      'default': None, 'type': str,
      'slurm': '-e {}', 'torque': '-e {}'}),
])

# Options used in only local runs
NORMAL  = OrderedDict([
    ('threads',
     {'help': 'Number of threads to use on the local machine',
      'default': 4, 'type': int}),
])

# Options used in both torque and slurm
CLUSTER_CORE = OrderedDict([
    ('nodes',
     {'help': 'Number of nodes to request',
      'default': 1, 'type': int}),
    ('features',
     {'help': 'A comma-separated list of node features to require',
      'slurm': '--constraint={}',  # Torque in options_to_string()
      'default': None, 'type': list, 'sjoin': '&'}),
    ('time',
     {'help': 'Walltime in HH:MM:SS',
      'default': '12:00:00', 'type': str,
      'slurm': '--time={}', 'torque': 'walltime={}'}),
    # We explictly set MB in torque
    ('mem',
     {'help': 'Memory to use in MB (e.g. 4000)',
      'default': 4000, 'type': (int, str),
      'slurm': '--mem={}', 'torque': 'mem={}MB'}),
    ('partition',
     {'help': 'The partition/queue to run in (e.g. local/batch)',
      'default': None, 'type': str,
      'slurm': '-p {}', 'torque': '-q {}'}),
])

# Note: There are many more options, as them as need to the following lists,
#       CLUSTER_OPTS should be used for options that work on both systems,
#       the TORQUE and SLURM dictionaries should be used for options that are
#       unique to one.

# Additional options shared between systems
CLUSTER_OPTS = OrderedDict([
    ('account',
     {'help': 'Account to be charged', 'default': None, 'type': str,
      'slurm': '--account={}', 'torque': '-A {}'}),
    ('export',
     {'help': 'Comma separated list of environmental variables to export',
      'default': None, 'type': str,
      'slurm': '--export={}', 'torque': '-v {}'}),
])

###############################################################################
#                                Torque Options                               #
#  from: adaptivecomputing.com/torque/4-0-2/Content/topics/commands/qsub.htm  #
###############################################################################

TORQUE = OrderedDict()

#####################################################
#                   SLURM Options                   #
#  from: http://slurm.schedmd.com/pdfs/summary.pdf  #
#####################################################

SLURM  = OrderedDict([
    ('begin',
     {'help': 'Start after this much time',
      'slurm': '--begin={}', 'type': str,
      'default': None}),
])

################################################################
#                         SYNONYMS                             #
#  These allow alternate keyword arguments for common options  #
################################################################


SYNONYMS = {
    'depend':       'depends',
    'dependency':   'depends',
    'dependencies': 'depends',
    'stdout':       'outfile',
    'stderr':       'errfile',
    'queue':        'partition',
    'memory':       'mem',
    'cpus':         'cores',
    'walltime':     'time',
}


###############################################################################
#                       DO NOT EDIT BELOW THIS LINE!!!                        #
###############################################################################


###############################################################################
#                     Composites for Checking and Lookup                      #
###############################################################################


SLURM_KWDS = COMMON.copy()
for kds in [CLUSTER_CORE, CLUSTER_OPTS, SLURM]:
    SLURM_KWDS.update(kds)

TORQUE_KWDS = COMMON.copy()
for kds in [CLUSTER_CORE, CLUSTER_OPTS, TORQUE]:
    TORQUE_KWDS.update(kds)

CLUSTER_KWDS = SLURM_KWDS.copy()
CLUSTER_KWDS.update(TORQUE_KWDS)

NORMAL_KWDS = COMMON.copy()
for kds in [NORMAL]:
    NORMAL_KWDS.update(kds)

ALL_KWDS = CLUSTER_KWDS.copy()
ALL_KWDS.update(NORMAL_KWDS)

# Will be 'name' -> type
ALLOWED_KWDS = OrderedDict()
for name, info in ALL_KWDS.items():
    ALLOWED_KWDS[name] = info['type'] if 'type' in info else None


###############################################################################
#                      Option Handling Custom Exception                       #
###############################################################################

class OptionsError(ClusterError):

    """A custom Exception for failures in option parsing."""

    pass


###############################################################################
#                          Option Handling Functions                          #
###############################################################################


def check_arguments(kwargs):
    """Make sure all keywords are allowed.

    Raises OptionsError on error, returns sanitized dictionary on success.

    Note: Checks in SYNONYMS if argument is not recognized, raises OptionsError
          if it is not found there either.
    """
    new_kwds = {}
    # Make sure types are correct
    for arg, opt in kwargs.items():
        if arg not in ALLOWED_KWDS:
            if arg in SYNONYMS:
                arg = SYNONYMS[arg]
                assert arg in ALLOWED_KWDS
            else:
                raise OptionsError('Unrecognized argument {}'.format(arg))
        if opt is not None and not isinstance(opt, ALLOWED_KWDS[arg]):
            try:
                newtype = ALLOWED_KWDS[arg]
                if (newtype is list or newtype is tuple) \
                        and not isinstance(arg, (list, tuple)):
                    if newtype is list:
                        opt2 = [opt]
                    elif newtype is tuple:
                        opt2 = (opt,)
                    else:
                        raise Exception("Shouldn't be here")
                else:
                    opt2 = newtype(opt)
            except:
                raise TypeError('arg must be {}, is {}'.format(
                    ALLOWED_KWDS[arg], type(opt)))
            new_kwds[arg] = opt2
        else:
            new_kwds[arg] = opt

    # Parse individual complex options
    for arg, opt in new_kwds.items():
        if arg == 'time':
            try:
                if '-' in opt:
                    day, time = opt.split('-')
                else:
                    day = 0
                    time = opt
                time = [int(i) for i in time.split(':')]
                if len(time) == 3:
                    hours, mins, secs = time
                elif len(time) == 2:
                    hours = 0
                    mins, secs = time
                elif len(time) == 1:
                    hours = mins = 0
                    secs = time[0]
                hours = (int(day)*24) + hours
                opt = '{}:{}:{}'.format(str(hours).rjust(2, '0'),
                                        str(mins).rjust(2, '0'),
                                        str(secs).rjust(2, '0'))
                new_kwds[arg] = opt
            except:
                raise OptionsError('time must be formatted as D-HH:MM:SS ' +
                                   'or a fragment of that (e.g. MM:SS) ' +
                                   'it is formatted as {}'.format(opt))

        # Force memory into an integer of megabytes
        elif arg == 'mem' and isinstance(opt, str):
            if opt.isdigit():
                opt = int(opt)
            else:
                # Try to guess unit by suffix
                try:
                    groups = groupby(opt, key=str.isdigit)
                except ValueError:
                    raise ValueError('mem is malformatted, should be a number '
                                     'of MB or a string like 24MB or 10GB, '
                                     'it is: {}'.format(opt))
                sval  = int(''.join(next(groups)[1]))
                sunit = ''.join(next(groups)[1]).lower()
                if sunit == 'b':
                    opt = int(float(sval)/float(1024)/float(1024))
                elif sunit == 'kb' or sunit == 'k':
                    opt = int(float(sval)/float(1024))
                elif sunit == 'mb' or sunit == 'm':
                    opt = sval
                elif sunit == 'gb' or sunit == 'g':
                    opt = sval*1024
                elif sunit == 'tb' or sunit == 't':
                    # Crazy people
                    opt = sval*1024*1024
                else:
                    raise ValueError('Unknown memory unit opt {}'
                                     .format(sunit))
                # Don't allow 0, minimum memory req is 5MB
                if opt < 5:
                    opt = 5

    return new_kwds


def option_to_string(option, value=None, qtype=None):
    """Return a string with an appropriate flag for slurm or torque.

    Args:
        option: An allowed option definied in options.all_options
        value:  A value for that option if required (if None, default used)
        qtype:  'torque', 'slurm', or 'local': override queue.MODE

    Returns:
        str: A string with the appropriate flags for the active queue.
    """
    # Import a couple of queue functions here
    from . import queue
    qtype = qtype if qtype else queue.MODE
    queue.check_queue(qtype)

    if isinstance(option, dict):
        raise ValueError('Arguments to option_to_string cannot be '
                         'dictionaries, you probably want options_to_string')

    option = str(option).rstrip()

    if option == 'cores' or option == 'nodes':
        raise OptionsError('Cannot handle cores or nodes here, use ' +
                           'options_to_string')

    if qtype == 'slurm':
        kwds = SLURM_KWDS
    elif qtype == 'torque':
        kwds = TORQUE_KWDS
    elif qtype == 'local':
        return ''  # There is no need of this in local mode
    else:
        # This should never happen
        raise ClusterError('Invalid qtype {}'.format(qtype))

    # Make sure argument allowed
    option, value = list(check_arguments({option: value}).items())[0]

    # Fail with debug error if option not available in this mode
    if option in ALLOWED_KWDS and option not in kwds:
        logme.log('{} not available in {} mode.'.format(option, qtype),
                  'debug')
        return ''

    # Try to get default
    if not value:
        if not kwds[option]['type'] == bool:
            if 'default' in kwds[option]:
                value = kwds[option]['default']
                logme.log('Using default value {} for {}'
                          .format(value, option), 'debug')
            else:
                raise OptionsError('{} requires a value'.format(option))

    # Return formatted string
    prefix = '#SBATCH' if qtype == 'slurm' else '#PBS'
    if '{}' in kwds[option][qtype]:
        if value is None:
            raise OptionsError('Cannot use None as an argument for option {}'
                               .format(option))
        return '{prefix} {optarg}'.format(
            prefix=prefix, optarg=kwds[option][qtype].format(value))
    else:
        return '{prefix} {option}'.format(prefix=prefix,
                                          option=kwds[option][qtype])


def options_to_string(option_dict, qtype=None):
    """Return a multi-line string for slurm or torque job submission.

    Args:
        option_dict: Dict in format {option: value} where value can be None.
                     If value is None, default used.
        qtype:       'torque', 'slurm', or 'local': override queue.MODE

    Returns:
        str: A multiline string of torque or slurm options.
    """
    # Import a couple of queue functions here
    from . import queue

    # Sanitize arguments
    if not isinstance(option_dict, dict):
        raise TypeError('option_dict must be dict is {}'.format(
            type(option_dict)))

    option_dict = check_arguments(option_dict.copy())

    qtype = qtype if qtype else queue.MODE

    queue.check_queue(qtype)

    outlist = []

    # Handle cores separately
    nodes = int(option_dict.pop('nodes')) if 'nodes' in option_dict else 1
    cores = int(option_dict.pop('cores')) if 'cores' in option_dict else 1

    # Set path if required
    if 'filedir' in option_dict:
        filedir = os.path.abspath(option_dict.pop('filedir'))
        if 'outfile' in option_dict:
            option_dict['outfile'] = os.path.join(
                filedir, os.path.basename(option_dict['outfile']))
        if 'errfile' in option_dict:
            option_dict['errfile'] = os.path.join(
                filedir, os.path.basename(option_dict['errfile']))

    if qtype == 'slurm':
        outlist.append('#SBATCH --ntasks {}'.format(nodes))
        outlist.append('#SBATCH --cpus-per-task {}'.format(cores))
    elif qtype == 'torque':
        outstring = '#PBS -l nodes={}:ppn={}'.format(nodes, cores)
        if 'features' in option_dict:
            outstring += ':' + ':'.join(
                run.opt_split(option_dict.pop('features'), (',', ':')))
        outlist.append(outstring)

    # Loop through all options
    for option, value in option_dict.items():
        outlist.append(option_to_string(option, value, qtype))

    return '\n'.join(outlist)


def option_help(qtype=None, mode='string'):
    """Print a sting to stdout displaying information on all options.

    Args:
        qtype: If provided only return info on that queue type.
        mode:  string: Return a formatted string
               print:  Print the string to stdout
               table:  Return a table of lists

    Returns:
        str: A formatted string
    """

    hlp = OrderedDict()

    # Expicitly get the function call help out of core to treat separately
    common = COMMON.copy()
    impts  = common.pop('imports')

    hlp['common'] = {
        'summary': 'Used in every mode',
        'help': common,
    }

    hlp['func'] = {
        'summary': 'Used for function calls',
        'help': OrderedDict([('imports', impts)]),
    }

    hlp['local'] = {
        'summary': 'Used only in local mode',
        'help': NORMAL,
    }

    # Include all cluster options in one
    cluster = CLUSTER_CORE.copy()
    cluster.update(CLUSTER_OPTS)
    hlp['cluster'] = {
        'summary': 'Options that work in both slurm and torque',
        'help': cluster,
    }

    if TORQUE:
        hlp['torque'] = {
            'summary': "Used for torque only",
            'help': TORQUE,
        }

    if SLURM:
        hlp['slurm'] = {
            'summary': "Used for slurm only",
            'help': SLURM,
        }

    if qtype:
        if qtype == 'local':
            hlp.pop('cluster')
            hlp.pop('torque')
            hlp.pop('slurm')
        elif qtype == 'slurm':
            hlp.pop('torque')
        elif qtype == 'torque':
            hlp.pop('slurm')
        else:
            raise ClusterError('qtype must be "torque", "slurm", or "local"')

    if mode == 'print' or mode == 'string':
        outstr = ''
        for option_class, hlp_info in hlp.items():
            tmpstr = ''
            for option, inf in hlp_info['help'].items():
                default = inf['default'] if 'default' in inf else None
                typ = inf['type']
                helpitems = wrap(inf['help'])
                helpstr   = helpitems[0]
                if len(helpitems) > 1:
                    helpstr  += '\n            '
                    helpstr  += '\n            '.join(helpitems[1:])
                if isinstance(typ, (tuple, list, set)):
                    typ = [t.__name__ for t in typ]
                else:
                    typ = typ.__name__
                tmpstr += ('{o:<12}{h}\n{s:<12}Type: {t}; Default: {d}\n'
                           .format(o=option + ':', h=helpstr, s=' ',
                                   t=typ, d=default))
            outstr += '{}::\n{}\n'.format(hlp_info['summary'], tmpstr)
        outstr = outstr.rstrip() + '\n'

        if mode == 'print':
            sys.stdout.write(outstr)
        else:
            return outstr
    elif mode == 'table':
        tables = OrderedDict()
        for option_class, hlp_info in hlp.items():
            tmptable = []
            for option, inf in hlp_info['help'].items():
                helpitems = wrap(inf['help'])
                default = inf['default'] if 'default' in inf else None
                typ = inf['type']
                if isinstance(typ, (tuple, list, set)):
                    typ = [t.__name__ for t in typ]
                else:
                    typ = typ.__name__
                tmptable.append((option, '{:<70}'
                                 .format(helpitems[0])))
                if len(helpitems) > 1:
                    for helpitem in helpitems[1:]:
                        tmptable.append(('', '{}'
                                         .format(helpitem)))
                tmptable.append(('', 'Type: {}; Default: {}'
                                 .format(typ, default)))
            tables[option_class] = {'summary': hlp_info['summary'],
                                    'table': tmptable}
        return tables
    else:
        raise ClusterError('mode must be "print", "string", or "table"')
