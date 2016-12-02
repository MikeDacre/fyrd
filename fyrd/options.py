# -*- coding: utf-8 -*-
"""
Available options for job submission.

All keyword arguments that can be used with Job() objects are defined in this
file. These can be edited by the end user to increase functionality.

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
from textwrap import wrap as _wrap
from itertools import groupby
from collections import OrderedDict

from tabulate import tabulate as _tabulate

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
      'slurm': '--ntasks {}',   # Torque handled separately
      'default': 1, 'type': int}),
    ('modules',
     {'help': 'Modules to load with the `module load` command',
      'default': None, 'type': list}),
    ('imports',
     {'help': 'Imports to be used in function calls (e.g. sys, os)',
      'default': None, 'type': list}),
    ('syspaths',
     {'help': 'Paths to add to sys.path for submitted functions',
      'default': None, 'type': list}),
    ('scriptpath',
     {'help': 'Folder to write cluster script files to, must be accessible ' +
              'to the compute nodes.',
      'default': '.', 'type': str}),
    ('outpath',
     {'help': 'Folder to write cluster output files to, must be accessible ' +
              'to the compute nodes.',
      'default': '.', 'type': str}),
    ('runpath',
     {'help': 'The working directory for the job',
      'default': '.', 'type': str,
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
      'slurm': '--cpus-per-task {}',   # Torque handled separately
      'default': 1, 'type': int}),
    ('features',
     {'help': 'A comma-separated list of node features to require',
      'slurm': '--constraint={}',  # Torque handled separately
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


SYNONYMS = OrderedDict([
    ('depend',         'depends'),
    ('dependency',     'depends'),
    ('dependencies',   'depends'),
    ('stdout',         'outfile'),
    ('stderr',         'errfile'),
    ('queue',          'partition'),
    ('memory',         'mem'),
    ('cpus',           'cores'),
    ('walltime',       'time'),
    ('delete_files',   'clean_files'),
    ('delete_outputs', 'clean_outputs'),
    ('filedir',        'scriptpath'),
    ('filepath',       'scriptpath'),
    ('dir',            'runpath'),
    ('path',           'runpath'),
    ('paths',          'syspaths'),
    ('syspath',        'syspaths'),
    ('scriptdir',      'scriptpath'),
    ('cleanfiles',     'clean_files'),
    ('delfiles',       'clean_files'),
    ('cleanouts',      'clean_outputs'),
    ('delouts',        'clean_outputs'),
    ('deloutputs',     'clean_outputs'),
    ('cleanoutputs',   'clean_outputs'),
])


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


def sanitize_arguments(kwds):
    """Run check_arguments, but return unmatched keywords as is."""
    new_kwds = dict()
    for opt, arg in kwds.items():
        try:
            o, a = list(check_arguments({opt: arg}).items())[0]
            new_kwds[o] = a
        except OptionsError:
            new_kwds[opt] = arg
    return new_kwds


def split_keywords(kwargs):
    """Split a dictionary of keyword arguments into two dictionaries.

    The first dictionary will contain valid arguments for fyrd, the second will
    contain all others.

    Returns:
        tuple: (dict, dict) —  valid args for fyrd, other args
    """
    if not isinstance(kwargs, dict):
        raise ValueError('Invalid argument. Should be a dictionary, is {}'
                         .format(type(kwargs)))
    good = {}
    bad  = {}
    for key, val in kwargs.items():
        try:
            good.update(check_arguments({key: val}))
        except OptionsError:
            bad.update({key: val})
    return check_arguments(good), bad


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
            new_kwds[arg] = opt

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
            return ''
        return '{prefix} {optarg}'.format(
            prefix=prefix, optarg=kwds[option][qtype].format(value))
    else:
        return '{prefix} {option}'.format(prefix=prefix,
                                          option=kwds[option][qtype])


def options_to_string(option_dict, qtype=None):
    """Return a multi-line string for slurm or torque job submission.

    Args:
        option_dict (dict): Dict in format {option: value} where value can be
                            None. If value is None, default used.
        qtype (str):        'torque', 'slurm', or 'local': override queue.MODE

    Returns:
        str: A multi-line string of torque or slurm options.
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

    # Set path if required
    if 'filepath' in option_dict:
        filepath = os.path.abspath(option_dict.pop('filepath'))
        if 'outfile' in option_dict:
            option_dict['outfile'] = os.path.join(
                filepath, os.path.basename(option_dict['outfile']))
        if 'errfile' in option_dict:
            option_dict['errfile'] = os.path.join(
                filepath, os.path.basename(option_dict['errfile']))

    # Loop through all options
    outlist = []
    for option, value in option_dict.items():
        outlist.append(option_to_string(option, value, qtype))

    return '\n'.join(outlist)


def option_help(mode='string', qtype=None, tablefmt='simple'):
    """Print a sting to stdout displaying information on all options.

    Args:
        mode (str):     string:       Return a formatted string
                        print:        Print the string to stdout
                        list:         Return a simple list of keywords
                        table:        Return a table of lists
                        merged_table: Combine all keywords into a single table
        qtype (str):    If provided only return info on that queue type.
        tablefmt (str): A tabulate-style table format, one of::

            'plain', 'simple', 'grid', 'pipe', 'orgtbl',
            'rst', 'mediawiki', 'latex', 'latex_booktabs'

    Returns:
        str: A formatted string
    """

    hlp = OrderedDict()

    # Explicitly get the function call help out of core to treat separately
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
        for hlp_info in hlp.values():
            tmpstr = ''
            for option, inf in hlp_info['help'].items():
                default   = inf['default'] if 'default' in inf else None
                typ       = inf['type']
                helpitems = _wrap(inf['help'])
                helpstr   = helpitems[0]
                if len(helpitems) > 1:
                    hstr     = '\n' + ' '*15
                    helpstr += hstr
                    helpstr += hstr.join(helpitems[1:])
                if isinstance(typ, (tuple, list, set)):
                    typ = [t.__name__ for t in typ]
                else:
                    typ = typ.__name__
                tmpstr += ('{o:<15}{h}\n{s:<15}Type: {t}; Default: {d}\n'
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
        for sect, ddct in hlp.items():
            summary = '{}: {}'.format(sect.title(), ddct['summary'])
            outtable = [['Option', 'Description', 'Type', 'Default']]
            dct = ddct['help']
            for opt, inf in dct.items():
                if isinstance(inf['type'], (tuple, list, set)):
                    typ = [t.__name__ for t in inf['type']]
                else:
                    typ = inf['type'].__name__
                outtable.append([
                    opt,
                    inf['help'],
                    typ,
                    str(inf['default'])
                ])
            tables[summary] = outtable

        tables['Synonyms'] = [
            ['Synonym', 'Option']
        ] + [
            list(i) for i in SYNONYMS.items()
        ]

        out_string = ''
        for section, table in tables.items():
            out_string += '\n' + section + '\n'
            out_string += '-'*len(section) + '\n\n'
            out_string += _tabulate(
                table, headers='firstrow', tablefmt=tablefmt
            ) + '\n\n'

        return out_string

    elif mode == 'merged_table':
        table = []
        headers  = ['Option', 'Description', 'Type', 'Default', 'Section']
        for sect, ddct in hlp.items():
            dct = ddct['help']
            for opt, inf in dct.items():
                if isinstance(inf['type'], (tuple, list, set)):
                    typ = [t.__name__ for t in inf['type']]
                else:
                    typ = inf['type'].__name__
                table.append([
                    opt,
                    inf['help'],
                    typ,
                    str(inf['default']),
                    sect
                ])
        out_string  = _tabulate(
            table, headers=headers, tablefmt=tablefmt
        ) + '\n\n'
        out_string += 'Synonyms\n'
        out_string += '-'*8 + '\n\n'
        out_string += _tabulate(
            [list(i) for i in SYNONYMS.items()],
            headers=['Synonym', 'Option'],
            tablefmt=tablefmt
        )

        return out_string

    elif mode == 'list':
        return '\n'.join(['\n'.join(i['help'].keys()) for i in hlp.values()])

    else:
        raise ClusterError('mode must be "print", "string", or "table"')
