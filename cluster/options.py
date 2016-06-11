"""
Available options for job submission.

============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2016-31-17 08:04
 Last modified: 2016-06-10 20:50

============================================================================
"""
import os
from itertools import groupby
from textwrap import dedent

from . import logme
from . import THREADS

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
COMMON  = {'modules':
           {'help': 'Modules to load with the `module load` command',
            'default': None, 'type': list},
           'imports':
           {'help': 'Imports to be used in function calls (e.g. sys, os)',
            'default': None, 'type': list},
           'filedir':
           {'help': 'Folder to write cluster files to.',
            'default': '.', 'type': str},
           'dir':
           {'help': 'The working directory for the job',
            'default': 'path argument', 'type': str,
            'slurm': '--workdir={}', 'torque': '-d {}'},
           'suffix':
           {'help': 'A suffix to append to job files (e.g. job.suffix.qsub)',
            'default': 'cluster', 'type': str},
           'outfile':
           {'help': 'File to write STDOUT to',
            'default': None, 'type': str,
            'slurm': '-o {}', 'torque': '-o {}'},
           'errfile':
           {'help': 'File to write STDERR to',
            'default': None, 'type': str,
            'slurm': '-e {}', 'torque': '-e {}'},
          }

# Options used in only local runs
NORMAL  = {'threads':
           {'help': 'Number of threads to use on the local machine',
            'default': THREADS, 'type': int}
          }

# Options used in both torque and slurm
CLUSTER_CORE = {'nodes':
                {'help': 'Number of nodes to request',
                 'default': 1, 'type': int},
                'cores':
                {'help': 'Number of cores to use for the job',
                 'default': 1, 'type': int},
                'features':
                {'help': 'A comma-separated list of node features to require',
                 'slurm': '--constraint={}', # Torque in options_to_string()
                 'default': None, 'type': list, 'sjoin': '&'},
                'time':
                {'help': 'Walltime in HH:MM:SS',
                 'default': '12:00:00', 'type': str,
                 'slurm': '--time={}',
                 'torque': 'walltime={}'},
                'mem':
                {'help': 'Memory to use in MB (e.g. 4000)',
                 'default': 4000, 'type': (int, str),
                 'slurm': '--mem={}',
                 'torque': 'mem={}MB'},  # We explictly set MB in torque
                'partition':
                {'help': 'The partition/queue to run in (e.g. normal/batch)',
                 'default': None, 'type': str,
                 'slurm': '-p {}',
                 'torque': '-q {}'},
               }

### Note: There are many more options, as them as need to the following lists,
###       CLUSTER_OPTS should be used for options that work on both systems,
###       the TORQUE and SLURM dictionaries should be used for options that are
###       unique to one.

# Additional options shared between systems
CLUSTER_OPTS = {'account':
                {'help': 'Account to be charged', 'default': None, 'type': str,
                 'slurm': '--account={}', 'torque': '-A {}'},
                'export':
                {'help': 'Comma separated list of environmental variables to export',
                 'default': None, 'type': str,
                 'slurm': '--export={}', 'torque': '-v {}'}
               }

###############################################################################
#                                Torque Options                               #
#  from: adaptivecomputing.com/torque/4-0-2/Content/topics/commands/qsub.htm  #
###############################################################################

TORQUE =  {}

#####################################################
#                   SLURM Options                   #
#  from: http://slurm.schedmd.com/pdfs/summary.pdf  #
#####################################################

SLURM  = {'begin':
          {'help': 'Start after this much time',
           'slurm': '--begin={}', 'type': str},
         }


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
ALLOWED_KWDS = {}
for name, info in ALL_KWDS.items():
    ALLOWED_KWDS[name] = info['type'] if 'type' in info else None

###############################################################################
#                          Option Handling Functions                          #
###############################################################################


def check_arguments(kwargs):
    """Make sure all keywords are allowed.

    Raises Exception on error, returns sanitized dictionary on success.
    """
    new_kwds = {}
    # Make sure types are correct
    for arg, opt in kwargs.items():
        if arg not in ALLOWED_KWDS:
            raise Exception('Unrecognized argument {}'.format(arg))
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
                raise Exception('time must be formatted as D-HH:MM:SS ' +
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
                    raise ValueError('mem is malformatted, should be a number of ' +
                                    'MB or a string like 24MB or 10GB, ' +
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
                    raise Exception('Unknown memory unit opt {}'.format(sunit))
                # Don't allow 0, minimum memory req is 5MB
                if opt < 5:
                    opt = 5

    return new_kwds


def option_to_string(option, value=None, qtype=None):
    """Return a string with an appropriate flag for slurm or torque.

    :option: An allowed option definied in options.all_options
    :value:  A value for that option if required (if None, default used)
    :qtype:  'torque', 'slurm', or 'normal': override queue.MODE
    """
    # Import a couple of queue functions here
    from . import queue
    qtype = qtype if qtype else queue.MODE
    queue.check_queue(qtype)

    option = option.rstrip()

    if option == 'cores' or option == 'nodes':
        raise Exception('Cannot handle cores or nodes here, use ' +
                        'options_to_string')

    if qtype == 'slurm':
        kwds = SLURM_KWDS
    elif qtype == 'torque':
        kwds = TORQUE_KWDS
    elif qtype == 'normal':
        return ''  # There is no need of this in normal mode

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
                raise Exception('{} requires a value'.format(option))

    # Return formatted string
    prefix = '#SBATCH' if qtype == 'slurm' else '#PBS'
    if '{}' in kwds[option][qtype]:
        if value is None:
            raise Exception('Cannot use None as an argument for option {}'
                            .format(option))
        return '{prefix} {optarg}'.format(
            prefix=prefix, optarg=kwds[option][qtype].format(value))
    else:
        return '{prefix} {option}'.format(prefix=prefix,
                                          option=kwds[option][qtype])


def options_to_string(option_dict, qtype=None):
    """Return a multi-line string for slurm or torque job submission.

    :option_dict: Dict in format {option: value} where value can be None.
                  If value is None, default used.
    :qtype:       'torque', 'slurm', or 'normal': override queue.MODE
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


def option_help(qtype=None):
    """Return a sting displaying information on all options.

    :qtype: If provided only return info on that queue type.
    """

    core_help = dedent("""\
    Options available in all modes::
    :cores:   How many cores to run on or threads to use.
    :depends: A list of dependencies for this job, must be either
              Job objects (required for normal mode) or job numbers.
    :suffix:  The name to use in the output and error files
    :dir:     The working directory to run in. Defaults to current.
    """)

    func_help = dedent("""\
    Used for function calls::
    :imports: A list of imports, if not provided, defaults to all current
                imports, which may not work if you use complex imports.
                The list can include the import call, or just be a name, e.g
                ['from os import path', 'sys']
    """)

    norm_help = dedent("""\
    Used only in normal mode::
    :threads:   How many threads to use in the multiprocessing pool.
                Defaults to all.
    """)

    c_help = dedent("""\
    Used for torque and slurm::
    :nodes:     The number of nodes to request.
                Type: int; Default: 1
    :time:      The time to run for in HH:MM:SS.
                Type: str; Default: 00:02:00
    :mem:       Memory to use in MB.
                Type: int; Default: 4000
    :partition: Partition/queue to run on
                Type: str; Default: 'normal' in slurm, 'batch' in torque.
    :modules:   Modules to load with the 'module load' command.
                Type: list; Default: None.
    """)

    for option, inf in CLUSTER_OPTS.items():
        c_help += ("{opt:<10} {help}\nType: {type}; Default: {default}\n"
                   .format(opt=':{}:'.format(option), help=inf['help'],
                           type=inf['type'].__name__,
                           default=inf['default']))

    t_help = "\nUsed for torque only::\n"
    for option, info in TORQUE.items():
        c_help += ("{opt:<10} {help}\nType: {type}; Default: {default}\n"
                   .format(opt=':{}:'.format(option), help=info['help'],
                           type=info['type'].__name__,
                           default=info['default']))

    s_help = "\nUsed for slurm only::\n"
    for option, info in SLURM.items():
        typ  = info['type'].__name__ if 'type' in info else None
        dflt = info['default'] if 'default' in info else None
        c_help += ("{opt:<10} {help}\nType: {typ}; Default: {default}\n"
                   .format(opt=':{}:'.format(option), help=info['help'],
                           typ=typ, default=dflt))

    outstr = core_help + func_help

    if qtype:
        if qtype == 'normal':
            outstr += norm_help
        elif qtype == 'slurm':
            outstr += c_help + s_help
        elif qtype == 'torque':
            outstr += c_help + t_help
        else:
            raise Exception('qtype must be "torque", "slurm", or "normal"')
    else:
        outstr += norm_help + c_help + t_help + s_help

    return outstr
