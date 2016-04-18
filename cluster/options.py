"""
Available options for job submission.

============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2016-31-17 08:04
 Last modified: 2016-04-18 15:28

============================================================================
"""
from itertools import groupby
from textwrap import dedent

from . import logme

###############################################################################
#                       Possible Job Submission Options                       #
###############################################################################


# Options available in all modes
COMMON  = {'modules':
           {'help': 'Modules to load with the `module load` command',
            'default': None, 'type': 'list'},
           'imports':
           {'help': 'Imports to be used in function calls (e.g. sys, os)',
            'default': None, 'type': 'list'}
           'dir':
           {'help': 'The working directory for the job',
            'default': 'path argument', type: str,
            'slurm': '--workdir={}', 'torque': '-d {}'},
           'suffix':
           {'help': 'A suffix to append to job files (e.g. job.suffix.qsub)',
            'default': 'cluster', type: str},
           }

# Options used in only local runs
NORMAL  = {'threads':
           {'help': 'Number of threads to use on the local machine',
            'default': THREADS, 'type': 'int'}
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
                 'default': None, 'type': list},
                'time':
                {'help': 'Walltime in HH:MM:SS',
                 'default': '12:00:00', 'type': str,
                 'slurm': '--time={}',
                 'torque': 'walltime={}'},
                'mem':
                {'help': 'Memory to use in MB (e.g. 4000)',
                 'default': 4000, type: (int, str),
                 'slurm': '--mem={}',
                 'torque': 'mem={}MB'},  # We explictly set MB in torque
                'partition':
                {'help': 'The partition/queue to run in (e.g. normal/batch)',
                 'default': None, type: str,
                 'slurm': '-p {}',
                 'torque': '-q {}'},
                }
CLUSTER_OPTS = {'account':
                {'help': 'Account to be charged', default: None, type: str,
                 'slurm': '--account={}', 'torque': '-A {}'},
                'export':
                {'help': 'Comma separated list of environmental variables to export',
                 'default': None, 'type': str,
                 'slurm': '--export={}', 'torque': '-v {}'}
                }

TORQUE =  {}

#####################################################
#                   SLURM Options                   #
#  from: http://slurm.schedmd.com/pdfs/summary.pdf  #
#####################################################

SLURM  = {'begin':
          {'help': 'Start after this much time',
           'slurm': '--begin={}'},
          }


old = {
            'exclusive':    'No other jobs may run on node',
            'export':       'Export these variables (dictionary)',
            'gres':         'Generic resources required per node',
            'input':        'File to read job input data from',
            'job-name':     'Job name',
            'licenses':     'Licenses required for job',
            'mem':          'Memory required in MB (int)',
            'mem_per_cpu':  'Memory per cpu in MB (int)',
            'N':            'Number of nodes required',
            'n':            'Number of tasks launched per node',
            'nodelist':     'Hosts job allowed to run on',
            'output':       'File where output will be writen',
            'partition':    'Partition/queue to run in',
            'qos':          'QOS specification (e.g. dev)',
            'signal':       'Signal to send to job when approaching time',
            'time':         'Time limit for job (d-HH:MM:SS)',
            'wrap':         'Wrap specified command as shell script'}


###############################################################################
#                     Composites for Checking and Lookup                      #
###############################################################################


SLURM_KWDS = COMMON.copy()
for kwds in [CLUSTER_CORE, CLUSTER_OPTS, SLURM]:
    SLURM_KWDS.update(kwds)

TORQUE_KWDS = COMMON.copy()
for kwds in [CLUSTER_CORE, CLUSTER_OPTS, TORQUE]:
    TORQUE_KWDS.update(kwds)

CLUSTER_KWDS = SLURM_KWDS.copy()
CLUSTER_KWDS.update(TORQUE_KWDS)

NORMAL_KWDS = COMMON.copy()
for kwds in [NORMAL]:
    NORMAL_KWDS.update(kwds)

ALL_KWDS = CLUSTER_KWDS.copy()
ALL_KWDS.update(NORMAL_KWDS)

# Will be 'name' -> type
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
    for arg, opt in kwargs:
        if arg not in ALLOWED_KWDS:
            raise Exception('Unrecognized argument {}'.format(arg))
        if opt is not None and not isinstance(opt, ALLOWED_KWDS[arg]):
            try:
                opt2 = ALLOWED_KWDS[arg](opt)
            except:
                raise TypeError('arg must be {}, is {}'.format(
                    ALLOWED_KWDS[arg], type(opt)))
            new_kwds[arg] = opt2
        else:
            new_kwds[arg] = opt
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
    check_arguments({option: value})

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

    # Handle time carefully
    if option == 'time':
        if not ':' in value or '-' in value:
            raise Exception('time must be formatted as HH:MM:SS ' +
                            'it is formatted as {}'.format(value))

    # Sanitize memory argument
    if option == 'mem' and isinstance(value, str):
        if value.isdigit:
            value = int(value)
        else:
            # Try to guess unit by suffix
            parts = groupby(value, key=str.isdigit)
            if len(parts) != 2:
                raise TypeError('mem is malformatted, should be a number of ' +
                                'MB or a string like 24MB or 10GB, ' +
                                'it is: {}'.format(value))
            sval  = int(parts[0])
            sunit = parts[1].lower()
            if sunit == 'b':
                value = sval*1024*1024
            elif sunit == 'kb' or sunit == 'k':
                value = sval*1024
            elif sunit == 'mb' or sunit == 'm':
                value = sval
            elif sunit == 'gb' or sunit == 'g':
                value = int(sval/1024)
            elif sunit == 'tb' or sunit == 't':
                # Crazy people
                value = int(sval/1024/1024)
            else:
                raise Exception('Unknown memory unit value {}'.format(sunit))

    # Return formatted string
    prefix = '#SBATCH' if qtype == 'slurm' else '#PBS'
    if '{}' in kwds[option][qtype]:
        if value is None:
            raise Exception('Cannot use None as an argument for option {}'
                            .format(option))
        return '{prefix} {optarg}\n'.format(
            prefix=prefix, optarg=kwds[option][qtype].format(value))
    else:
        return '{prefix} {option}\n'.format(prefix=prefix,
                                            option=kwds[option][qtype])


def options_to_string(option_dict, qtype=None):
    """Return a list with strings for slurm or torque job submission.

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
    if qtype == 'slurm':
        outlist.append('#SBATCH --ntasks {}'.format(nodes))
        outlist.append('#SBATCH --cpus-per-task {}'.format(cores))
    elif qtype == 'torque':
        outlist.append('#PBS -l nodes={}:ppn={}'.format(nodes, cores))

    # Loop through all options
    for option, value in option_dict.items():
        outlist.append(option_to_string(option, value, qtype))
    return outlist


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

    for option, info in cluster_opts.items():
        c_help += "{opt:<10} {help}\nType: {type}; Default: {default}\n"
            .format(opt=':{}:'.format(option), help=info['help'],
            type=info['type'].__name__, default=info['default'])

    t_help = "\nUsed for torque only::\n"
    for option, info in torque.items():
        c_help += "{opt:<10} {help}\nType: {type}; Default: {default}\n"
            .format(opt=':{}:'.format(option), help=info['help'],
            type=info['type'].__name__, default=info['default'])

    s_help = "\nUsed for slurm only::\n"
    for option, info in slurm.items():
        c_help += "{opt:<10} {help}\nType: {type}; Default: {default}\n"
            .format(opt=':{}:'.format(option), help=info['help'],
            type=info['type'].__name__, default=info['default'])

    outstr = core_help + func_help

    if qtype:
        if qtype == 'normal':
            outstr += norm_help
        elif qtype == 'slurm':
            outstr += c_help + s_help
        elif qtype == 'torque':
            outstr += c+help + t+help
        else:
            raise Exception('qtype must be "torque", "slurm", or "normal"')
    else:
        outstr += norm_help + c_help + t_help + s_help

    return outstr
