"""
Description:   Get and set global variables

Created:       2015-12-11
Last modified: 2016-02-25 13:43
"""
import os
import sys
from os import path    as _path
from os import environ as _environ
from os import system  as _system
from re import findall as _findall
from sys import stderr as _stderr
try:
    import configparser as _configparser
except ImportError:
    import ConfigParser as _configparser

################################################################################
#                            Configurable Defaults                             #
################################################################################

config_file = _environ['HOME'] + '/.slurmy'
config      = _configparser.ConfigParser()
defaults    = {}

__all__ = ['get_config', 'write_config']
initial_defaults = {}

# Different job run options
initial_defaults['jobs_small'] = {'nodes':        1,
                                  'cores':        1,
                                  'mem':          '4GB',
                                  'time':         '00:02:00'}
initial_defaults['jobs_large'] = {'nodes':        1,
                                  'cores':        16,
                                  'mem':          '64GB',
                                  'time':         '24:00:00'}

# Other options
initial_defaults['queue']      = {'max_jobs':     1000,  # Max number of jobs in queue
                                  'sleep_len':    5,     # Between submission attempts (in seconds)
                                  'queue_update': 20}    # Amount of time between getting fresh queue info (seconds)

#####################################################
#         Possible job submission commands          #
#  from: http://slurm.schedmd.com/pdfs/summary.pdf  #
#####################################################
commands = {'array': 'Job array spec',
            'account': 'Account to be charged',
            'begin': 'Start after this much time',
            'clusters': 'Clusters to run on',
            'constraint': 'Required node features',
            'cpu_per_task': 'Number of cpus per task',
            'dependency': 'Defer until specified ID completes',
            'error': 'File in which to store errors',
            'exclude': 'Host names to exclude',
            'exclusive': 'No other jobs may run on node',
            'export': 'Export these variables (dictionary)',
            'gres': 'Generic resources required per node',
            'input': 'File to read job input data from',
            'job-name': 'Job name',
            'licenses': 'Licenses required for job',
            'mem': 'Memory required in MB (int)',
            'mem_per_cpu': 'Memory per cpu in MB (int)',
            'N': 'Number of nodes required',
            'n': 'Number of tasks launched per node',
            'nodelist': 'Hosts job allowed to run on',
            'output': 'File where output will be writen',
            'partition': 'Partition/queue to run in',
            'qos': 'QOS specification (e.g. dev)',
            'signal': 'Signal to send to job when approaching time',
            'time': 'Time limit for job (d-HH:MM:SS)',
            'wrap': 'Wrap specified command as shell script'}

################################################################################
#                         Do Not Edit Below This Point                         #
################################################################################

############################
#  Profile Class Handling  #
############################


class Profile(object):
    """ A profile for job submission """
    nodes = None
    cores = None
    mem = None
    time = None
    def __init__(self):
        """ Set up bare minimum attributes """



#######################
#  General Functions  #
#######################


def get_initial_defaults():
    """ Return a sane set of starting defaults for config file creation """
    # Defined file wide
    return initial_defaults


def get_config():
    """ Load defaults from ~/.slurmy """
    global defaults, config
    defaults = {}
    if _path.isfile(config_file):
        config.read(config_file)
        defaults = _config_to_dict(config)
    else:
        defaults = create_config()
    return defaults


def write_config(section, key, value):
    """ Write a config key to the ~/.slurmy file"""
    global defaults, config
    config.read(config_file)
    if not config.has_section(section):
        config.add_section(section)
    config.set(section, key, value)
    with open(config_file, 'w') as outfile:
        config.write(outfile)
    return get_config()


def delete_config(section, key=None):
    """ Delete a config item
        If key is not provided deletes whole section """
    global defaults, config
    config.read(config_file)
    if key:
        config.remove_option(section, key)
    else:
        config.remove_section(section)
    with open(config_file, 'w') as outfile:
        config.write(outfile)
    return get_config()


def create_config():
    """ Create a ~/.slurmy file
        Will clobber an existing ~/.slurmy file """
    global defaults, config
    # Use defaults coded into this file
    defaults = get_initial_defaults()
    # Delete existing slurmy file
    if os.path.exists(config_file):
        os.remove(config_file)
    for k, v in defaults.items():
        if not config.has_section(k):
            config.add_section(k)
        for i, j in v.items():
            try:
                config.set(str(k), str(i), str(j))
            except TypeError:
                sys.stderr.write('{} {} {}\n'.format(k, i, j))
                raise
    with open(config_file, 'w') as outfile:
        config.write(outfile)
    _stderr.write('Created the file ~/.slurmy with default variables. ' +
                  'Please review this file and edit your defaults\n')
    return get_config()


#####################
# Private Functions #
#####################
def _config_to_dict(config):
    """ Convert a config object into a dictionary """
    global defaults
    for section in config.sections():
        # Jobs become a sub-dictionary
        if section.startswith('jobs_'):
            if 'jobs' not in defaults.keys():
                defaults['jobs'] = {}
            name = '_'.join(section.split('_')[1:])
            defaults['jobs'][name] = {}
            for k, v in config.items(section):
                defaults['jobs'][name][k] = v
        else:
            defaults[section] = {}
            for k, v in config.items(section):
                defaults[section][k] = v
    return defaults
