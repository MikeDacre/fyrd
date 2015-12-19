"""
Description:   Get and set global variables

Created:       2015-12-11
Last modified: 2015-12-18 16:30
"""
from os import path    as _path
from os import environ as _environ
from os import system  as _system
from re import findall as _findall
from sys import stderr as _stderr
try:
    import configparser as _configparser
except ImportError:
    import ConfigParser as _configparser

config_file = _environ['HOME'] + '/.slurmy'
config      = _configparser.ConfigParser()
defaults    = {}

__all__ = ['get_config', 'write_config']


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


def get_initial_defaults():
    """ Return a sane set of starting defaults for config file creation """
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

    return initial_defaults


def create_config():
    """ Create a ~/.slurmy file
        Will clobber an existing ~/.slurmy file """
    global defaults, config
    # Use defaults coded into this file
    defaults = get_initial_defaults()
    # Delete existing slurmy file
    _system("rm {}".format(config_file))
    for k, v in defaults.items():
        if not config.has_section(k):
            config.add_section(k)
        for i, j in v.items():
            config.set(k, i, j)
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
