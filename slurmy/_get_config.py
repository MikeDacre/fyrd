"""
Description:   Get and set global variables

Created:       2015-12-11
Last modified: 2015-12-11 22:52
"""
from os import path
from os import environ
from re import findall
from sys import stderr
import configparser

config_file = environ['HOME'] + '/.slurmy'
config      = configparser.ConfigParser()
defaults    = {}

# Starting defaults
initial_defaults = {}
initial_defaults['jobs']['small'] = {'nodes':        1,
                                     'cores':        1,
                                     'mem':          '4GB',
                                     'time':         '00:02:00'}
initial_defaults['jobs']['large'] = {'nodes':        1,
                                     'cores':        16,
                                     'mem':          '64GB',
                                     'time':         '24:00:00'}
initial_defaults['queue']         = {'max_jobs':     1000,  # Max number of jobs in queue
                                     'sleep_len':    5,     # Between submission attempts (in seconds)
                                     'queue_update': 20}    # Amount of time between getting fresh queue info (seconds)


def _config_to_dict(config):
    """ Convert a config object into a dictionary """
    for section in config.sections():
        defaults[section] = {}
        for k, v in config[section].items():
            defaults[section][k] = v
    return defaults


def _create_config():
    """ Create a ~/.slurmy file """
    global defaults, config
    # Use defaults coded into this file
    defaults = initial_defaults
    for k, v in defaults.items():
        config[k] = v
    with open(config_file, 'w') as outfile:
        config.write(outfile)
    stderr.write('Created the file ~/.slurmy with default variables. ' +
                 'Please review this file and edit your defaults\n')
    return defaults


def _get_config():
    """ Load defaults from ~/.slurmy """
    global defaults, config
    if path.isfile(config_file):
        config.read(config_file)
        defaults = _config_to_dict(config)
    else:
        defaults = _create_config()
    return defaults


def write_config(section, key, value):
    """ Write a config key to the ~/.slurmy file """
    global defaults, config
    config.read(config_file)
    config[section][key] = value
    with open(config_file, 'w') as outfile:
        config.write(outfile)
    return _get_config()
