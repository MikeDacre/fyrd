"""
Description:   Get and set global variables

Created:       2015-12-11
Last modified: 2015-12-11 22:16
"""
from os import path
from os import environ
from re import findall
from sys import stderr
import configparser

config_file = environ['HOME'] + '/.slurmy'
config      = configparser.ConfigParser()
defaults    = {}


def _create_config():
    """ Create a ~/.slurmy file """
    defaults['queue'] = {'max_jobs':     1000,  # Max number of jobs in queue
                         'sleep_len':    5,     # Between submission attempts (in seconds)
                         'queue_update': 20}    # Amount of time between getting fresh queue info (seconds)
    for k, v in defaults.items():
        config[k] = v
    with open(config_file, 'w') as outfile:
        config.write(outfile)
    stderr.write('Created the file ~/.slurmy with default variables. ' +
                 'Please review this file and edit your defaults\n')
    return defaults


def _get_config():
    """ Load defaults from ~/.slurmy """
    global defaults
    if path.isfile(config_file):
        config.read(config_file)
        for section in config.sections():
            defaults[section] = {}
            for k, v in config[section].items():
                defaults[section][k] = v
    else:
        # Make the config file
        defaults = _create_config()
    return defaults
