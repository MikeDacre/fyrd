"""
Description:   Get and set global variables

Created:       2015-12-11
Last modified: 2016-04-15 11:11
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

from . import logme

################################################################################
#                            Configurable Defaults                             #
################################################################################

CONFIG_FILE = _environ['HOME'] + '/.python-cluster'
config      = _configparser.ConfigParser()
defaults    = {}

__all__ = ['write', 'get', 'delete', 'get_config']
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


################################################################################
#                         Do Not Edit Below This Point                         #
################################################################################


############################
#  Profile Class Handling  #
############################


class Profile(object):

    """A job submission profile."""

    nodes = None
    cores = None
    mem   = None
    time  = None
    def __init__(self, **kwds):
        """Set up bare minimum attributes."""



###############################################################################
#                              Useful Functions                               #
###############################################################################


def get(section=None, key=None, default=None):
    """Get a single key or section.

    :section: The config section to use (e.g. queue, jobs)
    :key:     The config key to get (e.g. 'max_jobs')
    :default: If the key does not exist, create it with this default value.
    Returns None if key does not exist.
    """
    defaults = get_config()
    if not section:
        return defaults
    if not section in defaults:
        if key and default:
            write(section, key, default)
            return get(section, key)
        else:
            return None
    if key:
        if key in defaults[section]:
            return defaults[section][key]
        else:
            if default:
                logme.log('Creating new config entry {}:{} with val {}'.format(
                    section, key, default), 'debug')
                write(section, key, default)
                return get(section, key)
            else:
                return None
    else:
        return defaults[section]


def write(section, key, value):
    """Write a config key to the config file."""
    # Sanitize arguments
    section = str(section)
    key     = str(key)
    value   = str(value)

    # Edit the globals in this file
    global defaults, config
    config.read(CONFIG_FILE)

    if not config.has_section(section):
        config.add_section(section)
    config.set(section, key, value)

    with open(CONFIG_FILE, 'w') as outfile:
        config.write(outfile)

    return get_config()


def delete(section, key=None):
    """Delete a config item.

    If key is not provided deletes whole section.
    """
    global defaults, config
    config.read(CONFIG_FILE)
    if key:
        config.remove_option(section, key)
    else:
        config.remove_section(section)
    with open(CONFIG_FILE, 'w') as outfile:
        config.write(outfile)
    return get_config()


###############################################################################
#                             Internal Functions                              #
###############################################################################


def get_initial_defaults():
    """Return a sane set of starting defaults for config file creation."""
    # Defined file wide
    return initial_defaults


def get_config():
    """Load defaults from file."""
    global defaults, config
    defaults = {}
    if _path.isfile(CONFIG_FILE):
        config.read(CONFIG_FILE)
        defaults = _config_to_dict(config)
    else:
        defaults = create_config()
    return defaults


def create_config():
    """Create a config file

    Will clobber any existing file
    """
    global defaults, config
    # Use defaults coded into this file
    defaults = get_initial_defaults()
    # Delete existing file
    if os.path.exists(CONFIG_FILE):
        os.remove(CONFIG_FILE)
    for k, v in defaults.items():
        if not config.has_section(k):
            config.add_section(k)
        for i, j in v.items():
            try:
                config.set(str(k), str(i), str(j))
            except TypeError:
                sys.stderr.write('{} {} {}\n'.format(k, i, j))
                raise
    with open(CONFIG_FILE, 'w') as outfile:
        config.write(outfile)
    logme.log('Created the file with default variables '.format(CONFIG_FILE) +
              'Please review this file and edit your defaults', 'info')
    return get_config()


###############################################################################
#                              Private Functions                              #
###############################################################################


def _config_to_dict(config):
    """Convert a config object into a dictionary."""
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
