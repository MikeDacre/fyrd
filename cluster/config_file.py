"""
Description:   Get and set global variables

Created:       2015-12-11
Last modified: 2016-06-10 18:33
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
from . import options

################################################################################
#                            Configurable Defaults                             #
################################################################################

CONFIG_FILE = _environ['HOME'] + '/.python-cluster'
config      = _configparser.ConfigParser()
defaults    = {}

DEFAULT_PROFILE = {'nodes': 1,
                   'cores': 1,
                   'mem':   4000,
                   'time':  '02:00:00'}

__all__ = ['set', 'get', 'delete', 'get_config']
INITIAL_DEFAULTS = {}

# Pre-defined profiles, must begin with prof_. 'default' is required.
INITIAL_DEFAULTS['prof_default'] = DEFAULT_PROFILE
INITIAL_DEFAULTS['prof_large']   = {'nodes':        1,
                                    'cores':        16,
                                    'mem':          32000,
                                    'time':         '24:00:00'}

# Other options
INITIAL_DEFAULTS['opts']         = {}  # Set options that must always be set (e.g. partition)
INITIAL_DEFAULTS['queue']        = {'max_jobs':     1000,  # Max number of jobs in queue
                                    'sleep_len':    5,     # Between submission attempts (in seconds)
                                    'queue_update': 20}    # Amount of time between getting fresh queue info (seconds)


################################################################################
#                         Do Not Edit Below This Point                         #
################################################################################


############################
#  Profile Class Handling  #
############################


class Profile(object):

    """A job submission profile. Just a thin wrapper around a dict."""

    name = None
    args = None

    def __init__(self, name, kwds):
        """Set up bare minimum attributes."""
        self.__dict__['name'] = name
        # Check keywork arguments
        self.__dict__['args'] = options.check_arguments(kwds)

    def write(self):
        """Write self to config file."""
        set_profile(self.name, self.args)

    def __getattr__(self, key):
        """Access dict items as attributes."""
        return self.args[key] if key in self.args else None

    def __setattr__(self, key, value):
        """Access dict items as attributes."""
        if key == 'name':
            self.__dict__['name'] = key
        elif key == 'args':
            self.__dict__['args'] = options.check_arguments(key)
        else:
            opt, arg = list(options.check_arguments({key: value}).items())[0]
            self.args[opt] = arg

    def __len__(self):
        """Return arg count."""
        return len(self.args)

    def __repr__(self):
        """Display useful info."""
        return "{}<{}>".format(self.name, self.args)


def get_profile(profile=None):
    """Return a profile if it exists, if None, return all profiles."""
    if profile:
        prof = Profile(profile, get('prof', profile))
        if not prof and profile == 'default':
            logme.log('default profile missing, recreating. You can '
                      'override the defaults by editing {}'
                      .format(CONFIG_FILE), 'warn')
            prof = Profile('default', DEFAULT_PROFILE)
            prof.write()
        return prof
    else:
        profiles = get('prof')
        pfls = {}
        for profile, args in profiles.items():
            pfls[profile] = Profile(profile, args)
        return pfls


def set_profile(name, args):
    """Write profile to config file."""
    if not isinstance(args, dict):
        raise Exception('Profile arguments must be a dictionary')
    args = options.check_arguments(args)
    set('prof', name, args)


###############################################################################
#                              Useful Functions                               #
###############################################################################


def get(section=None, key=None, default=None):
    """Get a single key or section.

    :section: The config section to use (e.g. queue, prof)
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


def set(section, key, value):
    """Write a config key to the config file."""
    # Sanitize arguments
    section = str(section)
    key     = str(key)
    value   = value

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
    return INITIAL_DEFAULTS


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
    # Reset config class
    config = _configparser.ConfigParser()
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
        if section.startswith('prof_'):
            if 'prof' not in defaults.keys():
                defaults['prof'] = {}
            name = '_'.join(section.split('_')[1:])
            defaults['prof'][name] = {}
            for k, v in config.items(section):
                if v.isdigit():
                    v = int(v)
                defaults['prof'][name][k] = v
        else:
            defaults[section] = {}
            for k, v in config.items(section):
                if v.isdigit():
                    v = int(v)
                defaults[section][k] = v
    return defaults
