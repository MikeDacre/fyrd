"""
Get and set config file options.

===============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2015-12-11
 Last modified: 2016-08-31 17:10

   DESCRIPTION: The functions defined here provide an easy way to access the
                config file defined by CONFIG_FILE (default ~/.python-cluster).

                Importantly, this config file uses the concept of profiles,
                which is not built into python's configparser library. To get
                around this, we write profiles as sections that begin with
                'prof_' and the parse the whole config into a simple dictionary
                to make it easier to use.

                To work with profiles, use the get_profile() and set_profile()
                functions. Note that all options must be allowed in the
                options.py file before they can be added to a profile.

                Options will also be pre-sanitized before being added to
                profile. e.g. 'mem': '2GB' will become 'mem': 2000.

                The other options of interest are in the 'queue' section,
                specifically the max_jobs, sleep_len, and queue_update
                variables. max_jobs sets the maximum number of queing and
                running jobs allowed in the queue before submission will pause
                and wait; sleep_len sets the default amount of time various
                functions in this module wait when trying to run system calls;
                and queue_update sets the amount of time to wait between
                updating the queue from the system, to avoid overloading the
                system with too many updates.

===============================================================================
"""

import os
import sys
try:
    import configparser as _configparser
except ImportError:
    import ConfigParser as _configparser

from . import logme
from . import options

################################################################################
#                            Configurable Defaults                             #
################################################################################

CONFIG_FILE = os.environ['HOME'] + '/.python-cluster'
config      = _configparser.ConfigParser()
defaults    = {}

DEFAULT_PROFILE = {'nodes': 1,
                   'cores': 1,
                   'mem':   4000,
                   'time':  '02:00:00'}

__all__ = ['set', 'get', 'set_profile', 'get_profile', 'delete', 'get_config']
INITIAL_DEFAULTS = {}

# Pre-defined profiles, must begin with prof_. 'default' is required.
INITIAL_DEFAULTS['prof_default'] = DEFAULT_PROFILE
INITIAL_DEFAULTS['prof_large']   = {'nodes':        1,
                                    'cores':        16,
                                    'mem':          32000,
                                    'time':         '24:00:00'}

# Other options
INITIAL_DEFAULTS['opts']         = {}  # Set options that must always be set
INITIAL_DEFAULTS['queue']        = {'max_jobs':     1000,  # Max jobs in queue
                                    # Between submission attempts (in seconds)
                                    'sleep_len':    1,
                                    # Amount of time between getting fresh
                                    # queue info (seconds), 2 is a sensible
                                    # minimum
                                    'queue_update': 3}


################################################################################
#                         Do Not Edit Below This Point                         #
################################################################################


############################
#  Profile Class Handling  #
############################


class Profile(object):

    """A job submission profile. Just a thin wrapper around a dict."""

    name = None
    args = {}

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

    def __str__(self):
        """Pretty print."""
        return "{}:\n\t{}".format(
            self.name.title(),
            '\n\t'.join(['{}:\t{}'.format(i,j) for i,j in self.args.items()])
        )


def get_profile(profile=None):
    """Return a profile if it exists, if None, return all profiles.

    Will return None if profile is supplied but does not exist.

    :profile: The name of a profile to search for.

    """
    if profile:
        popt = get_option('prof', profile)
        if not popt:
            return None
        prof = Profile(profile, popt)
        if not prof and profile == 'default':
            logme.log('default profile missing, recreating. You can '
                      'override the defaults by editing {}'
                      .format(CONFIG_FILE), 'warn')
            prof = Profile('default', DEFAULT_PROFILE)
            prof.write()
        return prof
    else:
        profiles = get_option('prof')
        pfls = {}
        for profile, args in profiles.items():
            pfls[profile] = Profile(profile, args)
        return pfls


def set_profile(name, args):
    """Write profile to config file.

    :name: The name of the profile to add/edit.
    :args: Keyword arguments to add to the profile.

    """
    if not isinstance(args, dict):
        raise Exception('Profile arguments must be a dictionary')
    args = options.check_arguments(args)
    for arg, opt in args.items():
        set_option('prof_' + name, arg, opt)


def del_profile(name):
    """Delete a profile.

    :name: The name of the profile to delete.

    """
    delete('prof_{}'.format(name))


###############################################################################
#                              Useful Functions                               #
###############################################################################


def get_option(section=None, key=None, default=None):
    """Get a single key or section.

    :section: The config section to use (e.g. queue, prof)
    :key:     The config key to get (e.g. 'max_jobs')
    :default: If the key does not exist, create it with this default value.
    :returns: None if key does not exist.
    """
    defaults = get_config()
    if not section:
        return defaults
    if not section in defaults:
        if key and default:
            set_option(section, key, default)
            return get_option(section, key)
        else:
            return None
    if key:
        if key in defaults[section]:
            return defaults[section][key]
        else:
            if default:
                logme.log('Creating new config entry {}:{} with val {}'.format(
                    section, key, default), 'debug')
                set_option(section, key, default)
                return get_option(section, key)
            else:
                return None
    else:
        return defaults[section]


def set_option(section, key, value):
    """Write a config key to the config file."""
    # Sanitize arguments
    section = str(section)
    key     = str(key)
    value   = str(value)

    # Edit the globals in this file
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
    global defaults
    defaults = {}
    if os.path.isfile(CONFIG_FILE):
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
    logme.log(('Created the config file {} with default variables. '
               'This file contains the default profiles and can be used to '
               'control your job submission options, potentially saving you '
               'a lot of time. Please review this file and edit the defaults '
               'to fit your cluster configuration. In particular, edit the '
               '[opts] section to include job submission options that '
               'must be included every time on this cluster.')
              .format(CONFIG_FILE), 'info')
    return get_config()


###############################################################################
#                              Private Functions                              #
###############################################################################


def _config_to_dict(conf):
    """Convert a config object into a dictionary."""
    for section in conf.sections():
        # Jobs become a sub-dictionary
        if section.startswith('prof_'):
            if 'prof' not in defaults.keys():
                defaults['prof'] = {}
            name = '_'.join(section.split('_')[1:])
            defaults['prof'][name] = {}
            for k, v in conf.items(section):
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
