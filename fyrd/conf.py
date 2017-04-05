# -*- coding: utf-8 -*-
"""
Get and set config file options.

The functions defined here provide an easy way to access the config file
defined by CONFIG_FILE (default ~/.fyrd/config.txt) and the config.get('jobs',
'profile_file') (default ~/.fyrd/profile.txt).

Both files are managed by Python's ConfigParser class.

To work with profiles, use the `get_profile()` and `set_profile()` functions.
Note that all options must be allowed in the `options.py` file before they can
be added to a profile.

Options will also be pre-sanitized before being added to profile. e.g. 'mem':
    '2GB' will become 'mem': 2000.
"""
from __future__ import print_function
import os       as _os
import readline as _rl
from textwrap import dedent as _dnt
try:
    import configparser as _configparser
except ImportError:
    import ConfigParser as _configparser

from . import run     as _run
from . import logme   as _logme
from . import options as _opt


###############################################################################
#                            Configurable Defaults                            #
###############################################################################

# Config File
CONFIG_PATH  = _os.path.join(_os.environ['HOME'], '.fyrd')
"""
Where configuration files will be kept
"""
CONFIG_FILE  = _os.path.join(CONFIG_PATH, 'config.txt')
"""
Where the main config will be kept.
"""

# Set default options
DEFAULTS = {
    'queue': {
        'max_jobs':     1000,
        'sleep_len':    1,
        'queue_update': 2,
        'res_time':     2700,
        'queue_type':   'auto',
        'sbatch':       None, # Path to sbatch command
        'qsub':         None, # Path to qsub command
        # Not implemented yet
        #  'db':           _os.path.join(CONFIG_PATH, 'db.sql'),
    },
    'jobs': {
        'clean_files':     True,
        'clean_outputs':   False,
        'file_block_time': 200,
        'scriptpath':      None,
        'outpath':         None,
        'suffix':          'cluster',
        'auto_submit':     True,
        'generic_python':  False,
        'profile_file':    _os.path.join(
            CONFIG_PATH, 'profiles.txt'
        )
    },
    'jobqueue': {
        'jobno': 1
    },
}

CONF_HELP = {
    'summary': _dnt(
        """
        The following options and sections are recognized and defined by the
        DEFAULTS dictionary in the config.py file. The can be updated in the
        config file.

        Any options added to the config file not present here will be ignored
        or deleted, any options added which do not match the types below will
        be overwritten with the defaults.
        """
    ),
    'queue': _dnt(
        """
        [queue]
        Define options for queue handling

        Options:
            max_jobs (int):     sets the maximum number of running jobs before
                                submission will pause and wait for the queue to
                                empty
            sleep_len (int):    sets the amount of time the program will wait
                                between submission attempts
            queue_update (int): sets the amount of time between refreshes of
                                the queue.
            res_time (int):     Time in seconds to wait if a job is in an
                                uncertain state, usually preempted or
                                suspended. These jobs often resolve into
                                running or completed again after some time so
                                it makes sense to wait a bit, but not forever.
                                The default is 45 minutes: 2700 seconds.
            queue_type (str):   the type of queue to use, one of 'torque',
                                'slurm', 'local', 'auto'. Default is auto to
                                auto-detect the queue.
            sbatch (str):       A path to the sbatch executable, only required
                                for slurm mode if sbatch is not in the PATH.
            qsub (str):         A path to the qsub executable, only required
                                for torque mode if sbatch is not in the PATH.
            db_path (str):      Where to put the job database (Not implemented)
        """
    ),
    'jobs': _dnt(
        """
        [jobs]
        Set the options for managing job submission and getting

        Options:
            clean_files (bool):    means that by default files will be deleted
                                   when job completes
            clean_outputs (bool):  is the same but for output files (they are
                                   saved first)
            file_block_time (int): Max amount of time to block after job
                                   completes in the queue while waiting for
                                   output files to appear.  Some queues can
                                   take a long time to copy files under load,
                                   so it is worth setting this high, it won't
                                   block unless the files do not appear.
            scriptpath (str):      Path to write all script files by
                                   default, must be globally cluster
                                   accessible. Note: this is *not* the runtime
                                   path, just where files are written to.
            outpath (str):         Path to write all output files to by
                                   by default, must be globally cluster
                                   accessible.
            suffix (str):          The suffix to use when writing scripts and
                                   output files
            auto_submit (bool):    If wait() or get() are called prior to
                                   submission, auto-submit the job. Otherwise
                                   throws an error and returns None
            generic_python (bool): Use /usr/bin/env python instead of the
                                   current executable, not advised, but
                                   sometimes necessary.
            profile_file (str):    the config file where profiles are defined.
        """
    ),
    'jobqueue': _dnt(
        """
        [jobqueue]
        Sets options for the local queue system, will be removed in the future
        in favor of database.
        """
    ),
}

# Pre-defined profiles, 'DEFAULT' is required.
DEFAULT_PROFILES = {
    'DEFAULT':     {'nodes': 1,
                    'cores': 1,
                    'mem':   4000,
                    'time':  '04:00:00'},
    'large':       {'nodes': 1,
                    'cores': 16,
                    'mem':   32000,
                    'time':  '24:00:00'},
    'small':       {'nodes': 1,
                    'cores': 1,
                    'mem':   1000,
                    'time':  '01:00:00'},
    'long':        {'nodes': 1,
                    'cores': 1,
                    'mem':   4000,
                    'time':  '96:00:00'},
    'small_clean': {'nodes': 1,
                    'cores': 1,
                    'mem':   1000,
                    'time':  '01:00:00',
                    'clean_files':   True,
                    'clean_outputs': True},
}
"""
This defines the default profiles that will be stored in the profile file.
It is intended mostly as an example and should be edited in the profile file
to customize the profiles for each cluster.

The only required profile is 'DEFAULT', it must be set and is the fallback
profile if no profile is requested on job creation.
"""


###############################################################################
#                         Do Not Edit Below This Point                        #
###############################################################################

# Create the global config objects
config   = _configparser.ConfigParser(allow_no_value=True)
"""
This is the globally accessible ConfigParser object for the config.txt file.
"""
profiles = _configparser.ConfigParser(defaults=DEFAULT_PROFILES['DEFAULT'],
                                      allow_no_value=True)
"""
This is the globally accessible ConfigParser object for handling profiles.
"""

__all__ = ['set_option', 'get_option', 'delete', 'create_config',
           'create_config_interactive', 'set_profile',
           'get_profile', 'del_profile', 'create_profiles']


###############################################################################
#                        Config Manipulation Functions                        #
###############################################################################


def get_option(section=None, key=None, default=None):
    """Get a single key or section.

    All args are optional, if they are missing, the parent section or entire
    config will be returned.

    Args:
        section (str): The config section to use (e.g. queue), if None, all
                       sections returned.
        key (str) :    The config key to get (e.g. 'max_jobs'), if None, whole
                       section returned.
        default:       If the key does not exist, create it with this default
                       value.

    Returns:
        Option value if key exists, None if no key exists.
    """
    load_config()

    if not section:
        out = _config_to_dict(config)

    elif section and section not in _sections(config):
        _logme.log('{} not in the config file'.format(section), 'error')
        if section in DEFAULTS:
            _logme.log('CreAting {} from DEFAULTS'.format(section), 'info')
            config.add_section(section)
            _config_from_dict(config, DEFAULTS[section], section=section)
            write_config()
            out = get_option(section, key, default)
        else:
            raise ValueError('Section not in the config file or DEFAULTS')

    elif key:
        sect = _section_to_dict(config.items(section))
        if key in sect:
            out = sect[key]
        else:
            _logme.log('{} not in the {} section of the config file'
                       .format(key, section), 'warn')
            if default:
                _logme.log('Creating new config entry {}:{} with val {}'
                           .format(section, key, default))
                set_option(section, key, default)
                out = get_option(section, key)
            elif key in DEFAULTS[section]:
                _logme.log('Creating new config entry {}:{} with val {}'
                           .format(section, key, DEFAULTS[section][key]))
                set_option(section, key, DEFAULTS[section][key])
                out = get_option(section, key)
            else:
                raise ValueError('Option not in the config file')

    elif section:
        _logme.log('Getting the whole section: {}'.format(section), 'debug')
        out = _section_to_dict(config.items(section))

    else:
        _logme.log('No option specified, returning config dictionary', 'debug')
        out = _config_to_dict(config)

    return out


def set_option(section, key, value):
    """Write a config key to the config file.

    Args:
        section (str): Section of the config file to use.
        key (str):     Key to add.
        value:         Value to add for key.

    Returns:
        ConfigParser
    """
    # Sanitize arguments
    section = str(section)
    key     = str(key)
    value   = str(value)

    # Edit the globals in this file
    load_config()

    if not config.has_section(section):
        _logme.log('The {} section is not in the config file and cannot be'
                   .format(section) + 'and cannot be created', 'warn')
        return None

    config.set(section, key, value)

    write_config()

    return config


def delete(section, key):
    """Delete a config item.

    Args:
        section (str): Section of config file.
        key (str):     Key to delete

    Returns:
        ConfigParger
    """
    load_config()

    config.remove_option(section, key)

    write_config()

    return config


def load_config():
    """Load config from the config file.

    If any section or key from DEFAULTS is not present in the config, it is
    added back, enforcing a minimal configuration.

    Returns:
        ConfigParser: Config options.
    """
    if _os.path.isfile(CONFIG_FILE):
        config.read(CONFIG_FILE)
    else:
        create_config()

    for section in DEFAULTS:
        if section not in _sections(config):
            config.add_section(section)
            _config_from_dict(config, DEFAULTS[section], section)
            write_config()
        for key, val in DEFAULTS[section].items():
            if key not in dict(config.items(section)):
                config.set(section, key, str(val))
                write_config()
    return config

def get_config():
    """Return a dictionary representation of the entire config."""
    return _config_to_dict(load_config())


def write_config():
    """Write the current config to CONFIG_FILE."""
    with open(CONFIG_FILE, 'w') as fout:
        config.write(fout)


###############################################################################
#                          Initialization Functions                           #
###############################################################################


def create_config_interactive(prompt=True):
    """Interact with the user to create a new config.

    Uses readline autocompletion to make setup easier.

    Args:
        prompt (bool): As for confirmation before beginning wizard.
    """
    # Use tab completion
    t = _TabCompleter()
    _rl.set_completer_delims('\t')
    _rl.parse_and_bind("tab: complete")

    # Get permission
    if prompt:
        t.createListCompleter(['y', 'n'])
        _rl.set_completer(t.list_completer)
        print("Do you want to initialize your config at {}"
              .format(CONFIG_FILE))
        print("This will erase your current configuration (if it exists)")
        choice = _run.get_input("Initialize config? [y/N] ").strip().lower()
        if not choice == 'y':
            return

    cnf = DEFAULTS
    # Get path
    _rl.set_completer(_path_completer)
    # Not implemented yet
    #  print("\nThis module uses a database to store job information.",
          #  "This database should remain relatively small, but can get quite",
          #  "large if many jobs are submitted at once.\n"
          #  "It only needs to be accessible from the submit host, but should",
          #  "be somewhere with sufficient disk space (>500MB free).")
    #  print("Where would you like to put the db file?\n")
    #  file_path = _os.path.expanduser(
        #  _run.get_input('PATH: [{}] '.format(config.get('queue', 'db')))
    #  ).strip(' ').lower()

    #  file_path = file_path if file_path else cnf['queue']['db']
    #  cnf['queue']['db'] = _os.path.expanduser(file_path)

    print("We store job profile information in a small config file.")
    file_path = _os.path.expanduser(
        _run.get_input('Where would you like that file to go? [{}]'
                       .format(config.get('jobs', 'profile_file')))
    ).strip(' ').lower()

    file_path = file_path if file_path else cnf['jobs']['profile_file']
    cnf['jobs']['profile_file'] = _os.path.expanduser(file_path)


    # Temp file directory
    print("\nIt is possible to write all temporary and output files to a single",
          "temp file directory, regardless of where the job is run from.\n" +
          "As this library allows you to retrieve output files from the class",
          "directly, this is a good way of keeping your work directory tidy.",
          "If you do not choose to have files auto-deleted though, you will",
          "need to keep the directory tidy yourself.\n"
          "The value of this option is that if you run from a machine where",
          "some places are not cluster-accessible, jobs will run anyway if",
          "just one directory is accessible to the cluster.\n"
          "This option can always be overridden at runtime with the filepath",
          "argument.\nIf you leave the below option blank, the default will",
          "be to use the runtime path, which is usually the current working",
          "directory.\n")
    t.createListCompleter(['y', 'n'])
    _rl.set_completer(t.list_completer)
    if _run.get_yesno('Would you like to set a script file path?', 'y'):
        file_path = _get_set_path('Where would you like to put that file?')
        cnf['jobs']['scriptpath'] = file_path
    if _run.get_yesno('Would you like to set an output file path?', 'n'):
        file_path = _get_set_path('Where would you like to put that file?')
        cnf['jobs']['outpath'] = file_path

    # Cleaning
    t.createListCompleter(['y', 'n'])
    _rl.set_completer(t.list_completer)
    print('\nWe can automatically delete script and/or output files after',
          'results have been retrieved.\n'
          'This option can be overridden at run time on a per-job basis.\n'
          'Do you want to autoclean:\n')
    clean_files = _run.get_yesno('Autoclean script files?', 'y')
    cnf['jobs']['clean_files'] = clean_files

    clean_outs = _run.get_yesno('Autoclean output files (e.g. .out and .err)?',
                                'n')
    cnf['jobs']['clean_outputs'] = clean_outs

    # Wait times
    t.createListCompleter(cnf['queue'].values())
    _rl.set_completer(t.list_completer)
    max_len = _run.get_input("\nWhat is the maximum number of jobs allowed " +
                             "in your queue? [{}] "
                             .format(cnf['queue']['max_jobs']))
    max_len = max_len if max_len else cnf['queue']['max_jobs']
    cnf['queue']['max_jobs'] = int(max_len)

    # Options
    print('\nIs there a default queue you wish to submit to if no other',
          'options are given?\nIf so enter the name below, or leave blank',
          'to ignore.\n')
    def_queue = _run.get_input('Default queue: ').strip()

    print('\nThank you, configuring options.\n'
          'Please edit the file directly to inspect or edit your config.\n')

    create_config(cnf, def_queue)


def create_config(cnf=None, def_queue=None):
    """Create an initial config file.

    Gets all information from the file-wide DEFAULTS constant and overwrites
    specific keys using the values in cnf.

    This means that any records in the cnf dict that are not present in
    DEFAULTS will be ignored, and any records that are absent will be
    populated from DEFAULTS.

    Args:
        cnf (dict):      A dictionary of config defaults.
        def_queue (str): A name for a queue to add to the default profile.
    """
    global config
    config = _configparser.ConfigParser(allow_no_value=True)

    if _os.path.exists(CONFIG_FILE):
        _os.remove(CONFIG_FILE)

    init_conf = {}

    if not cnf or not isinstance(cnf, dict):
        cnf = {}

    for section, items in DEFAULTS.items():
        init_conf[section] = {}
        if not section in cnf:
            cnf[section] = {}
        for key, value in items.items():
            val = cnf[section][key] if key in cnf[section] else value
            init_conf[section][key] = str(val)

    _config_from_dict(config, init_conf)

    with open(CONFIG_FILE, 'w') as fout:
        config.write(fout)

    if def_queue:
        def_prof = get_profile('DEFAULT')
        def_prof.args.update({'partition': def_queue})
        def_prof.write()


###############################################################################
#                           Config Helper Fuctions                            #
###############################################################################


def get_job_paths(kwds):
    """Parse keyword arguments to get important paths.

    Args:
        kwds (dict): Keyword arguments accepted by fyrd.job.Job

    Returns:
        tuple: kwds, runpath, outpath, scriptpath.
    """
    runpath = _os.path.abspath(kwds['dir'] if 'dir' in kwds else '.')
    kwds['dir'] = runpath

    # Set the output path
    cpath = get_option('jobs', 'outpath')
    if 'outpath' in kwds:
        outpath = kwds.pop('outpath')
    elif cpath:
        outpath = cpath
    else:
        outpath = runpath
    outpath = _os.path.abspath(outpath)

    # Set the script path
    cpath = get_option('jobs', 'scriptpath')
    if 'scriptpath' in kwds:
        scriptpath = kwds.pop('scriptpath')
    elif cpath:
        scriptpath = cpath
    else:
        scriptpath = outpath
    scriptpath = _os.path.abspath(scriptpath)

    return kwds, runpath, outpath, scriptpath


###############################################################################
#                                  Profiles                                   #
###############################################################################


class Profile(object):

    """A job submission profile. Just a thin wrapper around a dict."""

    def __init__(self, name, kwds):
        """Set up bare minimum attributes.

        Args:
            name (str):  Name of the profile
            kwds (dict): Dictionary of keyword arguments (will be validated).
        """
        name = 'DEFAULT' if name.lower() == 'default' else name
        self.name = name
        self.args = kwds

    def write(self):
        """Write self to config file."""
        set_profile(self.name, self.args)

    def __getattr__(self, key):
        """Access dict items as attributes."""
        if key in self.args:
            return self.args[key]

    def __setattr__(self, key, value):
        """Force checking of keywords."""
        if key == 'name':
            object.__setattr__(self, key, value)
        elif key == 'args':
            if not isinstance(value, dict):
                raise Exception('Keyword arguments must be a dict')
            value = _opt.check_arguments(value)
            object.__setattr__(self, key, value)
        else:
            opt, arg = list(_opt.check_arguments({key: value}).items())[0]
            self.args[opt] = arg

    def __len__(self):
        """Return arg count."""
        return len(self.args)

    def __repr__(self):
        """Display useful info."""
        return "{}<{}>".format(self.name, self.args)

    def __str__(self):
        """Pretty print."""
        title = 'DEFAULT' if self.name.lower() == 'default' else self.name
        return "{}:\n\t{}".format(
            title,
            '\n\t'.join(['{:<11}{:}'.format(i, j) \
                         for i, j in self.args.items()])
        )


def get_profile(profile=None, allow_none=True):
    """Return a profile if it exists, if None, return all profiles.

    Will return None if profile is supplied but does not exist.

    Args:
        profile (str):     The name of a profile to search for.
        allow_none (bool): If True, return None if no profile matches,
                           otherwise raise a ValueError.

    Returns:
        fyrd.conf.Profile: The requested profile.
    """
    load_profiles()
    # Allow lowercase default profile
    if profile and profile.lower() == 'default':
        profile = 'DEFAULT'
    if profile in _sections(profiles):
        return Profile(profile, _section_to_dict(profiles.items(profile)))
    else:
        if profile.lower() == 'default':
            _logme.log('default profile missing, recreating. You can '
                       'override the defaults by editing {}'
                       .format(CONFIG_FILE), 'warn')
            prof = Profile('default', DEFAULT_PROFILES['default'])
            prof.write()
            return prof
        if allow_none:
            return None
        else:
            raise ValueError('Requested profile ({}) does not exist'
                             .format(profile))


def get_profiles(profiles=None, allow_none=True):
    """Return a dictionary of profiles from profiles.

    Returns all profiles if profiles argument is None.

    Args:
        profiles (list):   A list of profiles to get.
        allow_none (bool): If True, return None if no profile matches,
                           otherwise raise a ValueError.

    Returns:
        dict: A ditionary of profile: fyrd.conf.Profile
    """
    if profiles:
        profiles = _run.listify(profiles)
        pfls = {}
        for profile in profiles:
            pfls[profile] = get_profile(profile, allow_none)
    else:
        if not allow_none:
            raise ValueError('Profile required')
        pfls = {
            'DEFAULT': Profile(
                'DEFAULT', _section_to_dict(profiles.items('DEFAULT'))
            )
        }
        for section in _sections(profiles):
            pfls[section] = Profile(
                section, _section_to_dict(profiles.items(section))
            )
        return pfls


def set_profile(name, kwds, update=True):
    """Write profile to config file.

    Arguments:
        name (str):    The name of the profile to add/edit.
        kwds (dict):   Keyword arguments to add to the profile.
        update (bool): Update the profile rather than overwriting it.
    """
    load_profiles()
    name = 'DEFAULT' if name.lower() == 'default' else name

    if not isinstance(kwds, dict):
        raise Exception('Profile arguments must be a dictionary')

    kwds = _opt.check_arguments(kwds)

    if name in _sections(profiles):
        if not update:
            _logme.log('Profile {} already exists, overwriting'.format(name),
                       'debug')
            profiles.remove_section(name)
            profiles.add_section(name)
    else:
        profiles.add_section(name)

    _config_from_dict(profiles, kwds, name)

    write_profiles()
    return get_profile(name)


def del_profile(name):
    """Delete a profile.

    Args:
        name (str): The name of the profile to delete.
    """
    load_profiles()
    if name.lower() == 'default':
        for key in _section_to_dict(profiles.items('DEFAULT')):
            if key not in DEFAULT_PROFILES['DEFAULT']:
                profiles.remove_option('DEFAULT', key)
            else:
                profiles.set('DEFAULT', key,
                             str(DEFAULT_PROFILES['DEFAULT'][key]))
    elif name in _sections(profiles):
        _logme.log('Removing profile {}'.format(name))
        profiles.remove_section(name)
    else:
        _logme.log('Profile {} does not exist, cannot delete'.format(name),
                   'warn')
    write_profiles()


def create_profiles(profs=None):
    """Create an initial profiles file.

    Gets all information from the file-wide DEFAULT_PROFILES constant and
    overwrites specific keys using the values in cnf.

    This means that any records in the cnf dict that are not present in
    DEFAULT_PROFILES will be ignored, and any records that are absent will be
    populated from DEFAULT_PROFILES.

    Args:
        cnf (dict): A dictionary of config defaults.
    """
    global profiles

    profiles = _configparser.ConfigParser(
        defaults=DEFAULT_PROFILES['DEFAULT'],
        allow_no_value=True,
    )

    if _os.path.exists(config.get('jobs', 'profile_file')):
        _os.remove(config.get('jobs', 'profile_file'))

    init_conf = {}
    if not profs or not isinstance(profs, dict):
        profs = {}
    for section, items in DEFAULT_PROFILES.items():
        init_conf[section] = {}
        if section not in profs:
            profs[section] = {}
        for key, value in items.items():
            val = profs[section][key] if key in profs[section] else value
            init_conf[section][key] = str(val)

    _config_from_dict(profiles, init_conf)

    with open(config.get('jobs', 'profile_file'), 'w') as fout:
        profiles.write(fout)


def load_profiles():
    """Load the profiles, create them if they don't exist.

    Returns:
        ConfigParser: profiles
    """
    if not _os.path.isfile(config.get('jobs', 'profile_file')):
        create_profiles()
    profiles.read(config.get('jobs', 'profile_file'))

    # Recreate DEFAULT if necessary.
    def_prof = _config_to_dict(profiles)['DEFAULT']
    changed = False
    for key, val in DEFAULT_PROFILES['DEFAULT'].items():
        if key not in def_prof:
            profiles.set('DEFAULT', key, val)
            changed = True
    if changed:
        write_profiles()

    return profiles


def write_profiles():
    """Write the profiles to the file.

    Returns:
        ConfigParser: profiles
    """
    with open(config.get('jobs', 'profile_file'), 'w') as fout:
        profiles.write(fout)
    return load_profiles()


###############################################################################
#                              Helper Functions                               #
###############################################################################


def _sections(cnf, inc_anyway=False):
    """Include default in sections if it has items.

    Args:
        cnf (ConfigParser): Any ConfigParser object
        inc_anyway (bool):  Include the DEFAULT section even if empty

    Returns:
        list: A list of sections in cnf, including DEFAULT if defined.
    """
    merged_sections = cnf.sections() + ['DEFAULT']
    return merged_sections if cnf.items('DEFAULT') or inc_anyway \
            else cnf.sections()


def _config_from_dict(cnf, dct, section=None):
    """Python 2 ConfigParsers cannot handle dictionaries, so we do it here.

    Args:
        cnf (ConfigParser): A ConfigParser object.
        dct (dict):          A dictionary of {'key': 'value'} if section or
                             {'section': {'key': 'value'}}
        section (str):       An optional section name to update

    Returns:
        ConfigParser: The original ConfigParser object, updated.
    """
    assert isinstance(cnf, _configparser.ConfigParser)
    assert isinstance(dct, dict)

    if section:
        for v in dct.values():
            assert not isinstance(v, (tuple, list, dict, set))
    else:
        for v in dct.values():
            assert isinstance(v, dict)

    if section:
        if not section in cnf.sections() + ['DEFAULT']:
            cnf.add_section(section)
        for k, v in _dict_to_strings(dct).items():
            cnf.set(section, k, v)
    else:
        for sect, vals in dct.items():
            if not sect in cnf.sections() + ['DEFAULT']:
                cnf.add_section(sect)
            for k, v in _dict_to_strings(vals).items():
                cnf.set(sect, k, v)
    return cnf


def _dict_to_strings(kwds):
    """Convert all values in a dictionary to strings."""
    ot = {}
    assert isinstance(kwds, dict)
    for k, v in kwds.items():
        ot[k] = str(v)
    return ot


def _section_to_dict(section):
    """Convert a ConfigParser list of tuples to a dictionary.

    Args:
        section (list): Output of ConfigParser.items(section)

    Returns:
        dict: A dictionary of the above with typed values
    """
    dct = dict(section)
    out = {}
    for key, val in dct.items():
        if isinstance(val, str):
            if val == 'True':
                out[key] = True
            elif val == 'False':
                out[key] = False
            elif val == 'None':
                out[key] = None
            elif val.isdigit():
                out[key] = int(val)
            else:
                out[key] = val
        else:
            out[key] = val
    return out


def _config_to_dict(cnf):
    """Return a dictionary of all items from a ConfigParser object."""
    out = {}
    def_items = cnf.items('DEFAULT')
    if def_items:
        out['DEFAULT'] = _section_to_dict(def_items)
    for sect in _sections(cnf):
        out[sect] = _section_to_dict(cnf.items(sect))
    return out


###############################################################################
#                                 Completion                                  #
###############################################################################


class _TabCompleter(object):
    """
    A tab completer that can either complete from the filesystem or from a
    list.

    Taken from:

        `https://gist.github.com/iamatypeofwalrus/5637895`_

    """
    list_completer = None


    def createListCompleter(self,ll):
        """
        This is a closure that creates a method that autocompletes from
        the given list.

        Since the autocomplete function can't be given a list to complete from
        a closure is used to create the list_completer function with a list to
        complete from.
        """

        def list_completer(_, state):
            """Make a completer from a list."""
            line   = _rl.get_line_buffer()

            if not line:
                return [c + " " for c in ll][state]

            else:
                return [c + " " for c in ll if c.startswith(line)][state]

        self.list_completer = list_completer


def _path_completer(_, state):
    """
    This is the tab completer for systems paths.
    Only tested on *nix systems
    """
    line = _rl.get_line_buffer()
    if not line:
        return _complete_path('.')[state]
    else:
        return _complete_path(line)[state]


def _listdir(root):
    "List directory 'root' appending the path separator to subdirs."
    res = []
    for name in _os.listdir(root):
        path = _os.path.join(root, name)
        if _os.path.isdir(path):
            name += _os.sep
        res.append(name)
    return res


def _complete_path(path=None):
    "Perform completion of filesystem path."
    if not path:
        return _os.listdir('.')
    path = _os.path.expanduser(path)
    dirname, rest = _os.path.split(path)
    tmp = dirname if dirname else '.'
    res = [_os.path.join(dirname, p)
           for p in _os.listdir(tmp) if p.startswith(rest)]
    # more than one match, or single match which does not exist (typo)
    if len(res) > 1 or not _os.path.exists(path):
        return res
    # resolved to a single directory, so return list of files below it
    if _os.path.isdir(path):
        return [_os.path.join(path, p) for p in _os.listdir(path)]
    # exact file match terminates this completion
    return [path + ' ']


def _get_set_path(message):
    """Get a path from the user, make the directory if necessary."""
    t = _TabCompleter()
    while True:
        _rl.set_completer(_path_completer)
        file_path = _run.get_input(message + ' ')
        file_path = file_path.strip().lower()
        if file_path:
            file_path = _os.path.abspath(file_path)
            if _os.path.isdir(file_path):
                break
            else:
                t.createListCompleter(['y', 'n'])
                _rl.set_completer(t.list_completer)
                ans = _run.get_input(
                    'That directory does not exist, would you like to ' +
                    'try to create it? [y/N] ')
                if ans.lower() == 'y':
                    try:
                        _os.makedirs(file_path)
                        break
                    except OSError:
                        print("Failed to make {}".format(file_path),
                              "Please make it manually or choose another",
                              "directory\n")
                        continue
                else:
                    continue
    return file_path


###############################################################################
#           Set Constants from config, initialize config on import            #
###############################################################################

# Create directory if it doesn't exist
if not _os.path.isdir(CONFIG_PATH):
    if _os.path.exists(CONFIG_PATH):
        _os.remove(CONFIG_PATH)
    _os.makedirs(CONFIG_PATH)
if not _os.path.isfile(CONFIG_FILE):
    create_config()

# Load config
config = load_config()

# Load profiles
profiles = load_profiles()
