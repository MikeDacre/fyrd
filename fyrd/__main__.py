#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Manage fyrd config, profiles, and queue.

============   ======================================
Author         Michael D Dacre <mike.dacre@gmail.com>
Organization   Stanford University
License        MIT License, use as you wish
Version        0.6.2-beta.6
============   ======================================
"""
from __future__ import print_function
import sys
import argparse
from itertools import chain
from tabulate import tabulate

import fyrd

###############################################################################
#                                  Help Text                                  #
###############################################################################


CONF_HELP = """\
This script allows display and management of the fyrd config file found
here: {conf_file}.
""".format(conf_file=fyrd.conf.CONFIG_FILE)
CONF_EPI = """\
Show usage::
    fyrd conf show [-s <section>]

Update usage::
    fyrd conf update <section> <option> <value>

*Values can only be altered one at a time*

To create a new config from scratch interactively::
    fyrd conf init [--defaults]
"""
CONF_LIST_HELP = """\
By default shows the current config, which is based on the config file, but
with all parsing modifications applied, meaning that extra options or sections
are ignored.

To print the file as is, pass --file, or to limit to a single section, pass
--section <section>
"""

PROFILE_HELP = """\
Fyrd jobs use keyword arguments to run (for a complete list run this script
with the keywords command). These keywords can be bundled into profiles, which
are kept in {}. This file can be edited directly or manipulated here.
""".format(fyrd.conf.get_option('jobs', 'profile_file'))
PROF_EPI = """\
Show::
    fyrd prof show

Delete::
    fyrd prof delete <name>

Update::
    fyrd prof update <name> <options>

Add::
    fyrd prof add <name> <options>

<options>:
    The options arguments must be in the following format::
        opt:val opt2:val2 opt3:val3

Note: the DEFAULT profile is special and cannot be deleted, deleting it will
cause it to be instantly recreated with the default values. Values from this
profile will be available in EVERY other profile if they are not overriden
there. i.e. if DEFAULT contains `partition=normal`, if 'long' does not have
a 'partition' option, it will default to 'normal'.

To reset the profile to defaults, just delete the file and run this script
again.
"""

QUEUE_HELP = """\
Check the local queue, similar to squeue or qstat but simpler, good for
quickly checking the queue.

By default it searches only your own jobs, pass '--all-users' or
'--users <user> [<user2>...]' to change that behavior.

To just list jobs with some basic info, run with no arguments.
"""

WAIT_HELP = """\
Wait on a list of jobs, block until they complete.
"""

DEFAULT_CONF_SECTIONS = set(fyrd.conf.DEFAULTS.keys())
DEFAULT_CONF_OPTS = set(
    chain(*[list(i.keys()) for i in fyrd.conf.DEFAULTS.values()])
)

with open(fyrd.conf.CONFIG_FILE) as fin:
    CURRENT_CONF = fin.read()

CONF_UPDATE = """\
Update one option at a time.

Sections: {}
""".format(DEFAULT_CONF_SECTIONS)
CONF_UPDATE_EPI = """\
Current config:
{}
""".format(CURRENT_CONF)

KEYWORD_INFO = """\

=================
fyrd keyword help
=================

All job submission functions in this module make use of the same simple
keyword arguments. These keywords can be specified directly in `Job` based
function calls::

    fyrd.submit('ls', cores=10, mem=8000)

They can also be bundled together into profiles with the cluster-profile
script, and then specified as a profile::

    fyrd.submit('ls', profile='large')

Available keywords
==================

"""

CLEAN_HELP = """\
Clean all intermediate files created by the cluster module from this dir.

Uses the fyrd.job.clean_dir() function

Caution:
    The clean() function will delete **EVERY** file with
    extensions matching those these::

        .<suffix>.err
        .<suffix>.out
        .<suffix>.sbatch & .fyrd.script for slurm mode
        .<suffix>.qsub for torque mode
        .<suffix> for local mode
        _func.<suffix>.py
        _func.<suffix>.py.pickle.in
        _func.<suffix>.py.pickle.out

"""

###############################################################################
#                Functions to work with Command Line Arguments                #
###############################################################################


############
#  Config  #
############

def config(args):
    """Handle config management."""
    if not args.cmnd:
        sys.stdout.write('Subcommands required, for help run fyrd conf -h\n')
        return 1
    print(args)


def show_config(args):
    """Print the current config."""
    if args.file:
        print('Current config file ({}):\n'.format(fyrd.conf.CONFIG_FILE))
        with open(fyrd.conf.CONFIG_FILE) as fin1:
            print(fin1.read())
    else:
        print('Current config, the parsed output of the config file:\n')
        sections = args.sections if args.sections \
                                else fyrd.conf.DEFAULTS.keys()
        sections = sorted(sections, key=_sort_conf)
        for section in sections:
            print('[{}]'.format(section))
            sect = fyrd.conf.get_option(section=section)
            for opt, val in sect.items():
                print('{}: {}'.format(opt, val))
            print()
    print('For an explanation of options, run fyrd conf help')


def display_conf_help(args):
    """Print helpful information about the config options."""
    print(fyrd.conf.CONF_HELP['summary'])
    sections = args.sections if args.sections \
                            else fyrd.conf.DEFAULTS.keys()
    sections = sorted(sections, key=_sort_conf)
    for section in sections:
        print(fyrd.conf.CONF_HELP[section])


def update_option(args):
    """Update a single config option."""
    fyrd.conf.set_option(args.section, args.option, args.value)
    print('Done')


def init_config(args):
    """Start config initialization."""
    if args.defaults:
        if args.yes:
            ans = fyrd.run.get_input('Overwrite current config? [y/N] ')
            if ans.lower() != 'y':
                print('Aborting')
                return 1
            fyrd.conf.create_config()
    else:
        fyrd.conf.create_config_interactive(prompt=args.yes)
    print('Done\n')
    print('New config:\n')
    for section in sorted(fyrd.conf.DEFAULTS, key=_sort_conf):
        print('[{}]'.format(section))
        sect = fyrd.conf.get_option(section=section)
        for opt, val in sect.items():
            print('{}: {}'.format(opt, val))
        print()
    print('Current profiles:\n')
    list_profiles()


#############
#  Profile  #
#############


def profile(args):
    """This shouldn't be called."""
    print(args)
    sys.stdout.write('Subcommands required, for help run fyrd prof -h\n')
    return 1


def list_profiles(_=None):
    """List all profiles."""
    profiles = fyrd.conf.get_profile()
    print(profiles.pop('DEFAULT'))
    for prof in profiles.values():
        print(prof)


def add_profile(args):
    """Add a profile.

    Args:
        args (Namespace): Argparse command line arguments defined in main.

    """
    add_edit_profile(args, False)


def edit_profile(args):
    """Edit a profile.

    Args:
        args (Namespace): Argparse command line arguments defined in main.

    """
    add_edit_profile(args, True)


def del_profile(args):
    """Delete a profile.

    Args:
        args (Namespace): Argparse command line arguments defined in main.

    """
    args.name = 'DEFAULT' if args.name.lower() == 'default' else args.name

    print('This will delete the {} profile.'.format(args.name))
    if fyrd.run.get_input('Are you sure? [y/N] ', ['y', 'n']).lower() == 'y':
        fyrd.conf.del_profile(args.name)
        print('Done')
    else:
        print('Aborting')

    if args.name == 'DEFAULT':
        sys.stderr.write('Cannot delete DEFAULT profile, ' +
                         'resetting it instead\n')
        fyrd.conf.load_profiles()


def delete_profile_option(args):
    """Remove an option from a profile.

    Args:
        args (Namespace): Argparse command line arguments defined in main.

    """
    for opt in args.options:
        print('Removing {} from {}'.format(opt, args.section))
        fyrd.conf.profiles.remove_option(args.section, opt)
    print('Done')


#######################
#  Other Subcommands  #
#######################



def keyword_help(args):
    """Print keyword info."""
    if args.split_tables:
        print(KEYWORD_INFO + fyrd.options.option_help(mode='table'))
    elif args.table:
        print(KEYWORD_INFO + fyrd.options.option_help(mode='merged_table'))
    elif args.list:
        print(fyrd.options.option_help(mode='list'))
    else:
        print(KEYWORD_INFO + fyrd.options.option_help())


def queue(args):
    """Handle queue management."""
    # Create queue object
    if args.all_users or args.users:
        q = fyrd.queue.Queue()
    else:
        q = fyrd.queue.Queue(user='self')

    # Get jobs
    if args.running:
        jobs = q.running
    elif args.queued:
        jobs = q.queued
    elif args.done:
        jobs = q.completed
    elif args.bad:
        jobs = q.bad
    else:
        jobs = q.jobs

    # Filter users
    if args.users:
        jobs = {i: j for i, j in jobs.items() if j.owner in args.users}

    if not jobs:
        jobs = {}

    # Print requested output
    if args.count:
        print("{0}".format(len(jobs)))
    elif args.list:
        print("{0}".format('\n'.join([str(i) for i in jobs.keys()])))
    else:
        out_table = []
        for jid, job in jobs.items():
            cols = [jid, job.name, job.state]
            if args.users or args.all_users:
                cols.append(job.owner)
            out_table.append(cols)
        headers = ['ID', 'Name', 'State']
        if args.users or args.all_users:
            headers.append('User')
        print('\n' + tabulate(out_table, headers=headers) + '\n')


def wait(args):
    """Wait on jobs."""
    q = fyrd.queue.Queue()
    if args.users:
        users = args.users.strip().split(',')
        args.jobs += list(q.get_user_jobs(users).values())
    q.wait(args.jobs)


def clean_dir(args):
    """Clean up a job directory."""
    files = fyrd.basic.clean_dir(directory=args.dir, suffix=args.suffix,
                                 qtype=args.qtype, confirm=args.no_confirm,
                                 delete_outputs=args.outputs)

    if not files and args.no_confirm:
        print('No files found.')
        return

    # Print list of files if it wasn't done by the function
    if not args.no_confirm:
        sys.stdout.write('Deleted files:\n\t')
        sys.stdout.write('\n\t'.join(files))
        sys.stdout.write('\n')


######################
#  Helper Functions  #
######################


def add_edit_profile(args, overwrite):
    """Add or edit a profile.

    Arguments:
        args:      Command line arguments defined in main.
        overwrite: Edit a profile instead of adding one.

    """
    args.name = 'DEFAULT' if args.name.lower() == 'default' else args.name
    # Parse values
    values = get_values(args.options)

    if not overwrite and fyrd.conf.get_profile(args.name):
        sys.stderr.write("Profile '{}' already exists, use edit to change.\n"
                         .format(args.name))
        sys.exit(2)

    # Try to set the profile
    try:
        fyrd.conf.set_profile(args.name, values)
        print('Success')
    except fyrd.options.OptionsError as err:
        sys.stderr.write(err + '\n')
        sys.exit(3)


def get_values(keywords):
    """Return a dictionary of {keyword: arg} from a list of 'keyword:arg'.

    Args:
        keywords (list): A list of strings in the format keyword:arg.

    Returns
    dict: A dictionary of keyword arguments.

    """
    values = {}
    for arg in keywords:
        try:
            key, value = arg.split(':', 1)
        except ValueError:
            sys.stderr.write('Arguments must be in the format keyword:arg\n')
            sys.exit(1)
        values[key] = value
    return values


def _sort_conf(key):
    """Use with sorted: return an integer value for each section in config."""
    sections = {'queue': 1, 'jobs': 2, 'jobqueue': 3}
    return(sections[key])


###############################################################################
#                            Command Line Parsing                             #
###############################################################################

class AliasedSubParsersAction(argparse._SubParsersAction):

    """Allow aliased subparser arguments in python2.

    From: https://gist.github.com/sampsyo/471779

    """

    class _AliasedPseudoAction(argparse.Action):

        """Print alias info."""

        def __init__(self, name, aliases, hlp):
            """Print alias info."""
            dest = name
            if aliases:
                dest += ' (%s)' % ','.join(aliases)
            sup = super(AliasedSubParsersAction._AliasedPseudoAction, self)
            sup.__init__(option_strings=[], dest=dest, help=hlp)

    def add_parser(self, name, **kwargs):
        """Add a parser with aliases."""
        if 'aliases' in kwargs:
            aliases = kwargs['aliases']
            del kwargs['aliases']
        else:
            aliases = []

        assert 'aliases' not in kwargs
        parser = super(AliasedSubParsersAction, self).add_parser(
            name, **kwargs)

        # Make the aliases work.
        for alias in aliases:
            self._name_parser_map[alias] = parser
        # Make the help text reflect them, first removing old help entry.
        if 'help' in kwargs:
            hlp = kwargs.pop('help')
            self._choices_actions.pop()
            pseudo_action = self._AliasedPseudoAction(name, aliases, hlp)
            self._choices_actions.append(pseudo_action)

        return parser

def command_line_parser():
    """Parse command line options.

    Returns:
        argparse parser
    """

    parser  = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    if sys.version_info.major == 2:
        parser.register('action', 'parsers', AliasedSubParsersAction)

    # Global arguments
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Show debug outputs')

    # Subcommands
    modes = parser.add_subparsers(
        dest='modes', metavar='{conf,prof,keywords,queue,wait,clean}')

    #########################
    #  Config Manipulation  #
    #########################

    conf_sub = modes.add_parser(
        'conf', description=CONF_HELP, epilog=CONF_EPI, aliases=['config'],
        help="View and manage the config",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    if sys.version_info.major == 2:
        conf_sub.register('action', 'parsers', AliasedSubParsersAction)
    conf = conf_sub.add_subparsers(dest='cmnd')

    # Printing config
    conf_list = conf.add_parser('show', aliases=['list'],
                                description=CONF_LIST_HELP,
                                help="Show current config")
    conf_list_args = conf_list.add_mutually_exclusive_group()
    conf_list.add_argument('-s', '--sections', metavar='', nargs='+',
                           choices=DEFAULT_CONF_SECTIONS,
                           help="Limit results to a list of sections")
    conf_list_args.add_argument('-f', '--file', action='store_true',
                                help="Print file contents only")
    conf_list.set_defaults(func=show_config)

    # Config help
    conf_help = conf.add_parser('help',
                                help="Show info on every config option")
    conf_help.add_argument('-s', '--sections', metavar='', nargs='+',
                           choices=DEFAULT_CONF_SECTIONS,
                           help="Limit results to a list of sections")
    conf_help.set_defaults(func=display_conf_help)

    # Updating config
    conf_update = conf.add_parser(
        'update', help="Update the config", aliases=['alter'],
        description=CONF_UPDATE, epilog=CONF_UPDATE_EPI,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    conf_update.add_argument('section', help="Section to update",
                             choices=DEFAULT_CONF_SECTIONS,
                             metavar='section')
    conf_update.add_argument('option', help="Option to update",
                             choices=DEFAULT_CONF_OPTS, metavar='option')
    conf_update.add_argument('value', help="New value for option")
    conf_update.set_defaults(func=update_option)

    # Initializing config
    conf_init = conf.add_parser(
        'init', help="Interactively initialize the config",
        description="Overwrites the exiting config and starts from scratch. " +
        "Asks for input from user for default options."
    )
    conf_init.add_argument('--defaults', action='store_true',
                           help="Non-interactive, just use builtin defaults")
    conf_init.add_argument('--yes', action='store_false',
                           help="Do not as for confirmation")
    conf_init.set_defaults(func=init_config)

    # Set function
    conf_sub.set_defaults(func=config)

    ######################
    #  Profile Handling  #
    ######################

    prof_sub = modes.add_parser(
        'prof', description=PROFILE_HELP, epilog=PROF_EPI, aliases=['profile'],
        help="Manage profiles",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    if sys.version_info.major == 2:
        prof_sub.register('action', 'parsers', AliasedSubParsersAction)
    prof = prof_sub.add_subparsers(dest='cmnd')

    # List profiles
    prof_show = prof.add_parser('show', aliases=['list'],
                                help="Print current profiles")
    prof_show.add_argument('-n', '--name', nargs='+',
                           help="Limit to only these profiles")
    prof_show.set_defaults(func=list_profiles)

    # Add a profile
    prof_add = prof.add_parser('add', aliases=['new'],
                               help="Add a new profile")
    prof_add.add_argument('name', help='Profile name')
    prof_add.add_argument('options', nargs='+', help="Options to update")
    prof_add.set_defaults(func=add_profile)

    # Update profile
    prof_update = prof.add_parser('update', aliases=['alter', 'edit'],
                                  help="Update an existing profile")
    prof_update.add_argument('name', help='Profile name')
    prof_update.add_argument('options', nargs='+', help="Options to update")
    prof_update.set_defaults(func=edit_profile)

    # Remove an option
    prof_optdel = prof.add_parser('remove-option', aliases=['del-option'],
                                  help="Remove a profile option")
    prof_optdel.add_argument('name', help='Profile name')
    prof_optdel.add_argument('options', nargs='+', help="Options to remove")
    prof_optdel.set_defaults(func=delete_profile_option)

    # Delete a profile
    prof_del = prof.add_parser('delete', aliases=['del'],
                               help="Delete an existing profile")
    prof_del.add_argument('name', help='Profile name')
    prof_del.set_defaults(func=del_profile)

    ##################
    #  Keyword Help  #
    ##################

    keywords = modes.add_parser('keywords', aliases=['keys', 'options'],
                                help="Print available keyword arguments.")
    keywords_grp = keywords.add_mutually_exclusive_group()
    keywords_grp.add_argument('-t', '--table', action='store_true',
                              help="Print keywords as a table")
    keywords_grp.add_argument('-s', '--split-tables', action='store_true',
                              help="Print keywords as multiple tables")
    keywords_grp.add_argument('-l', '--list', action='store_true',
                              help="Print a list of keywords only")
    keywords.set_defaults(func=keyword_help)

    ###################
    #  Queue Parsing  #
    ###################

    queue_sub = modes.add_parser(
        'queue', aliases=['q'],
        description=QUEUE_HELP, help="Search the queue",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # User and partition filtering
    queue_filter = queue_sub.add_argument_group('queue filtering')
    queue_filter_m = queue_filter.add_mutually_exclusive_group()
    queue_filter_m.add_argument('-u', '--users', nargs='+', metavar='',
                                help='Limit to these users')
    queue_filter_m.add_argument('-a', '--all-users', action='store_true',
                                help='Display jobs for all users')
    queue_filter.add_argument('-p', '--partitions', nargs='+', metavar='',
                              help="Limit to these partitions (queues)")

    # State filtering
    queue_filter = queue_sub.add_argument_group('queue state filtering')
    queue_filter_s = queue_filter.add_mutually_exclusive_group()
    queue_filter_s.add_argument('-r', '--running', action='store_true',
                                help="Show only running jobs")
    queue_filter_s.add_argument('-q', '--queued', action='store_true',
                                help="Show only queued jobs")
    queue_filter_s.add_argument('-d', '--done', action='store_true',
                                help="Show only completed jobs")
    queue_filter_s.add_argument('-b', '--bad', action='store_true',
                                help="Show only completed jobs")

    # Display mode
    queue_disp_group = queue_sub.add_argument_group('display options')
    queue_disp = queue_disp_group.add_mutually_exclusive_group()
    queue_disp.add_argument('-l', '--list', action='store_true',
                            help="Print job numbers only, works well with " +
                            "xargs")
    queue_disp.add_argument('-c', '--count', action='store_true',
                            help="Print job count only")

    # Set function
    queue_sub.set_defaults(func=queue)

    #################
    #  Job Waiting  #
    #################

    wait_sub = modes.add_parser(
        'wait', description=WAIT_HELP, help="Wait for jobs",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    wait_sub.add_argument('jobs', nargs='*', help="Job list to wait for")
    wait_sub.add_argument('-u', '--users',
                          help='A comma-separated list of users to wait for')

    # Set function
    wait_sub.set_defaults(func=wait)

    ########################
    #  Directory Cleaning  #
    ########################

    clean = modes.add_parser(
        'clean', description=CLEAN_HELP, help="Clean up a job directory",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    clean.add_argument('-o', '--outputs', action='store_true',
                       help="Clean output files too")
    clean.add_argument('-d', '--dir',
                       default=fyrd.conf.get_option('jobs', 'filepath'),
                       help="Directory to clean")
    clean.add_argument('-s', '--suffix',
                       default=fyrd.conf.get_option('jobs', 'suffix'),
                       help="Suffix to use for cleaning")
    clean.add_argument('-q', '--qtype', choices=('torque', 'slurm', 'local'),
                       help="Limit deletions to this qtype")

    # We store this as false as the question is a negative
    clean.add_argument('-n', '--no-confirm', action='store_false',
                       help="Do not confirm before deleting (for scripts)")

    # Set function
    clean.set_defaults(func=clean_dir)

    return parser


###############################################################################
#                             Running as a Script                             #
###############################################################################


def main(argv=None):
    """Parse command line options to run as a script."""
    if not argv:
        argv = sys.argv[1:]

    parser = command_line_parser()

    args = parser.parse_args(argv)

    if not args.modes:
        parser.print_help()
        return 0

    if args.verbose:
        fyrd.logme.MIN_LEVEL = 'debug'

    # Call the subparser function
    try:
        return args.func(args)
    except TypeError:
        print(args)
        raise


if __name__ == '__main__':
    sys.exit(main())
