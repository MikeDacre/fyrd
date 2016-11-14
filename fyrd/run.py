# -*- coding: utf-8 -*-
"""
File management and execution functions.

Last modified: 2016-11-11 02:45
"""
import os
import re
import sys
import bz2
import gzip
import argparse
from subprocess import Popen
from subprocess import PIPE
from time import sleep

from . import logme

__all__ = ['cmd', 'which', 'open_zipped']


###############################################################################
#                               Useful Classes                                #
###############################################################################


class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter,
                      argparse.RawDescriptionHelpFormatter):

    """Custom argparse formatting."""

    pass


class CommandError(Exception):

    """A custom exception."""

    pass


###############################################################################
#                              Useful Functions                               #
###############################################################################


def open_zipped(infile, mode='r'):
    """Open a regular, gzipped, or bz2 file.

    If infile is a file handle or text device, it is returned without
    changes.

    Returns:
        text mode file handle.
    """
    mode   = mode[0] + 't'
    if hasattr(infile, 'write'):
        return infile
    if isinstance(infile, str):
        if infile.endswith('.gz'):
            return gzip.open(infile, mode)
        if infile.endswith('.bz2'):
            if hasattr(bz2, 'open'):
                return bz2.open(infile, mode)
            else:
                return bz2.BZ2File(infile, mode)
        return open(infile, mode)


def opt_split(opt, split_on):
    """Split opt by chars in split_on, merge all into single list."""
    if not isinstance(opt, (list, tuple, set)):
        opt = [opt]
    if not isinstance(split_on, (list, tuple, set)):
        split_on = [split_on]
    final_list = []
    for o in opt:
        final_list += re.split('[{}]'.format(''.join(split_on)), o)
    return list(set(final_list)) # Return unique options only, order lost.


def cmd(command, args=None, stdout=None, stderr=None, tries=1):
    """Run command and return status, output, stderr.

    Args:
        command (str): Path to executable.
        args (tuple):  Tuple of arguments.
        stdout (str):  File or open file like object to write STDOUT to.
        stderr (str):  File or open file like object to write STDERR to.
        tries (int):   Number of times to try to execute. 1+

    Returns:
        tuple: exit_code, STDOUT, STDERR
    """
    tries = int(tries)
    assert tries > 0
    count = 1
    if isinstance(command, (list, tuple)):
        if args:
            raise Exception('Cannot submit list/tuple command as i' +
                            'well as args argument')
        command = ' '.join(command)
    assert isinstance(command, str)
    if args:
        if isinstance(args, (list, tuple)):
            args = ' '.join(args)
        args = command + args
    else:
        args = command
    logme.log('Running {} as {}'.format(command, args), 'verbose')
    while True:
        try:
            pp = Popen(args, shell=True, universal_newlines=True,
                       stdout=PIPE, stderr=PIPE)
        except FileNotFoundError:
            logme.log('{} does not exist'.format(command), 'critical')
            raise
        out, err = pp.communicate()
        code = pp.returncode
        if code == 0 or count == tries:
            break
        logme.log('Command {} failed with code {}, retrying.'
                  .format(command, code), 'warn')
        sleep(1)
        count += 1
    logme.log('{} completed with code {}'.format(command, code), 'debug')
    if stdout:
        with open_zipped(stdout, 'w') as fout:
            fout.write(out)
    if stderr:
        with open_zipped(stderr, 'w') as fout:
            fout.write(err)
    return code, out.rstrip(), err.rstrip()


def is_exe(fpath):
    """Return True is fpath is executable."""
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)


def which(program):
    """Replicate the UNIX which command.

    Taken verbatim from:
        stackoverflow.com/questions/377017/test-if-executable-exists-in-python

    Args:
        program: Name of executable to test.

    Returns:
        Path to the program or None on failure.
    """
    fpath, program = os.path.split(program)
    if fpath:
        if is_exe(program):
            return os.path.abspath(program)
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return os.path.abspath(exe_file)

    return None


def file_type(infile):
    """Return file type after stripping gz or bz2."""
    name_parts = infile.split('.')
    if name_parts[-1] == 'gz' or name_parts[-1] == 'bz2':
        name_parts.pop()
    return name_parts[-1]


def is_file_type(infile, types):
    """Return True if infile is one of types.

    Args:
        infile:  Any file name
        types:   String or list/tuple of strings (e.g ['bed', 'gtf'])

    Returns:
        True or False
    """
    if hasattr(infile, 'write'):
        return False
    if isinstance(types, str):
        types = [types]
    if not isinstance(types, (list, tuple)):
        raise Exception('types must be string list or tuple')
    for typ in types:
        if file_type(infile) == typ:
            return True
    return False


def write_iterable(iterable, outfile):
    """Write all elements of iterable to outfile."""
    with open_zipped(outfile, 'w') as fout:
        fout.write('\n'.join(iterable))


def indent(string, prefix='    '):
    """Replicate python3's textwrap.indent for python2.

    Args:
        string (str): Any string.
        prefix (str): What to indent with.

    Returns:
        str: Indented string
    """
    out = ''
    for i in string.split('\n'):
        out += '{}{}\n'.format(prefix, i)
    return out


def check_pid(pid):
    """Check For the existence of a unix pid."""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def get_input(message, valid_answers=None):
    """Get input from the command line and check answers.

    Allows input to work with python 2/3

    Args:
        message (str):        A message to print, an additional space will be
                              added.
        valid_answers (list): A list of answers to accept, if None, ignored.
                              Case insensitive.

    Returns:
        str: The response
    """
    if valid_answers:
        if isinstance(valid_answers, str):
            valid_answers = [valid_answers]
        if not isinstance(valid_answers, (list, tuple, set, frozenset)):
            logme.log('valid_answers must be a list, is {}'
                      .format(type(valid_answers)), 'critical')
            raise ValueError('Invalid argument')
        valid_answers = [i.lower() for i in valid_answers]
        while True:
            ans = _get_input(message)
            if ans.lower() in valid_answers:
                return ans
            else:
                logme.log('Invalid response to input question', 'debug')
                sys.stderr.write('Invalid response: {}\n'.format(ans) +
                                 'Valid responses: {}\n'
                                 .format(valid_answers) +
                                 'Please try again.\n')
    else:
        return _get_input(message)


def _get_input(message):
    """Run either input or raw input depending on python version."""
    if sys.version_info.major == 2:
        return raw_input(message)
    else:
        return input(message)


###############################################################################
#                          Scripts to Write to File                           #
###############################################################################


SCRP_RUNNER = """\
#!/bin/bash
{precmd}
mkdir -p $LOCAL_SCRATCH > /dev/null 2>/dev/null
if [ -f {script} ]; then
    {command}
else
    echo "{script} does not exist, make sure you set your filepath to a "
    echo "directory that is available to the compute nodes."
    exit 1
fi
"""

SCRP_RUNNER_TRACK = """\
#!/bin/bash
{precmd}
mkdir -p $LOCAL_SCRATCH > /dev/null 2>/dev/null
if [ -f {script} ]; then
    cd {usedir}
    date +'%y-%m-%d-%H:%M:%S'
    echo "Running {name}"
    {command}
    exitcode=$?
    echo Done
    date +'%y-%m-%d-%H:%M:%S'
    if [[ $exitcode != 0 ]]; then
        echo Exited with code: $exitcode >&2
    fi
else
    echo "{script} does not exist, make sure you set your filepath to a "
    echo "directory that is available to the compute nodes."
    exit 1
fi
"""

CMND_RUNNER = """\
#!/bin/bash
{precmd}
mkdir -p $LOCAL_SCRATCH > /dev/null 2>/dev/null
cd {usedir}
{command}
exitcode=$?
if [[ $exitcode != 0 ]]; then
    echo Exited with code: $exitcode >&2
fi
"""

CMND_RUNNER_TRACK = """\
#!/bin/bash
{precmd}
mkdir -p $LOCAL_SCRATCH > /dev/null 2>/dev/null
cd {usedir}
date +'%y-%m-%d-%H:%M:%S'
echo "Running {name}"
{command}
exitcode=$?
echo Done
date +'%y-%m-%d-%H:%M:%S'
if [[ $exitcode != 0 ]]; then
    echo Exited with code: $exitcode >&2
fi
"""

FUNC_RUNNER = r"""\
'''
Run a function remotely and pickle the result.

To try and make this as resistent to failure as possible, we import everything
we can, this sometimes results in duplicate imports and always results in
unnecessary imports, but given the context we don't care, as we just want the
thing to run successfully on the first try, no matter what.
'''
import os
import sys
import socket
from subprocess import Popen, PIPE
# Try to use dill, revert to pickle if not found
try:
    import dill as pickle
except ImportError:
    try:
        import cPickle as pickle # For python2
    except ImportError:
        import pickle

out = None
try:
{imports}
{modimpstr}
except Exception as e:
    out = e

ERR_MESSAGE = '''\
Failed to import your function. This usually happens when you have a module
installed locally that is not available on the compute nodes.\n In this case
the module is {{}}.

However, I can only catch the first uninstalled module. To make sure all of
your modules are installed on the compute nodes, do this::

    freeze --local | grep -v ^\-e | cut -d = -f 1 "> module_list.txt\n

Then, submit a job to the compute nodes with this command::

    cat module_list.txt | xargs pip install --user
'''


def run_function(func_c, args=None, kwargs=None):
    '''Run a function with arglist and return output.'''
    if not hasattr(func_c, '__call__'):
        raise Exception('{{}} is not a callable function.'
                        .format(func_c))
    if args and kwargs:
        ot = func_c(*args, **kwargs)
    elif args:
        try:
            iter(args)
        except TypeError:
            args = (args,)
        if isinstance(args, str):
            args = (args,)
        ot = func_c(*args)
    elif kwargs:
        ot = func_c(**kwargs)
    else:
        ot = func_c()
    return ot


if __name__ == "__main__":
    # If an Exception was raised during import, skip this
    if not out:
        with open('{pickle_file}', 'rb') as fin:
            # Try to install packages first
            try:
                function_call, args, kwargs = pickle.load(fin)
            except ImportError as e:
                module = str(e).split(' ')[-1]
                node   = socket.gethostname()
                sys.stderr.write(ERR_MESSAGE.format(module))
                out = ImportError(('Module {{}} is not installed on compute '
                                   'node {{}}').format(module, node))

    try:
        if not out:
            out = run_function(function_call, args, kwargs)
    except Exception as e:
        out = e

    with open('{out_file}', 'wb') as fout:
        pickle.dump(out, fout)
"""
