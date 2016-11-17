# -*- coding: utf-8 -*-
"""
File management and execution functions.
"""
import os as _os
import re as _re
import sys as _sys
import inspect as _inspect
import argparse as _argparse

import bz2
import gzip
from subprocess import Popen
from subprocess import PIPE
from time import sleep

from . import logme as _logme

__all__ = ['cmd', 'which', 'open_zipped']


###############################################################################
#                               Useful Classes                                #
###############################################################################


class CustomFormatter(_argparse.ArgumentDefaultsHelpFormatter,
                      _argparse.RawDescriptionHelpFormatter):

    """Custom argparse formatting."""

    pass


class CommandError(Exception):

    """A custom exception."""

    pass


###############################################################################
#                               Misc Functions                                #
###############################################################################


def listify(iterable):
    """Try to force any iterable into a list sensibly."""
    if isinstance(iterable, list):
        return iterable
    if isinstance(iterable, (str, int, float)):
        return [iterable]
    return list(iter(iterable))


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


###############################################################################
#                               File Management                               #
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


def split_file(infile, parts, outpath='', keep_header=False):
    """Split a file in parts and return a list of paths.

    NOTE: Linux specific (uses wc).

    **Note**: If has_header is True, the top line is stripped off the infile
    prior to splitting and assumed to be the header.

    Args:
        outpath:     The directory to save the split files.
        has_header:  Add the header line to the top of every file.

    Returns:
        list: Paths to split files.
    """
    # Determine how many reads will be in each split sam file.
    _logme.log('Getting line count', 'debug')
    num_lines = int(_os.popen(
        'wc -l ' + infile + ' | awk \'{print $1}\'').read())
    num_lines   = int(int(num_lines)/int(parts)) + 1

    # Subset the file into X number of jobs, maintain extension
    cnt       = 0
    currjob   = 1
    suffix    = '.split_' + str(currjob).zfill(4) + '.' + infile.split('.')[-1]
    file_name = _os.path.basename(infile)
    run_file  = _os.path.join(outpath, file_name + suffix)
    outfiles  = [run_file]

    # Actually split the file
    _logme.log('Splitting file', 'debug')
    with open(infile) as fin:
        header = fin.readline() if keep_header else ''
        sfile = open(run_file, 'w')
        sfile.write(header)
        for line in fin:
            cnt += 1
            if cnt < num_lines:
                sfile.write(line)
            elif cnt == num_lines:
                sfile.write(line)
                sfile.close()
                currjob += 1
                suffix = '.split_' + str(currjob).zfill(4) + '.' + \
                    infile.split('.')[-1]
                run_file = _os.path.join(outpath, file_name + suffix)
                sfile = open(run_file, 'w')
                outfiles.append(run_file)
                sfile.write(header)
                cnt = 0
        sfile.close()
    return tuple(outfiles)


def is_exe(fpath):
    """Return True is fpath is executable."""
    return _os.path.isfile(fpath) and _os.access(fpath, _os.X_OK)


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


###############################################################################
#                              Running Commands                               #
###############################################################################


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
    _logme.log('Running {} as {}'.format(command, args), 'verbose')
    while True:
        try:
            pp = Popen(args, shell=True, universal_newlines=True,
                       stdout=PIPE, stderr=PIPE)
        except FileNotFoundError:
            _logme.log('{} does not exist'.format(command), 'critical')
            raise
        out, err = pp.communicate()
        code = pp.returncode
        if code == 0 or count == tries:
            break
        _logme.log('Command {} failed with code {}, retrying.'
                   .format(command, code), 'warn')
        sleep(1)
        count += 1
    _logme.log('{} completed with code {}'.format(command, code), 'debug')
    if stdout:
        with open_zipped(stdout, 'w') as fout:
            fout.write(out)
    if stderr:
        with open_zipped(stderr, 'w') as fout:
            fout.write(err)
    return code, out.rstrip(), err.rstrip()


def which(program):
    """Replicate the UNIX which command.

    Taken verbatim from:
        stackoverflow.com/questions/377017/test-if-executable-exists-in-python

    Args:
        program: Name of executable to test.

    Returns:
        Path to the program or None on failu_re.
    """
    fpath, program = _os.path.split(program)
    if fpath:
        if is_exe(program):
            return _os.path.abspath(program)
    else:
        for path in _os.environ["PATH"].split(_os.pathsep):
            path = path.strip('"')
            exe_file = _os.path.join(path, program)
            if is_exe(exe_file):
                return _os.path.abspath(exe_file)

    return None


def check_pid(pid):
    """Check For the existence of a unix pid."""
    try:
        _os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


###############################################################################
#                       Option and Argument Management                        #
###############################################################################


def replace_argument(args, find_string, replace_string, error=True):
    """Replace find_string with replace string in a tuple or dict.

    If dict, the values are replaced, not the keys.

    Note: args can also be a list, in which case the first item is assumed
    to be a tuple, and the second a dictionary

    Args:
        args (list/tuple/dict): Tuple or dict of args
        find_string (str):      A string to search for
        replace_string (str):   A string to replace with
        error (bool):           Raise ValueError if replacement fails

    Returns:
        The same object as was passed, with alterations made.
    """
    double = False
    if isinstance(args, list):
        args, kwargs = args
        double = True
    elif isinstance(args, tuple):
        kwargs = None
    elif isinstance(args, dict):
        kwargs = args.copy()
        args   = None

    if not args and not kwargs:
        msg = 'No arguments or keyword arguments found'
        if error:
            raise ValueError(msg)
        else:
            _logme.log(msg, 'warn')
            if double:
                return None, None
            else:
                return None

    found = False
    newargs = tuple()
    if args:
        for arg in listify(args):
            if isinstance(arg, str) and find_string in arg:
                arg = arg.format(file=replace_string)
                found = True
            newargs += (arg,)
    newkwds = {}
    if kwargs:
        for arg, value in kwargs.items():
            if isinstance(value, str) and find_string in value:
                value = replace_string
                found = True
            newkwds[arg] = value

    if found is not True:
        msg = 'Could not find {}'.format(find_string)
        if error:
            raise ValueError(msg)
        else:
            _logme.log(msg, 'warn')
            if double:
                return None, None
            else:
                return None

    if double:
        return newargs, newkwds
    else:
        if newargs:
            return newargs
        else:
            return newkwds


def opt_split(opt, split_on):
    """Split opt by chars in split_on, merge all into single list."""
    if not isinstance(opt, (list, tuple, set)):
        opt = [opt]
    if not isinstance(split_on, (list, tuple, set)):
        split_on = [split_on]
    final_list = []
    for o in opt:
        final_list += _re.split('[{}]'.format(''.join(split_on)), o)
    return list(set(final_list)) # Return unique options only, order lost.


###############################################################################
#                                 User Input                                  #
###############################################################################


def get_yesno(message, default=None):
    """Get yes/no answer from user.

    Args:
        message (str): A message to print, an additional space will be added.
        default (str): One of {'y', 'n'}, the default if the user gives no
                       answer. If None, answer forced.

    Returns:
        bool: True on yes, False on no
    """
    if default:
        if default.lower().startswith('y'):
            tailstr = '[Y/n] '
        elif default.lower().startswith('n'):
            tailstr = '[y/N] '
        else:
            raise ValueError('Invalid default')
    else:
        tailstr = '[y/n] '
    message = message + tailstr if message.endswith(' ') \
                else message + ' ' + tailstr
    ans = get_input(message, 'yesno', default)
    if ans.lower().startswith('y'):
        return True
    elif ans.lower().startswith('n'):
        return False
    else:
        raise ValueError('Invalid response: {}'.format(ans))


def get_input(message, valid_answers=None, default=None):
    """Get input from the command line and check answers.

    Allows input to work with python 2/3

    Args:
        message (str):        A message to print, an additional space will be
                              added.
        valid_answers (list): A list of answers to accept, if None, ignored.
                              Case insensitive. There is one special option
                              here: 'yesno', this allows all case insensitive
                              variations of y/n/yes/no.
        default (str):        The default answer.

    Returns:
        str: The response
    """
    if not message.endswith(' '):
        message = message + ' '
    if valid_answers:
        if isinstance(valid_answers, str):
            if valid_answers.lower() == 'yesno':
                valid_answers = ['yes', 'no', 'y', 'n']
            else:
                valid_answers = [valid_answers]
        if not isinstance(valid_answers, (list, tuple, set, frozenset)):
            _logme.log('valid_answers must be a list, is {}'
                       .format(type(valid_answers)), 'critical')
            raise ValueError('Invalid argument')
        valid_answers = [i.lower() for i in valid_answers]
        while True:
            ans = _get_input(message)
            if not ans and default:
                return default
            if ans.lower() in valid_answers:
                return ans
            else:
                _logme.log('Invalid response to input question', 'debug')
                _sys.stderr.write('Invalid response: {}\n'.format(ans) +
                                  'Valid responses: {}\n'
                                  .format(valid_answers) +
                                  'Please try again.\n')
    else:
        return _get_input(message)


def _get_input(message):
    """Run either input or raw input depending on python version."""
    if _sys.version_info.major == 2:
        return raw_input(message)
    else:
        return input(message)


###############################################################################
#                                   Imports                                   #
###############################################################################


PROT_IMPT = """\
try:
    {}
except ImportError:
    pass
"""


def normalize_imports(imports, prot=True):
    """Take a heterogenous list of imports and normalize it.

    Args:
        imports (list): A list of strings, formatted differently.
        prot (bool):    Protect imports with try..except blocks

    Returns:
        list: A list of strings that can be used for imports
    """
    out_impts  = []
    prot_impts = []
    for imp in imports:
        if not isinstance(imp, str):
            raise ValueError('All imports must be strings')
        if imp.startswith('try:'):
            prot_impts.append(imp.rstrip())
        elif imp.startswith('import') or imp.startswith('from'):
            out_impts.append(imp.rstrip())
        else:
            if imp.startswith('@'):
                continue
            out_impts.append('import {}'.format(imp))

    if prot:
        for imp in out_impts:
            prot_impts.append(PROT_IMPT.format(imp))
        return prot_impts
    else:
        return out_impts + prot_impts


def get_imports(function, mode='string'):
    """Build a list of potentially useful imports from a function handle.

    Gets:
        - All modules from globals()
        - All modules from the function's globals()
        - All functions from the function's globals()

    Modes:
        string: Return a list of strings formatted as unprotected import calls
        prot:   Similar to string, but with try..except blocks
        list:   Return two lists: (import name, module name) for modules
                and (import name, function name, module name) for functions

    Args:
        function (callable): A function handle
        mode (str):          A string corresponding to one of the above modes

    Returns:
        str or list
    """
    if mode not in ['string', 'prot', 'list']:
        raise ValueError('mode must be one of string/prot/list')

    rootmod  = _inspect.getmodule(function)

    imports      = []
    func_imports = []

    # Import everything in current and function globals
    import_places = [
        dict(globals().items()),
        dict(_inspect.getmembers(function))['__globals__'],
    ]

    # Modules
    for place in import_places:
        for name, item in place.items():
            if _inspect.ismodule(item):
                if name != '__main__' or not name.startswith('__'):
                    imports.append((name, item.__name__))

    # Functions
    for name, item in import_places[1].items():
        if callable(item):
            try:
                func_imports.append((name, item.__name__, item.__module__))
            except AttributeError:
                pass

    # Import all modules in the root module
    imports += [(k,v.__name__) for k,v in
                _inspect.getmembers(rootmod, _inspect.ismodule)
                if not k.startswith('__')]

    # Make unique
    imports      = sorted(list(set(imports)), key=_sort_imports)
    func_imports = sorted(list(set(func_imports)), key=_sort_imports)

    _logme.log('Imports: {}'.format(imports), 'debug')
    _logme.log('Func imports: {}'.format(func_imports), 'debug')

    # Create a sane set of imports
    ignore_list = ['os', 'sys', 'dill', 'pickle', '__main__']
    filtered_imports      = []
    filtered_func_imports = []
    for iname, name in imports:
        if iname in ignore_list:
            continue
        if name.startswith('@') or iname.startswith('@'):
            continue
        filtered_imports.append((iname, name))
    for iname, name, mod in func_imports:
        if iname in ignore_list:
            continue
        if name.startswith('@') or iname.startswith('@'):
            continue
        filtered_func_imports.append((iname, name, mod))

    if mode == 'list':
        return filtered_imports, filtered_func_imports

    import_strings = []
    for iname, name in filtered_imports:
        names = name.split('.')
        if iname != name:
            if len(names) > 1:
                if '.'.join(names[1:]) != iname:
                    import_strings.append(
                        'from {} import {} as {}'
                        .format('.'.join(names[:-1]), names[-1], iname)
                    )
                else:
                    import_strings.append(
                        'from {} import {}'
                        .format(names[0], '.'.join(names[1:]))
                    )
            else:
                import_strings.append(
                    ('import {} as {}').format(name, iname)
                )
        else:
            import_strings.append('import {}'.format(name))

    # Function imports
    for iname, name, mod in filtered_func_imports:
        if iname == name:
            import_strings.append('from {} import {}'.format(mod, name))
        else:
            import_strings.append('from {} import {} as {}'
                                  .format(mod, name, iname)
                                 )

    if mode == 'string':
        return import_strings

    elif mode == 'prot':
        return normalize_imports(import_strings)

    else:
        raise ValueError('Mode changed unexpectedly')


def _sort_imports(x):
    """Sort a list of tuples and strings, for use with sorted."""
    if isinstance(x, tuple):
        if x[1] == '__main__':
            return 0
        return x[1]
    return x


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
                _sys.stderr.write(ERR_MESSAGE.format(module))
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
