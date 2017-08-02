# -*- coding: utf-8 -*-
"""
A library of useful functions used throughout the *fyrd* package.

These include functions to handle data, format outputs, handle file opening,
run commands, check file extensions, get user input, and search and format
imports.

These functions are not intended to be accessed directly.
"""
from __future__ import print_function
from __future__ import with_statement
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

from six import reraise
from six.moves import input as _get_input

from . import logme as _logme


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
    if not iterable:
        return []
    if callable(iterable):
        iterable = iterable()
    return list(iter(iterable))


def merge_lists(lists):
    """Turn a list of lists into a single list."""
    outlist = []
    for lst in listify(lists):
        outlist += lst
    return outlist


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


def is_exc(x):
    """Check if x is the output of sys.exc_info().

    Returns:
        bool: True if matched the output of sys.exc_info().
    """
    return bool(isinstance(x, tuple)
                and len(x) == 3
                and issubclass(BaseException, x[0]))


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


def block_read(files, size=65536):
    """Iterate through a file by blocks."""
    while True:
        b = files.read(size)
        if not b:
            break
        yield b


def count_lines(infile, force_blocks=False):
    """Return the line count of a file as quickly as possible.

    Uses `wc` if avaialable, otherwise does a rapid read.
    """
    if which('wc') and not force_blocks:
        _logme.log('Using wc', 'debug')
        if infile.endswith('.gz'):
            cat = 'zcat'
        elif infile.endswith('.bz2'):
            cat = 'bzcat'
        else:
            cat = 'cat'
        command = "{cat} {infile} | wc -l | awk '{{print $1}}'".format(
            cat=cat, infile=infile
        )
        return int(cmd(command)[1])
    else:
        _logme.log('Using block read', 'debug')
        with open_zipped(infile) as fin:
            return sum(bl.count("\n") for bl in block_read(fin))


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

    num_lines = int(count_lines(infile)/int(parts)) + 1

    # Subset the file into X number of jobs, maintain extension
    cnt       = 0
    currjob   = 1
    suffix    = '.split_' + str(currjob).zfill(4) + '.' + infile.split('.')[-1]
    file_name = _os.path.basename(infile)
    run_file  = _os.path.join(outpath, file_name + suffix)
    outfiles  = [run_file]

    # Actually split the file
    _logme.log('Splitting file', 'debug')
    with open_zipped(infile) as fin:
        header = fin.readline() if keep_header else ''
        sfile = open_zipped(run_file, 'w')
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
                sfile = open_zipped(run_file, 'w')
                outfiles.append(run_file)
                sfile.write(header)
                cnt = 0
        sfile.close()
    _logme.log('Split files: {}'.format(outfiles), 'debug')
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
        infile = infile.name
    types = listify(types)
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
            raise ValueError('Cannot submit list/tuple command as ' +
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


def export_run(function, args, kwargs):
    """Execute a function after first exporting all imports."""
    kwargs['imports'] = export_imports(function, kwargs)
    print('bob', kwargs['imports'])
    return function(*args, **kwargs)


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
    else:
        raise ValueError('args must be list/tuple/dict, is {}\nval: {}'
                         .format(type(args), args))

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
                arg = arg.format(**{find_string.strip('{}'): replace_string})
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
        return [newargs, newkwds]
    else:
        if newargs:
            return newargs
        else:
            return newkwds


def opt_split(opt, split_on):
    """Split options by chars in split_on, merge all into single list.

    Args:
        opt (list):      A list of strings, can be a single string.
        split_on (list): A list of characters to use to split the options.

    Returns:
        list: A single merged list of split options, uniqueness guaranteed,
              order not.
    """
    opt = listify(opt)
    split_on = listify(split_on)
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


###############################################################################
#                                   Imports                                   #
###############################################################################


def syspath_fmt(syspaths):
    """Take a list of paths and return a sys of sys.path.append strings."""
    outlist = []
    for pth in listify(syspaths):
        if 'sys.path' in pth:
            outlist.append(pth)
            continue
        if _os.path.exists(pth):
            outlist.append("sys.path.append('{}')".format(
                _os.path.abspath(pth)
            ))
        else:
            raise OSError('Paths must exist, {} does not.'
                          .format(pth))
    return '\n'.join(outlist)


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
    path_impts = []
    imports = listify(imports)
    if not imports:
        return []
    for imp in imports:
        if not isinstance(imp, str):
            raise ValueError('All imports must be strings')
        if imp.startswith('try:'):
            prot_impts.append(imp.rstrip())
        elif imp.startswith('import') or imp.startswith('from'):
            out_impts.append(imp.rstrip())
        elif imp.startswith('sys.path.append')\
            or imp.startswith('sys.path.insert'):
            path_impts.append(imp.rstrip())
        else:
            if imp.startswith('@'):
                continue
            out_impts.append('import {}'.format(imp))

    if prot:
        for imp in out_impts:
            prot_impts.append(PROT_IMPT.format(imp))
        out = prot_impts
    else:
        out = out_impts + prot_impts

    # Remove duplicates
    out = list(set(out))

    # Add PATHS
    if path_impts:
        out = list(set(path_impts)) + out

    return out


def get_function_path(function):
    """Return path to module defining a function if it exists."""
    mod = _inspect.getmodule(function)
    if mod and mod != '__main__':
        return _os.path.dirname(_inspect.getabsfile(function))
    else:
        return None


def update_syspaths(function, kwds=None):
    """Add function path to 'syspaths' in kwds."""
    if kwds:
        syspaths = listify(kwds['syspaths']) if 'syspaths' in kwds else []
    else:
        syspaths = []
    return [get_function_path(function)] + syspaths


def import_function(function, mode='string'):
    """Return an import string for the function.

    Attempts to resolve the parent module also, if the parent module is a file,
    ie it isn't __main__, the import string will include a call to
    sys.path.append to ensure the module is importable.

    If this function isn't defined by a module, returns an empty string.

    Args:
        mode (str): string/list, return as a unified string or a list.
    """
    if not callable(function):
        raise ValueError('Function must be callable, {} is not'
                         .format(function))
    if mode not in ['string', 'list']:
        raise ValueError("Invalid mode {}, must be 'list' or 'string'"
                         .format(mode))

    if _inspect.ismethod(function):
        name = (dict(_inspect.getmembers(function.__self__))['__class__']
                .__name__)
    else:
        name    = function.__name__

    # Attempt to resolve defining file
    parent = _inspect.getmodule(function)

    imports = []
    if parent and parent.__name__ != '__main__':
        path   = _os.path.dirname(parent.__file__)
        module = parent.__name__
        # If module is the child of a package, change the directory up to the
        # parent
        if '.' in module:
            path = _os.path.abspath(
                _os.path.join(
                    path, *['..' for i in range(module.count('.'))]
                )
            )
        imports.append("sys.path.append('{}')".format(path))
        imports.append('import {}'.format(module))
        imports.append('from {} import *'.format(module))
        imports.append('from {} import {}'.format(module, name))

    return imports if mode == 'list' else '\n'.join(imports)


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

    # For interactive sessions
    members = dict(_inspect.getmembers(function))
    locations = [members]
    if '__globals__' in members:
        locations.append(members['__globals__'])
    for location in locations:
        for name, item in location.items():
            if name.startswith('__'):
                continue
            # Modules
            if _inspect.ismodule(item):
                imports.append((name, item.__name__))
            # Functions
            elif callable(item):
                try:
                    func_imports.append((name, item.__name__, item.__module__))
                except AttributeError:
                    pass

    # Import all modules in the root module
    imports += [(k,v.__name__)
                for k,v in _inspect.getmembers(rootmod, _inspect.ismodule)
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
        if names[0] == '__main__':
            continue
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
        if mod == '__main__':
            continue
        if iname == name:
            import_strings.append('from {} import {}'.format(mod, name))
        else:
            import_strings.append('from {} import {} as {}'
                                  .format(mod, name, iname)
                                 )

    if mode == 'string':
        return import_strings

    elif mode == 'prot':
        return normalize_imports(import_strings, prot=True)

    else:
        raise ValueError('Mode changed unexpectedly')


def export_globals(function):
    """Add a function's globals to the current globals."""
    rootmod = _inspect.getmodule(function)
    globals()[rootmod.__name__] = rootmod
    for k, v in _inspect.getmembers(rootmod, _inspect.ismodule):
        if not k.startswith('__'):
            globals()[k] = v


def get_all_imports(function, kwds, prot=False):
    """Get all imports from a function and from kwds.

    Args:
        function (callable): A function handle
        kwds (dict):         A dictionary of keyword arguments
        prot (bool):         Wrap all import in try statement

    Returns:
        list: Imports
    """
    imports  = listify(kwds['imports'] if 'imports' in kwds else None)
    imports  = normalize_imports(imports, prot=False)
    imports += get_imports(function, mode='string')
    return normalize_imports(imports, prot=prot)


def export_imports(function, kwds):
    """Get imports from a function and from kwds.

    Also sets globals and adds path to module to sys path.

    Args:
        function (callable): A function handle
        kwds (dict):         A dictionary of keyword arguments

    Returns:
        list: imports + sys.path.append for module path
    """
    export_globals(function)
    return import_function(function, 'list') + get_all_imports(function, kwds)


def _sort_imports(x):
    """Sort a list of tuples and strings, for use with sorted."""
    if isinstance(x, tuple):
        if x[1] == '__main__':
            return 0
        return x[1]
    return x
