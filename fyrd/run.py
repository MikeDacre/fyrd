# -*- coding: utf-8 -*-
"""
A library of useful functions used throughout the *fyrd* package.

These include functions to handle data, format outputs, handle file opening,
run commands, check file extensions, get user input, and search and format
imports.

These functions are not intended to be accessed directly and so documentation
is limited.
"""
from __future__ import with_statement
import os as _os
import re as _re
import sys as _sys
import inspect as _inspect
import argparse as _argparse
from collections import OrderedDict as _OD

import bz2
import gzip
from subprocess import Popen
from subprocess import PIPE
from time import sleep
from glob import glob as _glob

from six.moves import input as _get_input

# Progress bar handling
from tqdm import tqdm, tqdm_notebook
try:
    if str(type(get_ipython())) == "<class 'ipykernel.zmqshell.ZMQInteractiveShell'>":
        _pb = tqdm_notebook
    else:
        _pb = tqdm
except NameError:
    _pb = tqdm

from . import logme as _logme

STRPRSR = _re.compile(r'{(.*?)}')


def get_pbar(iterable, name=None, unit=None, **kwargs):
    """Return a tqdm progress bar iterable.

    If progressbar is set to False in the config, will not be shown.
    """
    from . import conf  # Avoid reciprocal import issues
    show_pb = bool(conf.get_option('queue', 'progressbar', True))
    if 'desc' in kwargs:
        dname = kwargs.pop('desc')
        name = name if name else dname
    if 'disable' in kwargs:
        disable = kwargs['disable']
    else:
        disable = False if show_pb else True
    return _pb(iterable, desc=name, unit=unit, disable=disable, **kwargs)


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


def string_getter(string):
    """Parse a string for `{}`, `{#}`, and `{string}`.

    Parameters
    ----------
    string : str

    Returns
    -------
    ints : set
        A set of ints containing all `{#}` values
    vrs : set
        A set of `{string}` values

    Raises
    ------
    ValueError
        If both `{}` and `{#}` are passed
    """
    locs = STRPRSR.findall(string)
    ints = {int(i) for i in locs if i.isdigit()}
    strs = {i for i in locs if not i.isdigit()}
    if ints and '{}' in string:
        raise ValueError('Cannot parse string with both numbered and '
                         'unnumbered braces')
    return ints, strs


def parse_glob(string, get_vars=None):
    """Return a list of files that match a simple regex glob.

    Parameters
    ----------
    string : str
    get_vars : list
        A list of variable names to search for. The string must contain these
        variables in the form `{variable}`. These variables will be temporarily
        replaced with a `*` and then run through `glob.glob` to generate a list
        of files. This list is then parsed to create the output.

    Returns
    -------
    dict
        Keys are all files that match the string, values are None if `get_vars`
        is not passed. If `get_vars` is passed, the values are dictionaries
        of `{'variable': 'result'}`. e.g. for `{name}.txt` and `hi.txt`::

            {hi.txt: {name: 'hi'}}

    Raises
    ------
    ValueError
        If blank or numeric variable names are used or if get_vars returns
        multiple different names for a file.
    """
    get_vars = listify(get_vars)
    test_string = STRPRSR.sub('*', string)
    files = _glob(test_string)
    if not get_vars:
        return _OD([(f, None) for f in files])
    results = _OD([(f, {}) for f in files])

    int_vars, str_vars = string_getter(string)
    if '{}' in string or int_vars:
        raise ValueError('Cannot have numeric placeholders in file strings ',
                         "i.e. no '{0}', '{1}', '{}', etc")
    for var in get_vars:
        if var not in str_vars:
            _logme.log('Variable {0} not in search string: {1}'
                       .format(var, string), 'warn')
            continue
        # Turn search string into a regular expression
        test_var = var if var.startswith('{') else '{' + var + '}'
        test_string = STRPRSR.sub('.*?', string.replace(test_var, '(.*?)'))
        test_string = _re.sub(r'([^.])\*', r'\1.*', test_string)
        test_string = _re.sub(r'^\*', r'.*', test_string)
        # Replace terminal non-greedy operators so we parse the whole string
        if test_string.endswith('?'):
            test_string = test_string[:-1]
        if test_string.endswith('?)'):
            test_string = test_string[:-2] + ')'
        test_regex = _re.compile(test_string)
        # Add to file dict
        for fl in files:
            vrs = test_regex.findall(fl)
            ulen = len(set(vrs))
            if ulen != 1:
                _logme.log('File {0} has multiple values for {1}: {2}'
                           .format(fl, test_var, vrs), 'critical')
                raise ValueError('Invalid file search string')
            if ulen == 0:
                _logme.log('File {0} has no results for {1}'
                           .format(fl, test_var), 'error')
                continue
            results[fl][var] = vrs[0]
    return results


def file_getter(file_strings, variables, extra_vars=None, max_count=None):
    """Get a list of files and variable values using the search string.

    The file strings can contain standard unix glob (like `*`) and variable
    containing strings in the form `{name}`.

    For example, a file_string of `{dir}/*.txt` will match every file that
    ends in `.txt` in every directory relative to the current path.

    The result for a directory name test with two files named 1.txt and 2.txt
    is a list of::

        [(('dir/1.txt'), {'dir': 'test'}),
         (('dir/2.txt'), {'dir': 'test'})]

    This is repeated for every file_string in file_strings, and the following
    tests are done:

        1. All file_strings must result in identical numbers of files
        2. All variables must have only a single value in every file string

    If there are multiple file_strings, they are added to the result x in
    order, but the dictionary remains the same as variables must be shared. If
    multiple file_strings are provided the results are combined by alphabetical
    order.

    Parameters
    ----------
    file_strings : list of str
        List of search strings, e.g. `*/*`, `*/*.txt`, `{dir}/*.txt` or
        `{dir}/{file}.txt`
    variables : list of str
        List of variables to look for
    extra_vars : list of str, optional
        A list of additional variables specified in a very precise format::

            new_var:orig_var:regex:sub_str

            or

            new_var:value

        The orig_var must correspond to a variable in variables. var will be
        generated by running re.sub(regex, sub_str, string) where string is
        the result of orig_var for the given file set
    max_count : int, optional
        Max number of file_strings to parse, default is all.

    Returns
    -------
    list
        A list of files. Each list item will be a two-item tuple of
        `(files, variables)`. Files will be a tuple with the same length as
        max_count, or file_strings if max_count is None. Variables will be
        a dictionary of all variables and extra_vars for this file set. e.g.::

            [((file1, dir1, file2), {var1: val, var2: val})]

    Raises
    ------
    ValueError
        Raised if any of the above tests are not met.
    """
    # Make extra_var an empty list if None
    extra_vars = listify(extra_vars) if extra_vars else []

    # Get all file information
    files = []
    count = 0
    for file_string in file_strings:
        files.append(parse_glob(file_string, variables))
        count += 1
        if max_count and count == max_count:
            break

    # Make sure all files have values for variables
    var_vals = {i: [] for i in variables}
    empty = {}
    for f in files:
        for pvars in f.values():
            # It's fine if the file string has no variables
            if not pvars:
                continue
            for var, val in pvars.items():
                if not val:
                    if var in empty:
                        empty[var].append(f)
                    else:
                        empty[var] = [f]
                else:
                    var_vals[var].append(val)
    fail = False
    if empty:
        _logme.log('The following variables had no '
                   'result in some files, cannot continue:\n'
                   '\n'.join(
                       ['{0} files: {1}'.format(i, j) for i, j in empty.items()]
                   ), 'critical')
        fail = True

    # Join files
    bad = []

    results = []
    for file_info in zip(*[fl.items() for fl in files]):
        good = True
        # Make sure dicts are compatible and make combined dict
        final_dict = {}
        final_files = tuple([_os.path.abspath(fl[0]) for fl in file_info])
        bad_dcts = []
        for dct in [f[1] for f in file_info]:
            if not dct:
                continue
            final_dict.update(dct)
            bad_dcts.append(dct)
            for var, val in dct.items():
                for fl in file_info:
                    if var in fl[1] and val != fl[1][var]:
                        good = False
        if not good:
            bad.append((final_files, bad_dcts))
            break
        for extra_var in extra_vars:
            try:
                evari = extra_var.split(':')
                if len(evari) == 2:
                    final_dict[evari[1]] = evari[2]
                    continue
                var, orig_var, parse_str, sub_str = evari
            except ValueError:
                _logme.log(
                    '{} is malformatted should be: '.format(extra_var) +
                    'either new_var:orig_var:regex:sub '
                    'or variable:value', 'critical'
                )
                raise
            if orig_var not in final_dict:
                raise ValueError(
                    'Extra variable {0} sets {1} as '.format(var, orig_var) +
                    'the original variable, but it is not in the dict for '
                    '{0}'.format(final_files)
                )
            final_dict[var] = _re.sub(parse_str, sub_str, final_dict[orig_var])
        results.append((final_files, final_dict))

    # Take care of bad items
    if empty:
        _logme.log('The following file combinations had mismatched variables, '
                   'cannot continue:\n'
                   '\n'.join(
                       ['{0} dicts: {1}'.format(i, j) for i, j in bad]
                   ), 'critical')
        fail = True

    if fail:
        raise ValueError('File parsing failure')

    return results


def listify(iterable):
    """Try to force any iterable into a list sensibly."""
    if isinstance(iterable, list):
        return iterable
    if isinstance(iterable, (str, int, float)):
        return [iterable]
    if not iterable:
        return []
    #  if callable(iterable):
        #  iterable = iterable()
    try:
        iterable = list(iterable)
    except TypeError:
        iterable = [iterable]
    return iterable


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

    Parameters
    ----------
    string : str
        Any string.
    prefix : str
        What to indent with.

    Returns
    -------
    str
        Indented string
    """
    out = ''
    for i in string.split('\n'):
        out += '{}{}\n'.format(prefix, i)
    return out


def is_exc(x):
    """Check if x is the output of sys.exc_info().

    Returns
    -------
    bool
        True if matched the output of sys.exc_info().
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

    Returns
    -------
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


def cmd_or_file(string):
    """If string is a file, return the contents, else return the string.

    Parameters
    ----------
    string : str
        Path to a file or any other string

    Returns
    -------
    script : str
        Either the contents of the file if string is a file or just the
        contents of string.
    """
    if _os.path.isfile(string):
        with open_zipped(string) as fin:
            command = fin.read().strip()
    else:
        command = string.strip()
    return command


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

    .. note:: Linux specific (uses wc).

    If has_header is True, the top line is stripped off the infile prior to
    splitting and assumed to be the header.

    Parameters
    ----------
    outpath : str, optional
        The directory to save the split files.
    keep_header : bool, optional
        Add the header line to the top of every file.

    Returns
    -------
    list
        Paths to split files.
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

    Parameters
    ----------
    infile : str
        Any file name
    types : list
        String or list/tuple of strings (e.g `['bed', 'gtf']`)

    Returns
    -------
    is_file_type : bool
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

    Parameters
    ----------
    command : str
        Path to executable.
    args : tuple, optional
        Tuple of arguments.
    stdout : str, optional
        File or open file like object to write STDOUT to.
    stderr : str, optional
        File or open file like object to write STDERR to.
    tries : int, optional
        Number of times to try to execute. 1+

    Returns
    -------
    exit_code : int
    STDOUT : str
    STDERR : str
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
    return function(*args, **kwargs)


def which(program):
    """Replicate the UNIX which command.

    Taken verbatim from:
        stackoverflow.com/questions/377017/test-if-executable-exists-in-python

    Parameters
    ----------
    program : str
        Name of executable to test.

    Returns
    -------
    str or None
        Path to the program or None on failure.
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

    Parameters
    ----------
    args : list/tuple/dict
        Tuple or dict of args
    find_string : str
        A string to search for
    replace_string : str
        A string to replace with
    error : bool
        Raise ValueError if replacement fails

    Returns
    -------
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

    Parameters
    ----------
    opt : list
        A list of strings, can be a single string.
    split_on : list
        A list of characters to use to split the options.

    Returns
    -------
    list
        A single merged list of split options, uniqueness guaranteed, order
        not.
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

    Parameters
    ----------
    message : str
        A message to print, an additional space will be added.
    default : {'y', 'n'}, optional
        One of `{'y', 'n'}`, the default if the user gives no answer. If None,
        answer forced.

    Returns
    -------
    bool
        True on yes, False on no
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

    Parameters
    ----------
    message : str
        A message to print, an additional space will be added.
    valid_answers : list
        A list of answers to accept, if None, ignored.  Case insensitive. There
        is one special option here: 'yesno', this allows all case insensitive
        variations of y/n/yes/no.
    default : str
        The default answer.

    Returns
    -------
    response : str
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

    Parameters
    ----------
    imports : list
        A list of strings, formatted differently.
    prot : bool
        Protect imports with try..except blocks

    Returns
    -------
    list
        A list of strings that can be used for imports
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

    Parameters
    ----------
    mode : {'string', 'list'}, optional
        string/list, return as a unified string or a list.
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

    string:
        Return a list of strings formatted as unprotected import calls
    prot:
        Similar to string, but with try..except blocks
    list:
        Return two lists: (import name, module name) for modules and (import
        name, function name, module name) for functions

    Parameters
    ----------
    function : callable
        A function handle
    mode : str
        A string corresponding to one of the above modes

    Returns
    -------
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

    Parameters
    ----------
    function : callable
        A function handle
    kwds : dict
        A dictionary of keyword arguments
    prot : bool
        Wrap all import in try statement

    Returns
    -------
    list
        Imports
    """
    imports  = listify(kwds['imports'] if 'imports' in kwds else None)
    imports  = normalize_imports(imports, prot=False)
    imports += get_imports(function, mode='string')
    return normalize_imports(imports, prot=prot)


def export_imports(function, kwds):
    """Get imports from a function and from kwds.

    Also sets globals and adds path to module to sys path.

    Parameters
    ----------
    function : callable
        A function handle
    kwds : dict
        A dictionary of keyword arguments

    Returns
    -------
    list
        imports + sys.path.append for module path
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
