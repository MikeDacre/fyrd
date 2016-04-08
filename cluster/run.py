"""
File management and execution functions.

============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2016-02-11 16:03
 Last modified: 2016-03-30 21:30

============================================================================
"""
import os
import gzip
import bz2
import argparse
from subprocess import Popen
from subprocess import PIPE

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

    Returns text mode file handle.

    If infile is a file handle or text device, it is returned without
    changes.
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


def cmd(command, args=None, stdout=None, stderr=None):
    """Run command and return status, output, stderr.

    :command: Path to executable.
    :args:    Tuple of arguments.
    :stdout:  File or open file like object to write STDOUT to.
    :stderr:  File or open file like object to write STDERR to.
    """
    if args:
        if isinstance(args, list):
            args = tuple(list)
        if not isinstance(args, tuple):
            raise CommandError('args must be tuple')
        args = (command,) + args
    else:
        args = (command,)
    logme.log('Running {} as {}'.format(command, args), 'debug')
    pp = Popen(args, shell=True, universal_newlines=True,
               stdout=PIPE, stderr=PIPE)
    out, err = pp.communicate()
    code = pp.returncode
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

    :program: Name of executable to test.
    :returns: Path to the program or None on failure.
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

    :infile:  Any file name
    :types:   String or list/tuple of strings (e.g ['bed', 'gtf'])
    :returns: True or False

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


def split_file(infile, parts, outpath='', keep_header=True):
    """Split a file in parts parts and return a list of paths.

    NOTE: Linux specific (uses wc).

    :outpath:     The directory to save the split files.
    :keep_header: Add the header line to the top of every file.

    """
    # Determine how many reads will be in each split sam file.
    logme.log('Getting line count', 'debug')
    num_lines = int(os.popen(
        'wc -l ' + infile + ' | awk \'{print $1}\'').read())
    num_lines   = int(int(num_lines)/int(parts)) + 1

    # Subset the file into X number of jobs, maintain extension
    cnt       = 0
    currjob   = 1
    suffix    = '.split_' + str(currjob).zfill(4) + '.' + infile.split('.')[-1]
    file_name = os.path.basename(infile)
    run_file  = os.path.join(outpath, file_name + suffix)
    outfiles  = [run_file]

    # Actually split the file
    logme.log('Splitting file', 'debug')
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
                run_file = os.path.join(outpath, file_name + suffix)
                sfile = open(run_file, 'w')
                outfiles.append(run_file)
                sfile.write(header)
                cnt = 0
        sfile.close()
    return tuple(outfiles)
