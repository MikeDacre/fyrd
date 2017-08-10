# -*- coding: utf-8 -*-
"""
High level functions to make complex tasks easier.

Functions
---------
parapply
    Run a function on a pandas DataFrame in parralel on a cluster
parapply_summary
    Run parapply and merge the results (don't concatenate).
splitrun
    Split a file, run a command in parallel, return result.
"""
import os as _os

###############################################################################
#                               Import Ourself                                #
###############################################################################

from . import run as _run
from . import conf as _conf
from . import logme as _logme
from . import batch_systems as _batch
from .job import Job as _Job

_options = _batch.options

###############################################################################
#                       Try Import Non-Required Modules                       #
###############################################################################

try:
    import numpy as _np
    import pandas as _pd
except ImportError:
    _logme.log('Could not import numpy and pandas for helpers', 'debug')

__all__ = ['parapply', 'parapply_summary', 'splitrun']

###############################################################################
#                                  parapply                                   #
###############################################################################


def parapply(jobs, df, func, args=(), profile=None, applymap=False,
             merge_axis=0, merge_apply=False, name='parapply', imports=None,
             direct=True, **kwds):
    """Split a dataframe, run apply in parallel, return result.

    This function will split a dataframe into however many pieces are requested
    with the jobs argument, run apply in parallel by submitting the jobs to the
    cluster, and then recombine the outputs.

    If the 'clean_files' and 'clean_outputs' arguments are not passed, we
    delete all intermediate files and output files by default.

    This function will take any keyword arguments accepted by Job, which can
    be found by running fyrd.options.option_help(). It also accepts any of
    the keywords accepted by by pandas.DataFrame.apply(), found
    `here <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.apply.html>`_

    Parapply
    --------
    jobs : int
        Number of pieces to split the dataframe into
    df : DataFrame
        Any pandas DataFrame
    args : tuple
        Positional arguments to pass to the function, keyword arguments can
        just be passed directly.
    profile : str
        A fyrd cluster profile to use
    applymap : bool
        Run applymap() instead of apply()
    merge_axis : int
        Which axis to merge on, 0 or 1, default is 1 as apply transposes
        columns
    merge_apply : bool
        Apply the function on the merged dataframe also
    name : str
        A prefix name for all of the jobs
    imports : list
        A list of imports in any format, e.g.
        `['import numpy', 'scipy', 'from numpy import mean']`
    direct : bool
        Whether to run the function directly or to return a Job. Default True.

    Any keyword arguments recognized by fyrd will be used for job
    submission.

    *Additional keyword arguments will be passed to DataFrame.apply()*

    Returns
    -------
    DataFrame
        A recombined DataFrame: concatenated version of original split
        DataFrame

    Example
    -------
    >>> import numpy
    >>> import pandas
    >>> import fyrd
    >>> df = pandas.DataFrame([[0, 1], [2, 6], [9, 24], [13, 76], [4, 12]])
    >>> df['sum'] = fyrd.helpers.parapply(2, df, lambda x: x[0]+x[1], axis=1)
    >>> df
        0   1  sum
        0   0   1    1
        1   2   6    8
        2   9  24   33
        3  13  76   89
        4   4  12   16

    See Also
    --------
    parapply_summary: Merge results of parapply using applied function
    splitrun: Run a command in parallel on a split file
    """
    if direct:
        return _parapply(
            jobs, df, func, args=args, profile=profile, applymap=applymap,
            merge_axis=merge_axis, merge_apply=merge_apply, name=name,
            imports=imports, **kwds
        )
    kwargs = dict(
        args=args, profile=profile, applymap=applymap,
        merge_axis=merge_axis, merge_apply=merge_apply, name=name,
        imports=imports
    )
    kwds = _options.sanitize_arguments(kwds)
    kwargs.update(kwds)
    kwargs['imports']  = _run.get_all_imports(func, kwargs)
    kwargs['syspaths'] = _run.update_syspaths(func, kwargs)
    return _wrap_runner(
        _parapply,
        *(jobs, df, func),
        **kwargs
    )


def _parapply(jobs, df, func, args=(), profile=None, applymap=False,
              merge_axis=0, merge_apply=False, name='parapply', imports=None,
              **kwds):
    """Direct running function for parapply, see parapply docstring."""
    # Handle arguments
    if not isinstance(jobs, int):
        raise ValueError('Jobs argument must be an integer.')
    if not isinstance(df, (_pd.core.frame.DataFrame, _np.ndarray)):
        raise ValueError('df must be a dataframe or numpy array, is {}'
                         .format(type(df)))
    if not callable(func):
        raise ValueError('function must be callable, current type is {}'
                         .format(type(func)))
    if profile is not None and not isinstance(profile, str):
        raise ValueError('Profile must be a string, is {}'
                         .format(type(profile)))
    fyrd_kwds, pandas_kwds = _options.split_keywords(kwds)

    # Get name
    name = name if name else 'split_file'

    # Set up auto-cleaning
    if 'clean_files' not in fyrd_kwds:
        fyrd_kwds['clean_files'] = True
    if 'clean_outputs' not in fyrd_kwds:
        fyrd_kwds['clean_outputs'] = True

    # Split dataframe
    _logme.log('Splitting dataframe', 'debug')
    dfs = _np.array_split(df, jobs)
    assert len(dfs) == jobs

    # Pick function
    sub_func = _run_applymap if applymap else _run_apply

    # Some sane imports
    imports  = _run.listify(imports) if imports else []
    imports += ['import numpy as np', 'import numpy', 'import scipy as sp',
                'import sp', 'import pandas as pd', 'import pandas',
                'from matplotlib import pyplot as plt',
                'from scipy import stats']
    imports = _run.export_imports(func, {'imports': imports})

    # Run the functions
    _logme.log('Submitting jobs', 'debug')
    outs = []
    count = 1
    for d in dfs:
        nm = '{}_{}_of_{}'.format(name, count, jobs)
        outs.append(
            _Job(sub_func, (d, func, args, pandas_kwds),
                 profile=profile, name=nm, imports=imports,
                 **fyrd_kwds).submit()
        )
        count += 1

    # Get the results
    _logme.log('Waiting for results', 'debug')
    results = []
    for out in outs:
        try:
            results.append(out.get())
        except IOError:
            _logme.log('Result getting failed, most likely one of the child ' +
                       'jobs crashed, check the error files', 'critical')
            raise

    # Return the recombined DataFrame
    _logme.log('Done, joinging', 'debug')
    try:
        out = _pd.concat(results, axis=merge_axis)
    except ValueError:
        _logme.log('DataFrame concatenation failed!', 'critical')
        raise

    if merge_apply:
        out = sub_func(out, func, args, pandas_kwds)

    return out


def parapply_summary(jobs, df, func, args=(), profile=None, applymap=False,
                     name='parapply', imports=None, direct=True, **kwds):
    """Run parapply for a function with summary stats.

    Instead of returning the concatenated result, merge the result using
    the same function as was used during apply.

    This works best for summary functions like `.mean()`, which do a linear
    operation on a whole dataframe or series.

    Parameters
    ----------
    jobs : int
        Number of pieces to split the dataframe into
    df : DataFrame
        Any pandas DataFrame
    args : tuple
        Positional arguments to pass to the function, keyword arguments can
        just be passed directly.
    profile : str
        A fyrd cluster profile to use
    applymap : bool
        Run applymap() instead of apply()
    merge_axis : int
        Which axis to merge on, 0 or 1, default is 1 as apply transposes
        columns
    merge_apply : bool
        Apply the function on the merged dataframe also
    name : str
        A prefix name for all of the jobs
    imports : list
        A list of imports in any format, e.g.
        `['import numpy', 'scipy', 'from numpy import mean']`
    direct : bool
        Whether to run the function directly or to return a Job. Default True.

    Any keyword arguments recognized by fyrd will be used for job
    submission.

    *Additional keyword arguments will be passed to DataFrame.apply()*

    Returns
    -------
    DataFrame
        A recombined DataFrame

    Example
    -------
    >>> import numpy
    >>> import pandas
    >>> import fyrd
    >>> df = pandas.DataFrame([[0, 1], [2, 6], [9, 24], [13, 76], [4, 12]])
    >>> df = fyrd.helpers.parapply_summary(2, df, numpy.mean)
    >>> df
    0     6.083333
    1    27.166667
    dtype: float64

    See Also
    --------
    parapply: Run a command in parallel on a DataFrame without merging the
    result
    """
    if direct:
        return _parapply_summary(
            jobs, df, func, args=args, profile=profile, applymap=applymap,
            name=name, imports=imports, **kwds
        )
    kwargs = dict(
        args=args, profile=profile, applymap=applymap,
        name=name, imports=imports
        )
    kwds = _options.sanitize_arguments(kwds)
    kwargs.update(kwds)
    kwargs['imports']  = _run.get_all_imports(func, kwargs)
    kwargs['syspaths'] = _run.update_syspaths(func, kwargs)
    return _wrap_runner(
        _parapply_summary,
        *(jobs, df, func),
        **kwargs
    )


def _parapply_summary(jobs, df, func, args=(), profile=None, applymap=False,
                      name='parapply', imports=None, **kwds):
    """Direct running function for parapply_sumary, see that docstring."""
    imports = _run.export_imports(func, {'imports': imports})
    out = parapply(jobs, df, func, args, profile, applymap, name=name,
                   merge_axis=1, imports=imports, **kwds)

    # Pick function and get args
    sub_func = _run_applymap if applymap else _run_apply
    pandas_kwds = _options.split_keywords(kwds)[1]

    # Transpose
    out = out.T

    # Run the function again
    return sub_func(out, func, args, pandas_kwds)


def _run_apply(df, func, args=None, pandas_kwds=None):
    """Run DataFrame.apply().

    Parameters
    ----------
    df : DataFrame
        Any pandas DataFrame
    args : tuple
        A tuple of arguments to submit to the function
    pandas_kwds : dict
        A dictionary of keyword arguments to pass to DataFrame.apply()

    Returns
    -------
    DataFrame
        The result of apply()
    """
    apply_kwds = {'args': args} if args else {}
    if pandas_kwds:
        apply_kwds.update(pandas_kwds)
    return df.apply(func, **apply_kwds)


def _run_applymap(df, func, args=None, pandas_kwds=None):
    """Run DataFrame.applymap().

    Parameters
    ----------
    df : DataFrame
        Any pandas DataFrame
    args : tuple
        A tuple of arguments to submit to the function
    pandas_kwds : dict
        A dictionary of keyword arguments to pass to DataFrame.apply()

    Returns
    -------
    DataFrame
        The result of apply()
    """
    apply_kwds = {'args': args} if args else {}
    if pandas_kwds:
        apply_kwds.update(pandas_kwds)
    return df.applymap(func, **apply_kwds)


###############################################################################
#                                  Split Run                                  #
###############################################################################


def splitrun(jobs, infile, inheader, command, args=None, kwargs=None,
             name=None, qtype=None, profile=None, outfile=None,
             outheader=False, merge_func=None, direct=True, **kwds):
    """Split a file, run command in parallel, return result.

    This function will split a file into however many pieces are requested
    with the jobs argument, and run command on each.

    Accepts exactly the same arguments as the Job class, with the exception of
    the first three and last four arguments, which are:

    - the number of jobs
    - the file to work on
    - whether the input file has a header
    - an optional output file
    - whether the output file has a header
    - an optional function to use to merge the resulting list, only used
      if there is no outfile.
    - whether to run directly or to return a Job. If direct is True, this
      function will just run and thus block until complete, if direct is
      False, the function will submit as a Job and return that Job.

    **Note**: If command is a string, `.format(file={file})` will be called on
    it, where file is each split file. If command is a function, the there must
    be an argument in either args or kwargs that contains `{file}`. It will be
    replaced with the *path to the file*, again by the format command.

    If outfile is specified, there must also be an '{outfile}' line in any
    script or an '{outfile}' argument in either args or kwargs. When this
    function completes, the file at outfile will contain the concatenated
    output files of all of the jobs.

    If the 'clean_files' and 'clean_outputs' arguments are not passed, we
    delete all intermediate files and output files by default.

    The intermediate files will be stored in the 'scriptpath' directory.

    Any header line is kept at the top of the file.

    Parameters
    ----------
    jobs : int
        Number of pieces to split the dataframe into
    infile : str
        The path to the file to be split.
    inheader : bool
        Does the input file have a header?
    command : function/str
        The command or function to execute.
    args : tuple/dict
        Optional arguments to add to command, particularly useful for
        functions.
    kwargs : dict
        Optional keyword arguments to pass to the command, only used for
        functions.
    name : str
        Optional name of the job. If not defined, guessed. If a job of the same
        name is already queued, an integer job number (not the queue number)
        will be added, ie.  <name>.1
    qtype : str
        Override the default queue type
    profile : str
        The name of a profile saved in the conf
    outfile : str
        The path to the expected output file.
    outheader : bool
        Does the input outfile have a header?
    merge_func : function
        An optional function used to merge the output list if there is no
        outfile.
    direct : bool
        Whether to run the function directly or to return a Job. Default True.

    *All other keywords are parsed into cluster keywords by the
    options system. For available keywords see `fyrd.option_help()`*

    Returns
    -------
    Primary return value varies and is decided in this order:

    If outfile:    the absolute path to that file
    If merge_func: the result of merge_func(list), where list
                    is the list of outputs.
    Else:          a list of results

    If direct is False, this function returns a fyrd.job.Job object which
    will return the results described above on get().
    """
    kwds = _options.check_arguments(kwds)
    if direct:
        return _splitrun(
            jobs, infile, inheader, command, args=args, kwargs=kwargs,
            name=name, qtype=qtype, profile=profile, outfile=outfile,
            outheader=outheader, merge_func=merge_func, **kwds
        )
    else:
        kk = dict(
            args=args, kwargs=kwargs, name=name, qtype=qtype, profile=profile,
            outfile=outfile, outheader=outheader, merge_func=merge_func
        )
        kwds = _options.sanitize_arguments(kwds)
        kk.update(_options.check_arguments(kwds))
        if callable(command):
            kk['imports']  = _run.get_all_imports(command, kk)
            kk['syspaths'] = _run.update_syspaths(command, kk)
        return _wrap_runner(
            _splitrun,
            *(jobs, infile, inheader, command),
            **kk
        )


def _splitrun(jobs, infile, inheader, command, args=None, kwargs=None,
              name=None, qtype=None, profile=None, outfile=None,
              outheader=False, merge_func=None, **kwds):
    """This is the direct running function for `splitrun()`.

    Please see that function's docstring for information and do not call this
    function directly.
    """
    # Handle arguments
    if not isinstance(jobs, int):
        raise ValueError('Jobs argument must be an integer.')
    if profile is not None and not isinstance(profile, str):
        raise ValueError('Profile must be a string, is {}'
                         .format(type(profile)))
    kwds = _options.check_arguments(kwds)

    # Get name
    name = name if name else 'split_file'

    # Check file
    infile = _os.path.abspath(_os.path.expandvars(_os.path.expanduser(infile)))
    if not _os.path.isfile(infile):
        raise OSError('Cannot find file {}'.format(infile))

    # Set up auto-cleaning
    if 'clean_files' not in kwds:
        kwds['clean_files'] = True
    if 'clean_outputs' not in kwds:
        kwds['clean_outputs'] = True

    # Get script path, returns: kwds, runpath, outpath, scriptpath
    cpath = _conf.get_job_paths(kwds)[3]

    # Prep function
    if callable(command):
        # Add all imports from the function file to globals
        kwds['imports'] = _run.export_imports(command, kwds)

    # Split file
    _logme.log('Splitting file', 'debug')
    files = _run.split_file(infile, jobs, outpath=cpath, keep_header=inheader)
    assert len(files) == jobs

    # Run the functions
    _logme.log('Submitting jobs', 'debug')
    outs = []
    count = 1
    for f in files:
        if outfile:
            o = f + '.out'
        nm = '{}_{}_of_{}'.format(name, count, jobs)
        if callable(command):
            try:
                runargs, runkwargs = _run.replace_argument(
                    [args, kwargs], '{file}', f)
            except ValueError:
                raise ValueError("No '{file}' argument in either args or " +
                                 "kwargs")
            if outfile:
                runargs, runkwargs = _run.replace_argument(
                    [runargs, runkwargs], '{outfile}', o)
            outs.append(
                _Job(command, args=runargs, kwargs=runkwargs, name=nm,
                     qtype=qtype, profile=profile, **kwds).submit()
            )
        else:
            if outfile:
                cmnd = command.format(file=f, outfile=o)
            else:
                cmnd = command.format(file=f)
            outs.append(
                _Job(cmnd, name=nm, qtype=qtype, profile=profile,
                     **kwds).submit()
            )
        count += 1

    # Get the results
    _logme.log('Waiting for results', 'debug')
    results = []
    for out in outs:
        try:
            results.append(out.get())
        except IOError:
            _logme.log('Result getting failed, most likely one of the child ' +
                       'jobs crashed, check the error files', 'critical')
            raise

    # Delete intermediate files
    _logme.log('Removing intermediate files', 'debug')
    for f in files:
        assert _os.path.isfile(f)
        _os.remove(f)

    # Return the recombined DataFrame
    _logme.log('Done, joining', 'debug')
    if outfile:
        with open(outfile, 'w') as fout:
            if outheader:
                with open(files[0]) as fin:
                    fout.write(fin.readline())
            for f in files:
                with open(f + '.out') as fin:
                    if outheader:
                        fin.readline()
                    fout.write(fin.read())
        out = _os.path.abspath(outfile)

    elif merge_func:
        assert callable(merge_func)
        out = merge_func(results)

    elif isinstance(results[0], list):
        out = _run.merge_lists(results)

    else:
        out = results

    return out


###############################################################################
#                              Helper Functions                               #
###############################################################################


def _wrap_runner(command, *args, **kkk):
    """Run a command as a job, for use with the functions in this file.

    *Note*: Only uses the 'profile', 'imports', 'syspaths', 'mem', 'clean_files',
    'clean_outputs', and 'partition' keyword arguments for job submission,
    all others are ignored.

    Returns
    -------
    fyrd.job.Job
        A Job class (not submitted) for the command.
    """
    mem = kkk['mem'] if 'mem' in kkk else 2000
    cln_f = kkk['clean_files'] if 'clean_files' in kkk else True
    cln_o = kkk['clean_outputs'] if 'clean_outputs' in kkk else True
    kkk['clean_files'] = cln_f
    kkk['clean_outputs'] = cln_o
    add_args = dict(
        cores=1, mem=mem, clean_files=cln_f, clean_outputs=cln_o
    )
    if 'partition' in kkk:
        add_args.update(dict(partition=kkk['partition']))
    if 'profile' in kkk:
        add_args.update(dict(profile=kkk['profile']))
    if callable(command):
        _logme.log('Running function {} with export run'
                   .format(command.__name__), 'debug')
        kkk['imports']  = _run.export_imports(command, kkk)
        spth = [_run.get_function_path(command)]
        kkk['syspaths'] = spth + _run.listify(kkk['syspaths']) \
            if 'syspaths' in kkk else spth
        add_args.update(dict(imports=kkk['imports'], syspaths=kkk['syspaths']))
        _logme.log('export_run imports: {}'.format(kkk['imports']), 'debug')
        _logme.log('export_run syspath: {}'.format(kkk['syspaths']), 'debug')
    else:
        _logme.log('Running command {} with export run'
                   .format(command), 'debug')
    return _Job(
        _run.export_run, (command, args, kkk), **add_args
    )
