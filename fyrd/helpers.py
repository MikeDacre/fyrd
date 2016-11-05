# -*- coding: utf-8 -*-
"""
High level functions to make complex tasks easier.

Last modified: 2016-11-05 11:06
"""
import numpy as _np
import pandas as _pd

###############################################################################
#                               Import Ourself                                #
###############################################################################

from . import logme   as _logme
from . import options as _options
from .job import Job as _Job


__all__ = ['parapply']

###############################################################################
#                                  Functions                                  #
###############################################################################


def parapply(jobs, df, func, args=(), profile=None, **kwds):
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

    Args:
        jobs (int):      Number of pieces to split the dataframe into
        df (DataFrame):  Any pandas DataFrame
        func (function): A function handle
        args (tuple):    Positional arguments to pass to the function, keyword
                         arguments can just be passed directly.
        profile (str):   A fyrd cluster profile to use

        Any keyword arguments recognized by fyrd will be used for job
        submission.

        *Additional keyword arguments will be passed to DataFrame.apply()*

    Returns:
        DataFrame: A recombined DataFrame
    """
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

    # Set up auto-cleaning
    if 'clean_files' not in fyrd_kwds:
        fyrd_kwds['clean_files'] = True
    if 'clean_outputs' not in fyrd_kwds:
        fyrd_kwds['clean_outputs'] = True

    # Split dataframe
    _logme.log('Splitting dataframe', 'debug')
    dfs = _np.array_split(df, jobs)
    assert len(dfs) == jobs

    # Run the functions
    _logme.log('Submitting jobs', 'debug')
    outs = []
    for d in dfs:
        outs.append(
            _Job(_run_apply, (d, func, args, pandas_kwds),
                 profile=profile, **fyrd_kwds)
        )

    # Get the results
    _logme.log('Waiting for results', 'debug')
    results = []
    for out in outs:
        results.append(out.get())

    # Return the recombined DataFrame
    _logme.log('Done, joinging', 'debug')
    try:
        return _pd.concat(results)
    except ValueError as e:
        _logme.log('Concatenating the results failed with the error {}\n'
                   .format(e) + 'Check your apply function', 'error')
        raise e


def _run_apply(df, func, args=None, pandas_kwds=None):
    """Run DataFrame.apply().

    Args:
        df (DataFrame):     Any pandas DataFrame
        args (tuple):       A tuple of arguments to submit to the function
        pandas_kwds (dict): A dictionary of keyword arguments to pass to
                            DataFrame.apply()

    Returns:
        DataFrame: The result of apply()
    """
    apply_kwds = {'args': args} if args else {}
    if pandas_kwds:
        apply_kwds.update(pandas_kwds)
    return df.apply(func, **apply_kwds)
