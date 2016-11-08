"""
Tests submitting pandas functions.

Pandas is hard to install, so this isn't part of the travis py.test.
"""
import sys
import argparse
from uuid import uuid4
import fyrd
import numpy as np
import pandas as pd


###############################################################################
#                      DataFrame Manipulation Functions                       #
###############################################################################


def make_df():
    """Create an example df."""
    df = pd.DataFrame(np.random.randint(0,100,size=(100, 4)),
                      columns=list('ABCD'))
    df['s1'] = [str(uuid4()).split('-')[0] for i in range(100)]
    df['s2'] = [str(uuid4()).split('-')[0] for i in range(100)]
    return df


def get_mean(d):
    """Get a dataframe mean"""
    return d.mean()


def merge_two(d1, d2):
    """Merge two pandas dataframes."""
    return pd.concat([d1, d2])


def join_columns(d):
    """Merge three columns plus a sum."""
    return '{}.{}.{}'.format(d.s1, d.s2, d.A + d.B)


###############################################################################
#                               Test Functions                                #
###############################################################################


def test_submission(delete=True):
    """Test getting a dataframe and getting the mean."""
    job = fyrd.Job(make_df).submit()
    df = job.get(cleanup=delete, delete_outfiles=delete)
    assert isinstance(df, pd.DataFrame)


def test_mean(delete=True):
    """Test getting the mean of the dataframe."""
    df = make_df()
    cmean = df.mean()
    job = fyrd.Job(get_mean, (df,)).submit()
    mean = job.get(cleanup=delete, delete_outfiles=delete)
    assert mean == cmean


def test_concat(delete=True):
    """Test concatenating two dataframes."""
    df1 = make_df()
    df2 = pd.DataFrame([[1, 2, 3, 4, 'hi', 'there']], columns=df1.columns)
    job = fyrd.Job(merge_two, (df1, df2)).submit()
    df = job.get(cleanup=delete, delete_outfiles=delete)
    assert len(df) == 101


def test_split_apply(delete=True):
    """Test running a string merge operation with two cores."""
    df = make_df()
    df_comp = df.copy()
    df_comp['joined'] = df.apply(join_columns, axis=1)
    new_df = df.copy()
    new_df['joined'] = fyrd.helpers.parapply(2, df, join_columns, axis=1,
                                             clean_files=delete,
                                             clean_outputs=delete)
    assert new_df.equals(df_comp)


def test_split_apply_summary(delete=True):
    """Test getting the mean with two cores."""
    df = make_df()
    df_comp = df[['A', 'B']].apply(get_mean)
    new_df = fyrd.helpers.parapply_summary(
        2, df[['A', 'B']], get_mean, clean_files=delete, clean_outputs=delete)
    assert new_df.T.A == df_comp.T.A
    assert new_df.T.B == df_comp.T.B


def main(argv=None):
    """Get arguments and run tests."""
    if not argv:
        argv = sys.argv[1:]

    parser  = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-l', '--local', action="store_true",
                        help="Skip remote tests")
    parser.add_argument('-v', '--verbose', action="store_true",
                        help="Skip remote tests")
    parser.add_argument('-k', '--keep-files', action="store_false",
                        help="Keep intermediate files on run")

    args = parser.parse_args(argv)

    if args.local:
        fyrd.queue.MODE = 'local'
    if args.verbose:
        fyrd.logme.MIN_LEVEL = 'debug'

    #  test_mean(args.keep_files)
    #  test_concat(args.keep_files)
    test_split_apply(args.keep_files)

if __name__ == '__main__' and '__file__' in globals():
    sys.exit(main())
