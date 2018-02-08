"""
Tests submitting pandas functions.

Pandas is hard to install, so this isn't part of the travis py.test.
"""
import sys
import argparse
from uuid import uuid4
import fyrd
import pytest
try:
    import numpy as np
    import pandas as pd
    canrun = True
except ImportError:
    canrun = False
env = fyrd.get_cluster_environment()


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


@pytest.mark.skipif(canrun is not True,
                    reason="Need pandas and numpy installed")
def test_submission(delete=True):
    """Test getting a dataframe and getting the mean."""
    job = fyrd.Job(make_df).submit()
    df = job.get(cleanup=delete, delete_outfiles=delete)
    assert isinstance(df, pd.DataFrame)


@pytest.mark.skipif(canrun is not True,
                    reason="Need pandas and numpy installed")
def test_mean(delete=True):
    """Test getting the mean of the dataframe."""
    df = make_df()
    cmean = df.mean()
    job = fyrd.Job(get_mean, (df,)).submit()
    mean = job.get(cleanup=delete, delete_outfiles=delete)
    assert cmean.equals(mean)


@pytest.mark.skipif(canrun is not True,
                    reason="Need pandas and numpy installed")
def test_concat(delete=True):
    """Test concatenating two dataframes."""
    df1 = make_df()
    df2 = pd.DataFrame([[1, 2, 3, 4, 'hi', 'there']], columns=df1.columns)
    job = fyrd.Job(merge_two, (df1, df2)).submit()
    df = job.get(cleanup=delete, delete_outfiles=delete)
    assert len(df) == 101


@pytest.mark.skipif(env == 'local',
                    reason="Occasionally hangs in local mode")
@pytest.mark.skipif(canrun is not True,
                    reason="Need pandas and numpy installed")
def test_parapply(delete=True):
    """Test running a string merge operation with two cores."""
    df = make_df()
    df_comp = df.copy()
    df_comp['joined'] = df.apply(join_columns, axis=1)
    new_df = df.copy()
    new_df['joined'] = fyrd.helpers.parapply(2, df, join_columns, axis=1,
                                             clean_files=delete,
                                             clean_outputs=delete)
    assert new_df.equals(df_comp)


@pytest.mark.skipif(env == 'local',
                    reason="Occasionally hangs in local mode")
@pytest.mark.skipif(canrun is not True,
                    reason="Need pandas and numpy installed")
def test_parapply_summary(delete=True):
    """Test getting the mean with two cores."""
    df = make_df()
    df_comp = df[['A', 'B']].apply(get_mean)
    new_df = fyrd.helpers.parapply_summary(
        2, df[['A', 'B']], get_mean, clean_files=delete, clean_outputs=delete)
    new_df.T.A
    new_df.T.B
    df_comp.T.A
    df_comp.T.B
    try:
        assert str(round(new_df.T.A, 2)) == str(round(df_comp.T.A, 2))
    except AssertionError:
        print('A:', type(new_df.T.A), ':', str(new_df.T.A),
              type(df_comp.T.A), ':', str(df_comp.T.A))
        print('Job df', new_df)
        print('Comp df', new_df)
        raise
    try:
        assert str(round(new_df.T.B, 2)) == str(round(df_comp.T.B, 2))
    except AssertionError:
        print('B:', type(new_df.T.B), ':', str(new_df.T.B),
              type(df_comp.T.B), ':', str(df_comp.T.B))
        print('Job df', new_df)
        print('Comp df', new_df)
        raise


@pytest.mark.skipif(env == 'local',
                    reason="Fails in local mode")
@pytest.mark.skipif(canrun is not True,
                    reason="Need pandas and numpy installed")
def test_parapply_indirect(delete=True):
    """Test running a string merge operation with two cores."""
    df = make_df()
    df_comp = df.copy()
    df_comp['joined'] = df.apply(join_columns, axis=1)
    new_df = df.copy()
    job = fyrd.helpers.parapply(2, df, join_columns, axis=1,
                                clean_files=delete,
                                clean_outputs=delete, direct=False).submit()
    new_df['joined'] = job.get()
    assert new_df.equals(df_comp)


@pytest.mark.skipif(env == 'local',
                    reason="Fails in local mode")
@pytest.mark.skipif(canrun is not True,
                    reason="Need pandas and numpy installed")
def test_parapply_summary_indirect(delete=True):
    """Test getting the mean with two cores."""
    df = make_df()
    df_comp = df[['A', 'B']].apply(get_mean)
    job = fyrd.helpers.parapply_summary(
        2, df[['A', 'B']], get_mean, clean_files=delete, clean_outputs=delete,
        direct=False
    ).submit()
    new_df = job.get()
    new_df.T.A
    new_df.T.B
    df_comp.T.A
    df_comp.T.B
    try:
        assert str(round(new_df.T.A, 2)) == str(round(df_comp.T.A, 2))
    except AssertionError:
        print('A:', type(new_df.T.A), ':', new_df.T.A,
              type(df_comp.T.A), ':', df_comp.T.A,)
        print('Job df:\n', new_df)
        print('Comp df:\n', new_df)
        raise
    try:
        assert str(round(new_df.T.B, 2)) == str(round(df_comp.T.B, 2))
    except AssertionError:
        print('B:', type(new_df.T.B), ':', str(new_df.T.B),
              type(df_comp.T.B), ':', str(df_comp.T.B))
        print('Job df:\n', new_df)
        print('Comp df:\n', new_df)
        raise


@pytest.mark.skip()
def main(argv=None):
    """Get arguments and run tests."""
    sys.stderr.write('HERE!!!\n')
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

    test_mean(args.keep_files)
    test_concat(args.keep_files)
    test_parapply(args.keep_files)
    test_parapply_summary(args.keep_files)
    test_parapply_indirect(args.keep_files)
    test_parapply_summary_indirect(args.keep_files)

if __name__ == '__main__' and '__file__' in globals():
    sys.exit(main())
else:
    fyrd.logme.MIN_LEVEL = 'debug'
