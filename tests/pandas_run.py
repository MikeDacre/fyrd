"""
Tests submitting pandas functions.

Pandas is hard to install, so this isn't part of the travis py.test.
"""
import sys
import argparse
import fyrd
import pandas as pd


def get_mean(d):
    """Get a dataframe mean"""
    return d.mean()


def merge_two(d1, d2):
    """Merge two pandas dataframes."""
    return pd.concat([d1, d2])


def make_df():
    """Create an example df."""
    return pd.DataFrame([['hi', 2, -47], ['there', 4, 26.4]])


def test_mean(delete=True):
    """Test getting a dataframe and getting the mean."""
    job = fyrd.Job(make_df).submit()
    df = job.get(cleanup=delete, delete_outfiles=delete)
    assert isinstance(df, pd.DataFrame)
    assert df[0][0] == 'hi'
    job2 = fyrd.Job(get_mean, (df,)).submit()
    mean = job2.get(cleanup=delete, delete_outfiles=delete)
    assert mean[1] == 3


def test_concat(delete=True):
    """Test concatenating two dataframes."""
    df1 = pd.DataFrame([['hi', 6, 42], ['there', 8, 24]])
    df2 = make_df()
    job = fyrd.Job(merge_two, (df1, df2)).submit()
    df = job.get(cleanup=delete, delete_outfiles=delete)
    assert len(df) == 4


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

    test_mean(args.keep_files)
    test_concat(args.keep_files)

if __name__ == '__main__' and '__file__' in globals():
    sys.exit(main())
