"""Test things in run.py that are not used often."""
import os
import fyrd
fyrd.logme.MIN_LEVEL = 'debug'

def test_write_iterable():
    """Run the write_iterable function."""
    fyrd.run.write_iterable(['hi', 'there'], 'test.out')
    with open('test.out') as infile:
        contents = infile.read()
    assert contents == 'hi\nthere'
    os.remove('test.out')

def test_file_type():
    """Test file type parsing."""
    assert fyrd.run.file_type('file.txt') == 'txt'
    assert fyrd.run.file_type('file.txt.gz') == 'txt'
    assert fyrd.run.file_type('file.txt.bz2') == 'txt'
    with open('hi.txt', 'w') as fout:
        assert fyrd.run.is_file_type(fout, 'txt')
    os.remove('hi.txt')
    assert not fyrd.run.is_file_type('file.txt', 'bob')

def test_which():
    """Test getting paths."""
    ls = fyrd.run.which('ls')
    ls = fyrd.run.which(ls)
