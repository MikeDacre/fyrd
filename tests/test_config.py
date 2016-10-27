"""Test the config_file handling."""
import os
import sys
sys.path.append(os.path.abspath('.'))
import fyrd


def test_change_file():
    """Change the default config file to here."""
    fyrd.config_file.CONFIG_FILE = os.path.abspath('conftest')


def test_create():
    """Try to create a config file."""
    assert fyrd.config_file.CONFIG_FILE == os.path.abspath('conftest')
    fyrd.config_file.create_config()
    assert os.path.isfile(os.path.abspath('conftest'))


def test_get():
    """Get the config a few ways."""
    fyrd.config_file.get_config()
    fyrd.config_file.get_option()


def test_set_get():
    """Create and option, check it, and delete it."""
    fyrd.config_file.set_option('bob', 'test', 42)
    assert fyrd.config_file.get_option('bob', 'test') is 42
    fyrd.config_file.delete('bob')


def test_profile():
    """Test the Profile() object."""
    p = fyrd.config_file.Profile('tprof', {'cores': 2})
    p.write()
    j = fyrd.config_file.get_profile('tprof')
    assert isinstance(j, fyrd.config_file.Profile)


def test_delete():
    """Delete the temp config file."""
    os.remove(os.path.abspath('conftest'))
