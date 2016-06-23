"""Test the config_file handling."""
import os
import sys
sys.path.append(os.path.abspath('.'))
import cluster


def test_change_file():
    """Change the default config file to here."""
    cluster.config_file.CONFIG_FILE = os.path.abspath('conftest')


def test_create():
    """Try to create a config file."""
    assert cluster.config_file.CONFIG_FILE == os.path.abspath('conftest')
    cluster.config_file.create_config()
    assert os.path.isfile(os.path.abspath('conftest'))


def test_get():
    """Get the config a few ways."""
    cluster.config_file.get_config()
    cluster.config_file.get_option()


def test_set_get():
    """Create and option, check it, and delete it."""
    cluster.config_file.set_option('bob', 'test', 42)
    assert cluster.config_file.get_option('bob', 'test') is 42
    cluster.config_file.delete('bob')


def test_profile():
    """Test the Profile() object."""
    p = cluster.config_file.Profile('tprof', {'cores': 2})
    p.write()
    j = cluster.config_file.get_profile('tprof')
    assert isinstance(j, cluster.config_file.Profile)


def test_delete():
    """Delete the temp config file."""
    os.remove(os.path.abspath('conftest'))
