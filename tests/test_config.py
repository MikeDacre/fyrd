"""Test the conf handling."""
import os
import sys
sys.path.append(os.path.abspath('.'))
import fyrd


def test_get_config():
    """Simply get the config."""
    fyrd.conf.get_config()

def test_change_file():
    """Change the default config file to here."""
    fyrd.conf.CONFIG_FILE = os.path.abspath('conftest')


def test_create():
    """Try to create a config file."""
    assert fyrd.conf.CONFIG_FILE == os.path.abspath('conftest')
    fyrd.conf.create_config(
        {'jobs':{'profile_file': os.path.abspath('proftest')}}
    )
    assert os.path.isfile(os.path.abspath('conftest'))
    fyrd.conf.load_config()


def test_get():
    """Get the config a few ways."""
    fyrd.conf.load_config()
    fyrd.conf.get_option()


def test_set_get():
    """Create and option, check it, and delete it."""
    fyrd.conf.set_option('jobs', 'test', 42)
    assert fyrd.conf.get_option('jobs', 'test') is 42
    fyrd.conf.delete('jobs', 'test')


def test_bool():
    """Set and get a bool option."""
    fyrd.conf.set_option('queue', 'bool', True)
    assert fyrd.conf.get_option('queue', 'bool') is True


def test_create_profile_file():
    """Make a new profile file."""
    assert fyrd.conf.get_option('jobs', 'profile_file')\
        == os.path.abspath('proftest')
    fyrd.conf.create_profiles()


def test_profile():
    """Test the Profile() object."""
    p = fyrd.conf.Profile('tprof', {'cores': 2})
    p.write()
    j = fyrd.conf.get_profile('tprof')
    assert isinstance(j, fyrd.conf.Profile)


def test_update():
    """Try to update and existing profile."""
    fyrd.conf.set_profile('tprof', {'mem': 16000}, update=True)
    p = fyrd.conf.get_profile('tprof')
    assert p.mem   == 16000
    assert p.cores == 2


def test_replace():
    """Try to replace an existing profile."""
    p = fyrd.conf.set_profile('tprof', {'mem': 26000}, update=False)
    assert p.mem   == 26000
    assert p.cores == 1


def test_del_profile():
    """Try to delete the profile we just made."""
    fyrd.conf.del_profile('tprof')


def test_delete():
    """Delete the temp config file."""
    os.remove(os.path.abspath('conftest'))
    os.remove(os.path.abspath('proftest'))
    fyrd.conf.CONFIG_FILE = os.path.abspath(
        os.path.expanduser('~/.fyrd/config.txt')
    )
    fyrd.conf.set_option(
        'jobs', 'profile_file', os.path.expanduser('~/.fyrd/profiles.txt')
    )
