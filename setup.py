"""
Setup Script for Slurmy
"""
import os
import sys
import codecs

# Make setuptools work everywhere
import ez_setup
ez_setup.use_setuptools()

import setuptools
from setuptools import setup
from setuptools.command.install import install
from setuptools.command.test import test as TestCommand
log = setuptools.distutils.log

here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with codecs.open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

# Generate a list of python scripts
scpts = []
scpt_dir = os.listdir(os.path.join(here, 'bin'))
for scpt in scpt_dir:
    scpts.append(os.path.join('bin', scpt))

class ScriptInstaller(install):

    """Make scripts copy directly instead of being wrapped.

    This also changes the way the module is installed, see::
        http://stackoverflow.com/questions/37874445
    """

    def run(self):
        """Wrapper for parent run, display message first."""

        sys.stderr.write('\nWelcome to Python Cluster!\n')
        sys.stderr.write('Make sure you install this program to a place \n'
                         'that is accessible cluster wide if you want\n'
                         'cluster jobs to be able to submit child jobs.\n')
        sys.stderr.write('\nSome helper scripts not required by the library \n'
                         'will also be installed to the default PATH chosen \n'
                         'by the python install software, this is usually \n'
                         '~/.local/bin or /usr/local/bin, you need to make \n'
                         'sure this location is in your PATH\n\n')

        # Call the main installer
        install.run(self)


class TestRunner(TestCommand):

    """Run script in tests directory."""

    def run_tests(self):
        """Run the test script, skip remote tests here."""
        import sys
        from subprocess import check_call
        # The remote queue testing can fail for a variety of config reasons
        # so we won't run it here
        check_call([sys.executable, 'tests/run_tests.py', '-l'])
        #  check_call([sys.executable, 'tests/run_tests.py'])

setup(
    name='python-cluster',
    version='0.6.1b',
    description='Submit functions and shell scripts to torque, slurm, ' +
                'or local machines',
    long_description=long_description,
    url='https://github.com/MikeDacre/python-cluster',
    author='Michael Dacre',
    author_email='mike.dacre@gmail.com',
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'Environment :: Console',
        'Operating System :: Linux',
        'Natural Language :: English',
        'Topic :: System :: Clustering',
        'Topic :: System :: Monitoring',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],

    keywords='slurm cluster job_management',

    requires=['dill'],
    tests_require=['pytest'],
    packages=['cluster'],
    cmdclass={'install': ScriptInstaller,
              'test': TestRunner},
    scripts=scpts,
)
