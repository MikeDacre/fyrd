# -*- coding: utf-8 -*-
"""
Setup Script for Fyrd
"""
import os
import codecs

import setuptools
from setuptools import setup
from setuptools.command.test import test as TestCommand
import versioneer

log = setuptools.distutils.log

VERSION=versioneer.get_version()
GITHUB='https://github.com/MikeDacre/fyrd'

###############################################################################
#                            A class to run tests                             #
###############################################################################

class TestRunner(TestCommand):

    """Run script in tests directory with py.test and without.

    The local queue can't be tested with py.test, so we run it outside of
    py.test, we also skip remote tests here because they require some config
    before being able to successfully execute.
    """

    def run_tests(self):
        """Run the test script, skip remote tests here."""
        import sys
        from subprocess import check_call
        # The remote queue testing can fail for a variety of config reasons
        # so we won't run it here
        check_call([sys.executable, 'tests/run_tests.py', '-l'])


###############################################################################
#                     Build the things we need for setup                      #
###############################################################################

# Get the long description from the README file
here = os.path.abspath(os.path.dirname(__file__))
with codecs.open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

# Generate a list of python scripts
scpts = []
scpt_dir = os.listdir(os.path.join(here, 'bin'))
for scpt in scpt_dir:
    scpts.append(os.path.join('bin', scpt))

# Set command class
cmdclass = versioneer.get_cmdclass()
tcmdclss = {'test': TestRunner}
for k, v in cmdclass.items():
    tcmdclss[k] = v
cmdclass = tcmdclss

###############################################################################
#                                Setup Options                                #
###############################################################################

setup(
    name='fyrd',
    version=VERSION,
    description=('Submit functions and shell scripts to torque, slurm, ' +
                 'or local machines'),
    long_description=long_description,
    url='https://fyrd.science',
    download_url='{}/archive/v{}.tar.gz'.format(
        GITHUB, VERSION
    ),
    author='Michael Dacre',
    author_email='mike.dacre@gmail.com',
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'Environment :: Console',
        'Operating System :: POSIX :: Linux',
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
        'Programming Language :: Python :: 3.6',
    ],

    keywords='slurm torque multiprocessing cluster job_management',

    requires=['dill', 'tabulate', 'six', 'tblib', 'psutil'
              'tqdm', 'Pyro4', 'sqlalchemy'],
    install_requires=['dill', 'tabulate', 'six', 'Pyro4', 'psutil',
                      'tblib', 'tqdm', 'sqlalchemy'],
    tests_require=['pytest'],
    packages=['fyrd', 'fyrd/batch_systems'],
    cmdclass=cmdclass,
    scripts=scpts,
    entry_points={
        'console_scripts': [
            'fyrd = fyrd.__main__:main',
        ]
    },
)
