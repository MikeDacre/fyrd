"""
Setup Script for Slurmy
"""
from setuptools import setup, find_packages
from Cython.Build import cythonize
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

# Generate a list of python scripts
scpts = []
for i in listdir(here + '/bin'):
    if i.endswith('.py'):
        scpts.append('bin/' + i)

setup(
    name='slurmy',
    version='0.2',
    description='Submit and monitor slurm jobs with python',
    long_description=long_description,
    url='https://github.com/MikeDacre/python_slurm',
    author='Michael Dacre',
    author_email='mike.dacre@gmail.com',
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 1 - Planning',
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

    install_requires=['pyslurm', 'cython'],
    packages=['slurmy'],
    scripts=scpts,
    ext_modules=cythonize("slurmy/*.pyx")
)
