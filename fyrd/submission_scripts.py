# -*- coding: utf-8 -*-
"""
Classes to build submission scripts.
"""
import os  as _os
import sys as _sys
import inspect as _inspect
from textwrap import dedent as _ddent

# Try to use dill, revert to pickle if not found
try:
    import dill as _pickle
except ImportError:
    try:
        import cPickle as _pickle # For python2
    except ImportError:
        import _pickle

###############################################################################
#                               Import Ourself                                #
###############################################################################

from . import run as _run
from . import logme as _logme
from .run import indent as _ident


class Script(object):

    """A script string plus a file name."""

    written = False

    def __init__(self, file_name, script):
        """Initialize the script and file name."""
        self.script    = script
        self.file_name = _os.path.abspath(file_name)

    def write(self, overwrite=True):
        """Write the script file."""
        _logme.log('Script: Writing {}'.format(self.file_name), 'debug')
        pth = _os.path.split(_os.path.abspath(self.file_name))[0]
        if not _os.path.isdir(pth):
            raise OSError('{} Does not exist, cannot write scripts'
                          .format(pth))
        if overwrite or not _os.path.exists(self.file_name):
            with open(self.file_name, 'w') as fout:
                fout.write(self.script + '\n')
            self.written = True
            return self.file_name
        else:
            return None

    def clean(self, _=None):
        """Delete any files made by us."""
        if self.written and self.exists:
            _logme.log('Script: Deleting {}'.format(self.file_name), 'debug')
            _os.remove(self.file_name)

    def __getattr__(self, attr):
        """Make sure boolean is up to date."""
        if attr == 'exists':
            return _os.path.exists(self.file_name)

    def __repr__(self):
        """Display simple info."""
        return "Script<{}(exists: {}; written: {})>".format(
            self.file_name, self.exists, self.written)

    def __str__(self):
        """Print the script."""
        return repr(self) + '::\n\n' + self.script + '\n'


class Function(Script):

    """A special Script used to run a function."""

    def __init__(self, file_name, function, args=None, kwargs=None,
                 imports=None, pickle_file=None, outfile=None):
        """Create a function wrapper.

        NOTE: Function submission will fail if the parent file's code is not
        wrapped in an if __main__ wrapper.

        Args:
            file_name (str):     A root name to the outfiles
            function (callable): Function handle.
            args (tuple):        Arguments to the function as a tuple.
            kwargs (dict):       Named keyword arguments to pass in the
                                 function call
            imports(list):       A list of imports, if not provided, defaults
                                 to all current imports, which may not work if
                                 you use complex imports.  The list can include
                                 the import call, or just be a name, e.g
                                 ['from os import path', 'sys']
            pickle_file (str): The file to hold the function.
            outfile (str):     The file to hold the output.
        """
        self.function = function
        rootmod       = _inspect.getmodule(self.function)
        self.parent   = rootmod.__name__
        self.args     = args
        self.kwargs   = kwargs

        # Get the module path
        if hasattr(rootmod, '__file__'):
            imppath, impt = _os.path.split(rootmod.__file__)
            impt = _os.path.splitext(impt)[0]
        else:
            imppath = '.'
            impt = None
        imppath = _os.path.abspath(imppath)

        # Clobber ourselves to prevent pickling errors
        if impt and self.function.__module__ == '__main__':
            self.function.__module__ = impt

        # Import the submitted function
        if impt:
            imp1 = 'from {} import {}'.format(impt, self.function.__name__)
            imp2 = 'from {} import *'.format(impt)
            if impt != self.parent:
                imppath2 = _os.path.abspath(_os.path.join(
                    imppath,
                    *['..' for i in range(self.parent.count('.'))]
                ))
                bimp1 = 'from {} import {}'.format(
                    self.parent, self.function.__name__)
                bimp2 = 'from {} import *'.format(self.parent)
            else:
                bimp1 = None
        elif self.parent != self.function.__name__ and self.parent != '__main__':
            imp1 = 'from {} import {}'.format(
                self.parent, self.function.__name__)
            imp2 = 'from {} import *'.format(self.parent)
            bimp1 = None
        else:
            imp1 = 'import {}'.format(self.function.__name__)
            imp2 = None
            bimp1 = None

        # Try to set a sane import string to make the function work
        if bimp1:
            modstr = _ddent("""\
            sys.path.append('{imppath1}')
            try:
                try:
                    {imp1}
                except SystemError:
                    sys.path.append('{imppath2}')
                    try:
                        {bimp1}
                    except ImportError:
                        pass
            except ImportError:
                pass
            try:
                try:
                    {imp2}
                except SystemError:
                    try:
                        {bimp2}
                    except ImportError:
                        pass
            except ImportError:
                pass
            """).format(imppath1=imppath, imppath2=imppath2,
                        imp1=imp1, imp2=imp2, bimp1=bimp1, bimp2=bimp2)
        else:
            modstr = _ddent("""\
            sys.path.append('{imppath1}')
            try:
                {imp1}
            except ImportError:
                pass
            try:
                {imp2}
            except ImportError:
                pass
            """).format(imppath1=imppath, imp1=imp1, imp2=imp2)
        modstr = _ident(modstr, '    ')

        ##########################
        #  Take care of imports  #
        ##########################
        if imports:
            if isinstance(imports, str):
                imports = [imports]
            else:
                try:
                    imports = list(imports)
                except TypeError:
                    pass
        else:
            imports = []
        imports = _run.normalize_imports(imports, prot=False)

        # Compute imports automatically
        auto_imports     = _run.get_imports(function, 'string')
        filtered_imports = _run.normalize_imports(imports + auto_imports)

        # Get rid of duplicates and join imports
        impts = _ident('\n'.join(set(filtered_imports)), '    ')

        # Set file names
        self.pickle_file = pickle_file if pickle_file else file_name + '.pickle.in'
        self.outfile     = outfile if outfile else file_name + '.pickle.out'

        # Create script text
        script = '#!{}\n'.format(_sys.executable)
        script += _run.FUNC_RUNNER.format(name=file_name,
                                          path=imppath,
                                          modimpstr=modstr,
                                          imports=impts,
                                          pickle_file=self.pickle_file,
                                          out_file=self.outfile)

        super(Function, self).__init__(file_name, script)

    def write(self, overwrite=True):
        """Write the pickle file and call the parent Script write function."""
        _logme.log('Writing pickle file {}'.format(self.pickle_file), 'debug')
        with open(self.pickle_file, 'wb') as fout:
            _pickle.dump((self.function, self.args, self.kwargs), fout)
        super(Function, self).write(overwrite)

    def clean(self, delete_output=False):
        """Delete the input pickle file and any scripts.

        Args:
            delete_output (bool): Delete the output pickle file too.
        """
        if self.written:
            if _os.path.isfile(self.pickle_file):
                _logme.log('Function: Deleting {}'.format(self.pickle_file),
                           'debug')
                _os.remove(self.pickle_file)
            if delete_output and _os.path.isfile(self.outfile):
                _logme.log('Function: Deleting {}'.format(self.outfile),
                           'debug')
                _os.remove(self.outfile)
        super(Function, self).clean(delete_output)
