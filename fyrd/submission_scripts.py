# -*- coding: utf-8 -*-
"""
Classes to build submission scripts.

Last modified: 2016-11-04 17:59
"""
import os  as _os
import sys as _sys
import inspect as _inspect
from types import ModuleType as _ModuleType

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

    def __init__(self, file_name, function, args=None, imports=None,
                 pickle_file=None, outfile=None):
        """Create a function wrapper.

        NOTE: Function submission will fail if the parent file's code is not
        wrapped in an if __main__ wrapper.

        Args:
            file_name:   A root name to the outfiles
            function:    Function handle.
            args:        Arguments to the function as a tuple.
            imports:     A list of imports, if not provided, defaults to all
                         current imports, which may not work if you use complex
                         imports.  The list can include the import call, or
                         just be a name, e.g ['from os import path', 'sys']
            pickle_file: The file to hold the function.
            outfile:     The file to hold the output.
        """
        self.function = function
        rootmod       = _inspect.getmodule(self.function)
        self.parent   = rootmod.__name__
        self.args     = args

        # Get the module path
        rootname = self.parent
        if hasattr(rootmod, '__file__'):
            imppath, impt = _os.path.split(rootmod.__file__)
            impt = _os.path.splitext(impt)[0]
        else:
            imppath = '.'
            impt = None

        # Clobber ourselves to prevent pickling errors
        if impt and self.function.__module__ == '__main__':
            self.function.__module__ = impt

        # Try to set a sane import string to make the function work
        if impt:
            modstr  = ('try:\n    from {} import {}\n'
                       .format(impt, self.function.__name__) +
                       'except ImportError:\n    pass\n')
            modstr += ('try:\n    from {} import *\n'
                       .format(impt) +
                       'except ImportError:\n    pass')
        elif rootname != self.function.__name__ and rootname != '__main__':
            modstr  = ('try:\n    from {} import {}\n'
                       .format(rootname, self.function.__name__) +
                       'except ImportError:\n    pass\n')
            modstr += ('try:\n    from {} import *\n'
                       .format(rootname) +
                       'except ImportError:\n    pass')
        else:
            modstr = ('try:\n    import {}\n'.format(self.function.__name__) +
                      'except ImportError:\n    pass')

        # Take care of imports, either use manual or all current imports
        if imports:
            if not isinstance(imports, (list, tuple)):
                imports = [imports]
        else:
            imports = []

        for name, module in {k:i for k,i in globals().items()
                             if isinstance(i, _ModuleType)}.items():
            if name.startswith('_'):
                continue
            imports.append((name, module.__name__))

        imports = list(set(imports))

        # Create a sane set of imports
        ignore_list = ['os', 'sys', 'dill', 'pickle']
        filtered_imports = []
        for imp in sorted(imports):
            if imp in ignore_list:
                continue
            if isinstance(imp, tuple):
                iname, name = imp
                names = name.split('.')
                if iname in ignore_list:
                    continue
                if iname != name:
                    if len(names) > 1:
                        if '.'.join(names[1:]) != iname:
                            filtered_imports.append(
                                ('try:\n    from {} import {} as {}\n'
                                 'except ImportError:\n    pass\n')
                                .format(names[0], '.'.join(names[1:]), iname)
                            )
                        else:
                            filtered_imports.append(
                                ('try:\n    from {} import {}\n'
                                 'except ImportError:\n    pass\n')
                                .format(names[0], '.'.join(names[1:]))
                            )
                    else:
                        filtered_imports.append(
                            ('try:\n    import {} as {}\n'
                             'except ImportError:\n    pass\n')
                            .format(name, iname)
                        )
                else:
                    filtered_imports.append(('try:\n    import {}\n'
                                             'except ImportError:\n    pass\n')
                                            .format(name))
                if names[0] not in ignore_list:
                    filtered_imports.append(('try:\n    from {} import *\n'
                                             'except ImportError:\n    pass\n')
                                            .format(names[0]))

            else:
                if imp.startswith('import') or imp.startswith('from'):
                    filtered_imports.append('try:\n    ' + imp.rstrip() +
                                            'except ImportError:\n    pass')
                else:
                    filtered_imports.append(('try:\n    import {}\n'
                                             'except ImportError:\n    pass\n')
                                            .format(imp))

        # Get rid of duplicates and sort imports
        impts = '\n'.join(set(filtered_imports))

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
            _pickle.dump((self.function, self.args), fout)
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
