# -*- coding: utf-8 -*-
"""
Classes to build submission scripts.
"""
import os  as _os
import sys as _sys
import inspect as _inspect
from textwrap import dedent as _ddent
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
            if not isinstance(imports, (list, tuple)):
                imports = [imports]
        else:
            imports = []

        func_imports = []

        # Import everything in current and function globals
        import_places = [
            #  dict(globals().items()),
            dict(_inspect.getmembers(function))['__globals__'],
        ]
        for place in import_places:
            for name, item in place.items():
                # Module
                if _inspect.ismodule(item):
                    if name != '__main__' or not name.startswith('__'):
                        imports.append((name, item.__name__))
                # Function
                elif callable(item):
                    try:
                        func_imports.append((name, item.__name__,
                                             item.__module__))
                    except AttributeError:
                        pass

        # Import all modules in the root module
        imports += [(k,v.__name__) for k,v in
                    _inspect.getmembers(rootmod, _inspect.ismodule)
                    if not k.startswith('__')]

        imports = sorted(list(set(imports)), key=_sort_imports)
        func_imports = sorted(list(set(func_imports)), key=_sort_imports)
        _logme.log('Imports: {}'.format(imports), 'debug')

        # Create a sane set of imports
        ignore_list = ['os', 'sys', 'dill', 'pickle', '__main__']
        filtered_imports = []
        for imp in imports:
            if imp in ignore_list:
                continue
            if isinstance(imp, tuple):
                iname, name = imp
                names = name.split('.')
                if iname in ignore_list:
                    continue
                if name.startswith('@') or iname.startswith('@'):
                    continue
                if iname != name:
                    if len(names) > 1:
                        if '.'.join(names[1:]) != iname:
                            filtered_imports.append(
                                ('try:\n    from {} import {} as {}\n'
                                 'except ImportError:\n    pass\n')
                                .format('.'.join(names[:-1]), names[-1], iname)
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

            else:
                if imp.startswith('import') or imp.startswith('from'):
                    filtered_imports.append('try:\n    ' + imp.rstrip() +
                                            '\nexcept ImportError:\n    pass')
                else:
                    if imp.startswith('@'):
                        continue
                    filtered_imports.append(('try:\n    import {}\n'
                                             'except ImportError:\n    pass\n')
                                            .format(imp))

            # Function imports
            for iname, name, mod in func_imports:
                if iname in ignore_list:
                    continue
                if iname == name:
                    filtered_imports.append(
                        ('try:\n    from {} import {}\n'
                         'except ImportError:\n    pass\n')
                        .format(mod, name)
                    )
                else:
                    filtered_imports.append(
                        ('try:\n    from {} import {} as {}\n'
                         'except ImportError:\n    pass\n')
                        .format(mod, name, iname)
                    )


        # Get rid of duplicates and sort imports
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


def _sort_imports(x):
    """Sort a list of tuples and strings, for use with sorted."""
    if isinstance(x, tuple):
        if x[1] == '__main__':
            return 0
        return x[1]
    return x
