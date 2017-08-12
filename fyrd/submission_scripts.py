# -*- coding: utf-8 -*-
"""
Classes to build submission scripts.
"""
import os  as _os
import sys as _sys
import inspect as _inspect
import dill as _pickle

###############################################################################
#                               Import Ourself                                #
###############################################################################

from . import run as _run
from . import logme as _logme
from . import script_runners as _scrpts


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

    def clean(self, delete_output=None):
        """Delete any files made by us."""
        if delete_output:
            _logme.log('delete_output not implemented in Script', 'debug')
        if self.written and self.exists:
            _logme.log('Script: Deleting {}'.format(self.file_name), 'debug')
            _os.remove(self.file_name)

    @property
    def exists(self):
        """True if file is on disk, False if not."""
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
                 imports=None, syspaths=None, pickle_file=None, outfile=None):
        """Create a function wrapper.

        NOTE: Function submission will fail if the parent file's code is not
        wrapped in an if __main__ wrapper.

        Parameters
        ----------
        file_name : str
            A root name to the outfiles
        function : callable
            Function handle.
        args : tuple, optional
            Arguments to the function as a tuple.
        kwargs : dict, optional
            Named keyword arguments to pass in the function call
        imports : list, optional
            A list of imports, if not provided, defaults to all current
            imports, which may not work if you use complex imports.  The list
            can include the import call, or just be a name, e.g ['from os
            import path', 'sys']
        syspaths : list, optional
            Paths to be included in submitted function
        pickle_file : str, optional
            The file to hold the function.
        outfile : str, optional
            The file to hold the output.
        """
        _logme.log('Building Function for {}'.format(function), 'debug')
        self.function = function
        self.parent   = _inspect.getmodule(function)
        self.args     = args
        self.kwargs   = kwargs

        ##########################
        #  Take care of imports  #
        ##########################
        filtered_imports = _run.get_all_imports(
            function, {'imports': imports}, prot=True
        )

        # Get rid of duplicates and join imports
        impts = _run.indent('\n'.join(set(filtered_imports)), '    ')

        # Import the function itself
        func_import = _run.indent(_run.import_function(function), '    ')

        # sys paths
        if syspaths:
            _logme.log('Syspaths: {}'.format(syspaths), 'debug')
            impts = (_run.indent(_run.syspath_fmt(syspaths), '    ') + '\n\n'
                     + impts)

        # Set file names
        self.pickle_file = pickle_file if pickle_file else file_name + '.pickle.in'
        self.outfile     = outfile if outfile else file_name + '.pickle.out'

        # Create script text
        script = '#!{}\n'.format(_sys.executable)
        script += _scrpts.FUNC_RUNNER.format(name=file_name,
                                             modimpstr=func_import,
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

        Parameters
        ----------
        delete_output : bool, optional
            Delete the output pickle file too.
        """
        if self.written:
            if _os.path.isfile(self.pickle_file):
                _logme.log('Function: Deleting {}'.format(self.pickle_file),
                           'debug')
                _os.remove(self.pickle_file)
            else:
                _logme.log('Function: {} already gone'
                           .format(self.pickle_file), 'debug')
            if delete_output:
                if _os.path.isfile(self.outfile):
                    _logme.log('Function: Deleting {}'.format(self.outfile),
                               'debug')
                    _os.remove(self.outfile)
                else:
                    _logme.log('Function: {} already gone'
                               .format(self.outfile), 'debug')
        super(Function, self).clean(None)
