#!/usr/bin/env python
"""
Submission strings for running remote commands.
"""

SCRP_RUNNER = """\
#!/bin/bash
{precmd}
mkdir -p $LOCAL_SCRATCH > /dev/null 2>/dev/null
if [ -f {script} ]; then
    {command}
    exit $?
else
    echo "{script} does not exist, make sure you set your filepath to a "
    echo "directory that is available to the compute nodes."
    exit 1
fi
"""

SCRP_RUNNER_TRACK = """\
#!/bin/bash
{precmd}
mkdir -p $LOCAL_SCRATCH > /dev/null 2>/dev/null
if [ -f {script} ]; then
    cd {usedir}
    date +'%y-%m-%d-%H:%M:%S'
    echo "Running {name}"
    {command}
    exitcode=$?
    echo Done
    date +'%y-%m-%d-%H:%M:%S'
    if [[ $exitcode != 0 ]]; then
        echo Exited with code: $exitcode >&2
    fi
    exit $exitcode
else
    echo "{script} does not exist, make sure you set your filepath to a "
    echo "directory that is available to the compute nodes."
    exit 1
fi
"""

CMND_RUNNER = """\
{precmd}
mkdir -p $LOCAL_SCRATCH > /dev/null 2>/dev/null
cd {usedir}
{command}
exitcode=$?
if [[ $exitcode != 0 ]]; then
    echo Exited with code: $exitcode >&2
fi
exit $exitcode
"""

CMND_RUNNER_TRACK = """\
{precmd}
mkdir -p $LOCAL_SCRATCH > /dev/null 2>/dev/null
cd {usedir}
date +'%y-%m-%d-%H:%M:%S'
echo "Running {name}"
{command}
exitcode=$?
echo Done
date +'%y-%m-%d-%H:%M:%S'
if [[ $exitcode != 0 ]]; then
    echo Exited with code: $exitcode >&2
fi
exit $exitcode
"""

FUNC_RUNNER = r"""\
'''
Run a function remotely and pickle the result.

To try and make this as resistent to failure as possible, we import everything
we can, this sometimes results in duplicate imports and always results in
unnecessary imports, but given the context we don't care, as we just want the
thing to run successfully on the first try, no matter what.
'''
import os
import sys
import socket
from subprocess import Popen, PIPE
import six
from tblib import pickling_support
pickling_support.install()
import dill as pickle

out = None
try:
{imports}
{modimpstr}
except Exception:
    out = sys.exc_info()

ERR_MESSAGE = '''\
Failed to import your function. This usually happens when you have a module \
installed locally that is not available on the compute nodes.

In this case the module is {{}}.

However, I can only catch the first uninstalled module. To make sure all of \
your modules are installed on the compute nodes, do this::

    freeze --local | grep -v ^\-e | cut -d = -f 1 "> module_list.txt\n

Then, submit a job to the compute nodes with this command::

    cat module_list.txt | xargs pip install --user
'''


def run_function(func_c, args=None, kwargs=None):
    '''Run a function with arglist and return output.'''
    if not hasattr(func_c, '__call__'):
        raise Exception('{{}} is not a callable function.'
                        .format(func_c))
    if args and kwargs:
        ot = func_c(*args, **kwargs)
    elif args:
        try:
            iter(args)
        except TypeError:
            args = (args,)
        if isinstance(args, str):
            args = (args,)
        ot = func_c(*args)
    elif kwargs:
        ot = func_c(**kwargs)
    else:
        ot = func_c()
    return ot


if __name__ == "__main__":
    # If an Exception was raised during import, skip this
    if not out:
        with open('{pickle_file}', 'rb') as fin:
            # Try to install packages first
            try:
                function_call, args, kwargs = pickle.load(fin)
            except ImportError as e:
                out = sys.exc_info()
                module = str(e).split(' ')[-1]
                node   = socket.gethostname()
                sys.stderr.write(ERR_MESSAGE.format(module))
                out = list(out)
                out[1] = ImportError(
                    'Module {{}} is not installed on compute node {{}}'
                    .format(module, node)
                )
                out = tuple(out)
            except:
                out = sys.exc_info()

    try:
        if not out:
            out = run_function(function_call, args, kwargs)
    except Exception:
        out = sys.exc_info()

    with open('{out_file}', 'wb') as fout:
        pickle.dump(out, fout)

    if isinstance(out, tuple):
        if issubclass(BaseException, out[0]):
            six.reraise(*out)
"""
