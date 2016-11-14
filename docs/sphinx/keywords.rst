Keyword Arguments
=================

To make submission easier, this module defines a number of keyword arguments in
the options.py file that can be used for all submission and Job() functions.
These include things like 'cores' and 'nodes' and 'mem'. 

The following is a complete list of arguments that can be used in this version

.. include:: keyword_table.rst

*Note:* Type is enforced, any provided argument must match that python type
(automatic conversion is attempted), the default is just a recommendation and is
not currently used. These arguments are passed like regular arguments to the
submission and Job() functions, eg::

  Job(nodes=1, cores=4, mem='20MB')

This will be interpretted correctly on any system. If torque or slurm are not
available, any cluster arguments will be ignored. The module will attempt to
honor the cores request, but if it exceeds the maximum number of cores on the
local machine, then the request will be trimmed accordingly (i.e. a 50 core
request will become 8 cores on an 8 core machine).

Adding your own keywords
------------------------

There are many more options available for torque and slurm, to add your own,
edit the options.py file, and look for CLUSTER_OPTS (or TORQUE/SLURM if your
keyword option is only availble on one system). Add your option using the same
format as is present in that file. The format is::

  ('name', {'slurm': '--option-str={}', 'torque': '--torque-option={}',
            'help': 'This is an option!', 'type': str, 'default': None})

You can also add list options, but they must include 'sjoin' and 'tjoin' keys to
define how to merge the list for slurm and torque, or you must write custom
option handling code in ``fyrd.options.options_to_string()``. For an
excellent example of both approaches included in a single option, see the
'features' keyword above.
