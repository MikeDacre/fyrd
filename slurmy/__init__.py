# Load config file
from ._get_config import _get_config

_defaults = _get_config()

# Regular functions
from .slurmy import *

# Cython
from .queue import *
