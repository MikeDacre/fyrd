"""
Defined all available cluster environements.
"""
CLUSTERS = [
    'slurm',
    'torque',
    'local',
]
"""These batch systems are evaluated in order when detecting cluster env."""
for cluster in CLUSTERS:
    exec('from . import {}'.format(cluster))
