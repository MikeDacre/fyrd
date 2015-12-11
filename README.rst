# Python Slurm

Depends on [pyslurm](https://github.com/gingergeeks/pyslurm) by [Ginger Geeks](gingergeeks.co.uk).

Allows the user to submit slurm jobs from a python script and monitor progress.

Useful if your cluster limits total queue submissions, as your personal queue length can be monitored and jobs only submitted when queue length drops below a certain count.

Allows simple dependency tracking with SLURM's built in dependency tracking commands, or script level dependency tracking by monitoring job state and output files.

NOTE: Under active development. Unstable. Do not use.
