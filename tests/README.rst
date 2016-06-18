Testing Scripts
===============

The bulk of the testing for this pipeline is contained in the test_*.py files
in this directory. However, py.test has some limitations, it does not play nice
with the local queue portion of this script, possibly because it aggressively
takes control of STDOUT and STDERR, and the local queue does pipeing within
child processes. To circumvent that, the local_queue.py script exists.

To test the local queue, just execute `local_queue.py`, if it exits
successfully the tests passed.

The ./write_options_to_file.py script should be run every time the keyword
options are updated, it generates ./options_help.txt, which is used by the
testing suite to make sure options are being formatted correctly.
