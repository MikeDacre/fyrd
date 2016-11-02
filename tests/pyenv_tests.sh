#!/bin/bash
# I intentionally am not spinning up new virtualenvs every time, this is
# done by Travis CI anyway, and it takes a long time, using existing
# virtualenvs is much faster.
# Running this script is not required, simply running a single test is
# usually sufficient (python tests/run_tests.py). To use this code to
# test multiple virtualenvs, you must:
# - Use pyenv
# - Install an anaconda3-4.1.1 environment
# - Create any number of virtualenvs named fyrd_# where # can be anything
# This script will then reinstall the latest version of this code into each
# of these virtualenvs and run the test suite in each. It will additionally
# run the pandas specific tests in the anaconda environment only.
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

versions=$(pyenv versions | grep " fyrd_" | sed 's/\*//g' | sed 's/^ \+//g' | sed 's/ .*//g')

counter=0
codes=0
for i in ${versions[@]}; do
  echo "Testing in $i"
  pyenv shell $i
  pip uninstall -y fyrd
  pip install .
  pip install -r tests/test_requirements.txt
  if [ -e 'tests' ]; then
    python tests/run_tests.py $@
    code=$!
  else
    python run_tests.py $@
    code=$!
  fi
  counter=$((counter+1))
  codes=$((codes+code))
done

echo "Completed main tests. Ran $counter, total exit code: $codes"
echo ""

echo "Running tests in anaconda"
pyenv shell anaconda3-4.1.1
pip uninstall -y fyrd
pip install .
pip install -r tests/test_requirements.txt
if [ -e 'tests' ]; then
  python tests/run_tests.py $@
else
  python run_tests.py $@
fi
echo "Running pandas test in anaconda"
if [ -e 'tests' ]; then
  python tests/pandas_run.py $@
else
  python pandas_run.py $@
fi
echo "All tests complete, please review the outputs manually."
