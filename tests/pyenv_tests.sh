#!/bin/bash
# I intentionally am not spinning up new virtualenvs every time, this is
# done by Travis CI anyway, and it takes a long time, using existing
# virtualenvs is much faster.
# Running this script is not required, simply running a single test is
# usually sufficient (python tests/run_tests.py). To use this code to
# test multiple virtualenvs, you must:
# - Use pyenv and pyenv-virtualenv
# - Keep the versions array up to date with the versions you want to test
# This script will then reinstall the latest version of this code into each
# of these virtualenvs and run the test suite in each. It will additionally
# run the pandas specific tests in the anaconda environment only.
PYENV_HOME=$HOME/.pyenv
PATH=$PYENV_HOME/bin:$PATH
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

versions=('2.7.10' '2.7.11' '2.7.12' '3.3.0' '3.4.0' '3.5.2')
bad_build_versions=('3.6-dev' '3.7-dev')

counter=0
codes=0
for i in ${versions[@]}; do
  echo "Testing in $i"
  v="fyrd_$i"
  pyenv install -s $i
  echo "Creating virtualenv $v"
  pyenv virtualenv $i $v
  pyenv shell $v
  echo "Installing fyrd"
  python ./setup.py install >/dev/null
  echo "Installing requirements"
  pip install -r tests/test_requirements.txt >/dev/null
  echo "Running test suite"
  python tests/run_tests.py $@
  code=$!
  counter=$((counter+1))
  codes=$((codes+code))
  echo "Deleteing $v"
  pyenv virtualenv-delete -f $v
done

echo "Completed main tests. Ran $counter, total exit code: $codes"
echo ""

echo "Running pandas tests in anaconda"
anaconda_versions=(anaconda2-4.1.1, anaconda3-4.1.1)
for i in ${version[@]}; do
  echo "Testing in $i"
  v="fyrd_$i"
  pyenv install -s $i
  echo "Creating virtualenv $v"
  pyenv virtualenv $i $v
  pyenv shell $v
  python ./setup.py develop >/dev/null
  echo "Installing requirements"
  pip install -r tests/test_requirements.txt >/dev/null
  pip install pandas numpy scipy >/dev/null
  echo "Running test suite"
  python tests/run_tests.py $@
  code=$!
  counter=$((counter+1))
  codes=$((codes+code))
  python tests/pandas_run.py $@
  code=$!
  counter=$((counter+1))
  codes=$((codes+code))
  echo "Deleteing $v"
  pyenv virtualenv-delete $v
done

echo "All tests complete, please review the outputs manually."
exit $codes
