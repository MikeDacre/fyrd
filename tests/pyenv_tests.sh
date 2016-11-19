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
if [[ $? > 0 ]]; then
  echo "Cannot load pyenv"
  exit 5
fi
eval "$(pyenv virtualenv-init -)"
if [[ $? > 0 ]]; then
  echo "Cannot load pyenv virtualenv"
  exit 50
fi


# Versions to test
if [[ $1 == '--limited' ]]; then
  versions=('2.7.10' '3.3.0' '3.5.2')
else
  versions=('2.7.10' '2.7.11' '2.7.12' '3.3.0' '3.4.0' '3.5.2' '3.6-dev' '3.7-dev')
fi
anaconda_versions=(anaconda2-4.1.1, anaconda3-4.1.1)
build_string="fyrd_$(cat /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z0-9' | fold -w 8 | head -n 1)"

# Delete
function on_exit() {
  echo "Making sure virtual envs are gone"
  for i in ${versions[@]}; do
    v="${build_string}_${i}"
    echo "Deleting ${v}"
    pyenv virtualenv-delete --force $v >/dev/null 2>/dev/null
  done
  for i in ${anaconda_versions[@]}; do
    v="${build_string}_${i}"
    echo "Deleting ${v}"
    pyenv virtualenv-delete --force $v >/dev/null 2>/dev/null
  done
}
trap on_exit EXIT

counter=0
aborted=0
codes=0
for i in ${versions[@]}; do
  echo "Testing in $i"
  v="${build_string}_${i}"
  pyenv install -s $i
  if [[ $? > 0 ]]; then
    aborted=$((aborted+1))
    continue
  fi
  echo "Creating virtualenv $v"
  pyenv virtualenv-delete $v >/dev/null 2>/dev/null
  pyenv virtualenv --force $i $v
  if [[ $? > 0 ]]; then
    aborted=$((aborted+1))
    continue
  fi
  pyenv shell $v
  echo "Installing fyrd"
  python ./setup.py install >/dev/null
  if [[ $? > 0 ]]; then
    aborted=$((aborted+1))
    continue
  fi
  echo "Installing requirements"
  pip install -r tests/test_requirements.txt >/dev/null
  if [[ $? > 0 ]]; then
    aborted=$((aborted+1))
    continue
  fi
  echo "Running test suite"
  python tests/run_tests.py $@
  code=$?
  counter=$((counter+1))
  codes=$((codes+code))
  echo "Deleteing $v"
  pyenv virtualenv-delete -f $v
done

echo "Completed main tests."
echo ""

echo "Running pandas tests in anaconda"
for i in ${version[@]}; do
  echo "Testing in $i"
  pyenv install -s $i
  if [[ $? > 0 ]]; then
    aborted=$((aborted+1))
    continue
  fi
  v="${build_string}_${i}"
  echo "Creating virtualenv $v"
  pyenv virtualenv-delete $v >/dev/null 2>/dev/null
  pyenv virtualenv --force $i $v
  if [[ $? > 0 ]]; then
    aborted=$((aborted+1))
    continue
  fi
  pyenv shell $v
  if [[ $? > 0 ]]; then
    aborted=$((aborted+1))
    continue
  fi
  python ./setup.py develop >/dev/null
  if [[ $? > 0 ]]; then
    aborted=$((aborted+1))
    continue
  fi
  echo "Installing requirements"
  pip install -r tests/test_requirements.txt >/dev/null
  if [[ $? > 0 ]]; then
    aborted=$((aborted+1))
    continue
  fi
  pip install pandas numpy scipy >/dev/null
  if [[ $? > 0 ]]; then
    aborted=$((aborted+1))
    continue
  fi
  echo "Running test suite"
  python tests/run_tests.py $@
  code=$?
  counter=$((counter+1))
  codes=$((codes+code))
  python tests/pandas_run.py $@
  code=$?
  counter=$((counter+1))
  codes=$((codes+code))
  echo "Deleteing $v"
  pyenv virtualenv-delete $v
done

echo "Completed pandas tests."
echo ""
echo "All tests complete."
echo "Ran $counter, aborted $aborted, total exit code: $codes"
echo ""
echo "Please review the outputs manually."
exit $codes
