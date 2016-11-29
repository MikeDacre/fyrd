#!/bin/bash
# Run a complete test series in an anaconda environement and generate coverage
# This is based used on both torque and slurm followed by coverage combine
# to combine the results.
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
 
# Starting string for virtualenvs
v="fyrd_$(cat /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z0-9' | fold -w 8 | head -n 1)_conda"

# Delete virtualenvs on exit
function on_exit() {
  pyenv virtualenv-delete --force $v >/dev/null 2>/dev/null
}
trap on_exit EXIT
 
pyenv virtualenv-delete -f $v >/dev/null 2>/dev/null
pyenv virtualenv --force anaconda3-4.1.1 $v
pyenv shell $v
python ./setup.py develop >/dev/null
echo "Installing requirements"
pip install -r tests/test_requirements.txt >/dev/null
pip install pandas numpy scipy >/dev/null
# Actually run tests here
echo "Running test suite"
python tests/run_tests.py --coverage
pyenv virtualenv-delete -f $v >/dev/null 2>/dev/null
