#!/bin/bash
# To use this code, create virtualenvs with pyenv that begin with fyrd_
# Tests will be run in all of these virtualenvs
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

versions=$(pyenv versions | grep " fyrd" | sed 's/\*//g' | sed 's/^ \+//g' | sed 's/ .*//g')

for i in ${versions[@]}; do
  echo "Testing in $i"
  pyenv shell $i
  pip uninstall -y fyrd
  pip install .
  pip install -r tests/test_requirements.txt
  if [ -e 'tests' ]; then
    python tests/run_tests.py
  else
    python run_tests.py
  fi
done
