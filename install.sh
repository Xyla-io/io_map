#!/bin/bash

set -e
set -x

install_development_packages () {
  for DEVPKG in $(ls -d */)
  do
    cd $DEVPKG
    if [ -f requirements.txt ]; then
      pip install -r requirements.txt
    fi
    if [ -f setup.py ]; then
      python setup.py develop
    fi
    if [ -d development_packages ]; then
      cd development_packages
      install_development_packages
      cd ..
    fi
    cd ..
  done
}

git submodule update --init

rm -rf .venv
python3.7 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt

deactivate