#!/bin/bash
mkdir homework
cd homework
sudo yum install centos-release-scl
sudo yum install rh-python36
scl enable rh-python36 bash
pip install avro-python3
git clone https://github.com/hfvh/Converter.git
cd Converter
python3 -m pip install --user --upgrade setuptools wheel
python setup.py sdist bdist_wheel
cd dist
pip install converter
