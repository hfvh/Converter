#!/bin/bash
mkdir homework
cd homework
pip install avro-python3
git clone https://github.com/hfvh/Converter.git
cd Converter
python3 -m pip install --user --upgrade setuptools wheel
python setup.py sdist bdist_wheel
cd dist
pip install converter
