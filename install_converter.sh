#!/bin/bash
mkdir homework
cd homework
pip install avro-python3 hdfs
git clone https://github.com/hfvh/Converter.git
cd Converter
python3 -m pip install --user --upgrade setuptools wheel
python setup.py sdist bdist_wheel
cd dist
pip install converter-1.5.0.tar.gz
