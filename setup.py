#!/bin/bash
from setuptools import setup

setup(
    name='csv_converter',
    version='1.0.0',
    description='Command-line converter csv format to avro',
    author='Anastasiya Holubeva',
    packages=['converter'],
    entry_points={
        'console_scripts': [
            'csv_converter=avro.converter:main',
        ],
    },
)