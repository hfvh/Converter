#!/bin/bash
from setuptools import setup

setup(
    name='converter',
    version='1.5.0',
    description='Command-line converter csv format to avro',
    author='Anastasiya Holubeva',
    packages=['converter'],
    entry_points={
        'console_scripts': [
            'converter=converter.main:main',
        ],
    },
)