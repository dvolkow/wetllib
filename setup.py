#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from setuptools import setup
from wetllib.__init__ import VERSION

setup(
    name='wetllib',
    version=VERSION,
    packages=['wetllib'],
    install_requires=['datetime', 'clickhouse_driver', 'pandas',
                      'kafka-python', 'requests', 'numpy', 'ipwhois',
                      'nolds', 'hurst', 'scipy', 'neurokit'],
    url='nope',
    license='GPLv3',
    author='Daniel Wolkow',
    author_email='volkov12@rambler.ru',
    python_requires='>=3.6',
    description='ETL processes library'
)
