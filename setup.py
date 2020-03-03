#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name='aggregate-s3-logs',
    version='0.0.1',
    classifiers=[
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    packages=find_packages(exclude=['doc', 'tests*']),
    install_requires=[
        'boto3',
    ],
    entry_points={
        'console_scripts': [
            'aggregate_s3_logs=aggregate_s3_logs:aggregate_s3_logs_main',
        ],
    })

