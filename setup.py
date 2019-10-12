#!/usr/bin/env python3
import os
import re

from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as readme:
    long_description = readme.read()
with open(os.path.join(os.path.dirname(__file__), 'pyhdfs.py')) as py:
    version_match = re.search(r"__version__ = '(.+?)'", py.read())
    assert version_match
    version = version_match.group(1)
with open(os.path.join(os.path.dirname(__file__), 'dev_requirements.txt')) as dev_requirements:
    tests_require = dev_requirements.read().splitlines()

setup(
    name="PyHDFS",
    version=version,
    description="Pure Python HDFS client",
    long_description=long_description,
    url='https://github.com/jingw/pyhdfs',
    author="Jing Wang",
    author_email="99jingw@gmail.com",
    license="MIT License",
    py_modules=['pyhdfs'],
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: System :: Filesystems",
    ],
    install_requires=[
        'requests',
        'simplejson',
    ],
    tests_require=tests_require,
    package_data={
        '': ['*.rst'],
    },
)
