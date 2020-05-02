#!/usr/bin/env python3
import os
import re

from setuptools import setup

# Leaving this here because using "attr: ..." in the config file requires the package to be
# importable, which might not be the case due to missing dependencies.
with open(os.path.join(os.path.dirname(__file__), "pyhdfs", "__init__.py")) as py:
    version_match = re.search(r'__version__ = "(.+?)"', py.read())
    assert version_match
    version = version_match.group(1)

setup(version=version)
