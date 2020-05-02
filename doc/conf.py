# Configuration file for the Sphinx documentation builder.
#
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import datetime

project = "PyHDFS"
author = "Jing Wang"
copyright = "{}, {}".format(datetime.datetime.now().year, author)

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
]

master_doc = "index"

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
