==================
Python HDFS client
==================

Because the world needs `yet <https://github.com/spotify/snakebite>`_ `another <https://github.com/ProjectMeniscus/pywebhdfs>`_ `way <https://pypi.python.org/pypi/hdfs>`_ to talk to HDFS from Python.

Usage
=====

This library provides a Python client for `WebHDFS <https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html>`_.
NameNode HA is supported by passing in both NameNodes.
Responses are returned as nice Python classes, and any failed operation will raise some subclass of ``HdfsException`` matching the Java exception.

Example usage:

.. code-block:: python

    >>> fs = pyhdfs.HdfsClient(hosts='nn1.example.com:50070,nn2.example.com:50070', user_name='someone')
    >>> fs.list_status('/')
    [FileStatus(pathSuffix='benchmarks', permission='777', type='DIRECTORY', ...), FileStatus(...), ...]
    >>> fs.listdir('/')
    ['benchmarks', 'hbase', 'solr', 'tmp', 'user', 'var']
    >>> fs.mkdirs('/fruit/x/y')
    True
    >>> fs.create('/fruit/apple', 'delicious')
    >>> fs.append('/fruit/apple', ' food')
    >>> with contextlib.closing(fs.open('/fruit/apple')) as f:
    ...     f.read()
    ...
    b'delicious food'
    >>> fs.get_file_status('/fruit/apple')
    FileStatus(length=14, owner='someone', type='FILE', ...)
    >>> fs.get_file_status('/fruit/apple').owner
    'someone'
    >>> fs.get_content_summary('/fruit')
    ContentSummary(directoryCount=3, fileCount=1, length=14, quota=-1, spaceConsumed=14, spaceQuota=-1)
    >>> list(fs.walk('/fruit'))
    [('/fruit', ['x'], ['apple']), ('/fruit/x', ['y'], []), ('/fruit/x/y', [], [])]
    >>> fs.exists('/fruit/apple')
    True
    >>> fs.delete('/fruit')
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File ".../pyhdfs.py", line 525, in delete
      ...
    pyhdfs.HdfsPathIsNotEmptyDirectoryException: `/fruit is non empty': Directory is not empty
    >>> fs.delete('/fruit', recursive=True)
    True
    >>> fs.exists('/fruit/apple')
    False
    >>> issubclass(pyhdfs.HdfsFileNotFoundException, pyhdfs.HdfsIOException)
    True


You can also pass the hostname as part of the URI:

.. code-block:: python

    fs.list_status('//nn1.example.com:50070;nn2.example.com:50070/')

The methods and return values generally map directly to `WebHDFS endpoints <https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html>`_.
The client also provides convenience methods that mimic Python ``os`` methods and HDFS CLI commands (e.g. ``walk`` and ``copy_to_local``).

``pyhdfs`` logs all HDFS actions at the INFO level, so turning on INFO level logging will give you a debug record for your application.

For more information, see the `full API docs <http://pyhdfs.readthedocs.io/en/latest/>`_.

Installing
==========

``pip install pyhdfs``

Python 3 is required.

Development testing
===================

.. image:: https://github.com/jingw/pyhdfs/workflows/CI/badge.svg
    :target: https://github.com/jingw/pyhdfs/actions?query=workflow%3ACI

.. image:: http://codecov.io/github/jingw/pyhdfs/coverage.svg?branch=master
    :target: http://codecov.io/github/jingw/pyhdfs?branch=master

.. image:: https://readthedocs.org/projects/pyhdfs/badge/?version=latest
    :target: https://pyhdfs.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black

First run ``install-hdfs.sh x.y.z``, which will download, extract, and run the HDFS NN/DN processes in the current directory.
(Replace ``x.y.z`` with a real version.)
Then run the following commands.
Note they will create and delete ``hdfs://localhost/tmp/pyhdfs_test``.

Commands::

    python3 -m venv env
    source env/bin/activate
    pip install -e .
    pip install -r dev_requirements.txt
    pytest
