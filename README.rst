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

You'll need Python 2.7 or Python 3.

Development testing
===================

.. image:: https://travis-ci.org/jingw/pyhdfs.svg?branch=master
    :target: https://travis-ci.org/jingw/pyhdfs

.. image:: http://codecov.io/github/jingw/pyhdfs/coverage.svg?branch=master
    :target: http://codecov.io/github/jingw/pyhdfs?branch=master

First get an environment with HDFS.
The `Cloudera QuickStart VM <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cloudera_quickstart_vm.html>`_ works fine for this.
(Note that the VM only comes with Python 2.6, so you might want to use your host and forward port 50070.)

WARNING: The tests create and delete ``hdfs://localhost/tmp/pyhdfs_test``.

Python 3::

    virtualenv3 --no-site-packages env3
    source env3/bin/activate
    pip3 install -e .
    pip3 install -r dev_requirements.txt
    py.test

And again for Python 2 (after ``deactivate``)::

    virtualenv2 --no-site-packages env2
    source env2/bin/activate
    pip2 install -e .
    pip2 install -r dev_requirements.txt
    py.test
