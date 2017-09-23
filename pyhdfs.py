"""WebHDFS client with support for NN HA and automatic error checking

For details on the WebHDFS endpoints, see the Hadoop documentation:

- https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
- https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html
"""
from __future__ import absolute_import, print_function, unicode_literals
import base64
import binascii
import getpass
import io
import logging
import os
import posixpath
import random
import re
import shutil
import time
import warnings

import requests.api
import requests.exceptions
import simplejson
import simplejson.scanner

try:
    # Python 3
    import http.client as httplib
    from urllib.parse import quote as url_quote
    from urllib.parse import urlsplit
except ImportError:  # pragma: no cover
    # Python 2
    import httplib
    from urllib import quote as url_quote
    from urlparse import urlsplit

try:
    basestring
except NameError:  # pragma: no cover
    # Python 3
    basestring = str

try:
    NotADirectoryError
except NameError:  # pragma: no cover
    # Python 2
    NotADirectoryError = OSError

DEFAULT_PORT = 50070
WEBHDFS_PATH = '/webhdfs/v1'

__version__ = '0.2.1'
_logger = logging.getLogger(__name__)


class HdfsException(Exception):
    """Base class for all errors while communicating with WebHDFS server"""
    pass


class HdfsNoServerException(HdfsException):
    """The client was not able to reach any of the given servers"""
    pass


class HdfsHttpException(HdfsException):
    """The client was able to talk to the server but got a HTTP error code.

    :param message: Exception message
    :param exception: Name of the exception
    :param javaClassName: Java class name of the exception
    :param status_code: HTTP status code
    :type status_code: int
    :param kwargs: any extra attributes in case Hadoop adds more stuff
    """
    _expected_status_code = None

    def __init__(self, message, exception, status_code, **kwargs):
        assert self._expected_status_code is None or self._expected_status_code == status_code, (
            "Expected status {} for {}, got {}".format(
                self._expected_status_code, exception, status_code))
        super(HdfsHttpException, self).__init__(message)
        self.message = message
        self.exception = exception
        self.status_code = status_code
        self.__dict__.update(kwargs)


# NOTE: the following exceptions are referenced using globals() in _check_response

class HdfsIllegalArgumentException(HdfsHttpException):
    _expected_status_code = 400


class HdfsHadoopIllegalArgumentException(HdfsIllegalArgumentException):
    pass


class HdfsInvalidPathException(HdfsHadoopIllegalArgumentException):
    pass


class HdfsUnsupportedOperationException(HdfsHttpException):
    _expected_status_code = 400


class HdfsSecurityException(HdfsHttpException):
    _expected_status_code = 401


class HdfsIOException(HdfsHttpException):
    _expected_status_code = 403


class HdfsQuotaExceededException(HdfsIOException):
    pass


class HdfsNSQuotaExceededException(HdfsQuotaExceededException):
    pass


class HdfsDSQuotaExceededException(HdfsQuotaExceededException):
    pass


class HdfsAccessControlException(HdfsIOException):
    pass


class HdfsFileAlreadyExistsException(HdfsIOException):
    pass


class HdfsPathIsNotEmptyDirectoryException(HdfsIOException):
    pass


# thrown in safe mode
class HdfsRemoteException(HdfsIOException):
    pass


# thrown in startup mode
class HdfsRetriableException(HdfsIOException):
    pass


class HdfsStandbyException(HdfsIOException):
    pass


class HdfsSnapshotException(HdfsIOException):
    pass


class HdfsFileNotFoundException(HdfsIOException):
    _expected_status_code = 404


class HdfsRuntimeException(HdfsHttpException):
    _expected_status_code = 500


class _BoilerplateClass(dict):
    """Turns a dictionary into a nice looking object with a pretty repr.

    Unlike namedtuple, this class is very lenient. It will not error out when it gets extra
    attributes. This lets us tolerate new HDFS features without any code change at the expense of
    higher chance of error / more black magic.
    """
    def __init__(self, *args, **kwargs):
        super(_BoilerplateClass, self).__init__(*args, **kwargs)
        self.__dict__ = self

    def __repr__(self):
        kvs = ['{}={!r}'.format(k, v) for k, v in self.items()]
        return '{}({})'.format(self.__class__.__name__, ', '.join(kvs))

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__) and
            dict.__eq__(self, other)
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class ContentSummary(_BoilerplateClass):
    """
    :param spaceQuota: The disk space quota.
    :type spaceQuota: int
    :param fileCount: The number of files.
    :type fileCount: int
    :param quota: The namespace quota of this directory.
    :type quota: int
    :param directoryCount: The number of directories.
    :type directoryCount: int
    :param spaceConsumed: The disk space consumed by the content.
    :type spaceConsumed: int
    :param length: The number of bytes used by the content.
    :type length: int
    """


class FileChecksum(_BoilerplateClass):
    """
    :param algorithm: The name of the checksum algorithm.
    :type algorithm: str
    :param length: The length of the bytes (not the length of the string).
    :type length: int
    :param bytes: The byte sequence of the checksum in hexadecimal.
    :type bytes: str
    """


class FileStatus(_BoilerplateClass):
    """
    :param owner: The user who is the owner.
    :type owner: str
    :param modificationTime: The modification time.
    :type modificationTime: int
    :param symlink: The link target of a symlink.
    :type symlink: str
    :param childrenNum: The number of children.
    :type childrenNum: int
    :param pathSuffix: The path suffix.
    :type pathSuffix: str
    :param blockSize: The block size of a file.
    :type blockSize: int
    :param length: The number of bytes in a file.
    :type length: int
    :param replication: The number of replication of a file.
    :type replication: int
    :param permission: The permission represented as a octal string.
    :type permission: str
    :param fileId: The inode ID.
    :type fileId: int
    :param type: The type of the path object - FILE, DIRECTORY, or SYMLINK.
    :type type: str
    :param group: The group owner.
    :type group: str
    :param accessTime: The access time.
    :type accessTime: int
    """


class HdfsClient(object):
    """HDFS client backed by WebHDFS.

    All functions take arbitrary query parameters to pass to WebHDFS, in addition to any documented
    keyword arguments. In particular, any function will accept ``user.name``, which for convenience
    may be passed as ``user_name``.

    If multiple HA NameNodes are given, all functions submit HTTP requests to both NameNodes until
    they find the active NameNode.

    :param hosts: List of NameNode HTTP host:port strings, either as ``list`` or a comma separated
        string. Port defaults to 50070 if left unspecified.
    :type hosts: list or str
    :param randomize_hosts: By default randomize host selection.
    :type randomize_hosts: bool
    :param user_name: What Hadoop user to run as. Defaults to the ``HADOOP_USER_NAME`` environment
        variable if present, otherwise ``getpass.getuser()``.
    :param timeout: How long to wait on a single NameNode in seconds before moving on.
        In some cases the standby NameNode can be unresponsive (e.g. loading fsimage or
        checkpointing), so we don't want to block on it.
    :type timeout: float
    :param max_tries: How many times to retry a request for each NameNode. If NN1 is standby and NN2
        is active, we might first contact NN1 and then observe a failover to NN1 when we contact
        NN2. In this situation we want to retry against NN1.
    :type max_tries: int
    :param retry_delay: How long to wait in seconds before going through NameNodes again
    :type retry_delay: float
    :param requests_session: A ``requests.Session`` object for advanced usage. If absent, this
        class will use the default requests behavior of making a new session per HTTP request.
        Caller is responsible for closing session.
    :param requests_kwargs: Additional ``**kwargs`` to pass to requests
    """

    def __init__(self, hosts='localhost', randomize_hosts=True, user_name=None,
                 timeout=20, max_tries=2, retry_delay=5,
                 requests_session=None, requests_kwargs=None):
        """Create a new HDFS client"""
        if max_tries < 1:
            raise ValueError("Invalid max_tries: {}".format(max_tries))
        if retry_delay < 0:
            raise ValueError("Invalid retry_delay: {}".format(retry_delay))
        self.randomize_hosts = randomize_hosts
        self.hosts = self._parse_hosts(hosts)
        if not self.hosts:
            raise ValueError("No hosts given")
        if randomize_hosts:
            self.hosts = list(self.hosts)
            random.shuffle(self.hosts)
        self.timeout = timeout
        self.max_tries = max_tries
        self.retry_delay = retry_delay
        self.user_name = user_name or os.environ.get('HADOOP_USER_NAME', getpass.getuser())
        self._last_time_recorded_active = None
        self._requests_session = requests_session or requests.api
        self._requests_kwargs = requests_kwargs or {}
        for k in ('method', 'url', 'data', 'timeout', 'stream', 'params'):
            if k in self._requests_kwargs:
                raise ValueError("Cannot override requests argument {}".format(k))

    def _parse_hosts(self, hosts):
        host_list = list(hosts) if isinstance(hosts, list) else re.split(r',|;', hosts)
        for i, host in enumerate(host_list):
            if ':' not in host:
                host_list[i] = '{:s}:{:d}'.format(host, DEFAULT_PORT)
        if self.randomize_hosts:
            random.shuffle(host_list)
        return host_list

    def _parse_path(self, path):
        """Return (hosts, path) tuple"""
        # Support specifying another host via hdfs://host:port/path syntax
        # We ignore the scheme and piece together the query and fragment
        # Note that HDFS URIs are not URL encoded, so a '?' or a '#' in the URI is part of the
        # path
        parts = urlsplit(path, allow_fragments=False)
        if not parts.path.startswith('/'):
            raise ValueError("Path must be absolute, was given {}".format(path))
        if parts.scheme not in ('', 'hdfs', 'hftp', 'webhdfs'):
            warnings.warn("Unexpected scheme {}".format(parts.scheme))
        assert not parts.fragment
        path = parts.path
        if parts.query:
            path += '?' + parts.query
        if parts.netloc:
            hosts = self._parse_hosts(parts.netloc)
        else:
            hosts = self.hosts
        return hosts, path

    def _record_last_active(self, host):
        """Put host first in our host list, so we try it first next time

        The implementation of get_active_namenode relies on this reordering.
        """
        if host in self.hosts:  # this check is for when user passes a host at request time
            # Keep this thread safe: set hosts atomically and update it before the timestamp
            self.hosts = [host] + [h for h in self.hosts if h != host]
            self._last_time_recorded_active = time.time()

    def _request(self, method, path, op, expected_status=httplib.OK, **kwargs):
        """Make a WebHDFS request against the NameNodes

        This function handles NameNode failover and error checking.
        All kwargs are passed as query params to the WebHDFS server.
        """
        hosts, path = self._parse_path(path)
        _transform_user_name_key(kwargs)
        kwargs.setdefault('user.name', self.user_name)

        formatted_args = ' '.join('{}={}'.format(*t) for t in kwargs.items())
        _logger.info("%s %s %s %s", op, path, formatted_args, ','.join(hosts))
        kwargs['op'] = op
        for i in range(self.max_tries):
            log_level = logging.DEBUG if i < self.max_tries - 1 else logging.WARNING
            for host in hosts:
                try:
                    response = self._requests_session.request(
                        method,
                        'http://{}{}{}'.format(host, WEBHDFS_PATH, url_quote(path.encode('utf-8'))),
                        params=kwargs, timeout=self.timeout, allow_redirects=False,
                        **self._requests_kwargs
                    )
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                    _logger.log(log_level, "Failed to reach to %s (attempt %d/%d)",
                                host, i + 1, self.max_tries, exc_info=True)
                    continue
                try:
                    _check_response(response, expected_status)
                except (HdfsRetriableException, HdfsStandbyException):
                    _logger.log(log_level, "%s is in startup or standby mode (attempt %d/%d)",
                                host, i + 1, self.max_tries, exc_info=True)
                    continue
                # Note: standby NN can still return basic validation errors, so non-StandbyException
                # does not necessarily mean we have the active NN.
                self._record_last_active(host)
                return response
            if i != self.max_tries - 1:
                time.sleep(self.retry_delay)
        raise HdfsNoServerException("Could not use any of the given hosts")

    def _get(self, *args, **kwargs):
        return self._request('get', *args, **kwargs)

    def _put(self, *args, **kwargs):
        return self._request('put', *args, **kwargs)

    def _post(self, *args, **kwargs):
        return self._request('post', *args, **kwargs)

    def _delete(self, *args, **kwargs):
        return self._request('delete', *args, **kwargs)

    #################################
    # File and Directory Operations #
    #################################

    def create(self, path, data, **kwargs):
        """Create a file at the given path.

        :param data: ``bytes`` or a ``file``-like object to upload
        :param overwrite: If a file already exists, should it be overwritten?
        :type overwrite: bool
        :param blocksize: The block size of a file.
        :type blocksize: long
        :param replication: The number of replications of a file.
        :type replication: short
        :param permission: The permission of a file/directory. Any radix-8 integer (leading zeros
            may be omitted.)
        :type permission: octal
        :param buffersize: The size of the buffer used in transferring data.
        :type buffersize: int
        """
        metadata_response = self._put(
            path, 'CREATE', expected_status=httplib.TEMPORARY_REDIRECT, **kwargs)
        assert not metadata_response.content
        data_response = self._requests_session.put(
            metadata_response.headers['location'], data=data, **self._requests_kwargs)
        _check_response(data_response, expected_status=httplib.CREATED)
        assert not data_response.content

    def append(self, path, data, **kwargs):
        """Append to the given file.

        :param data: ``bytes`` or a ``file``-like object
        :param buffersize: The size of the buffer used in transferring data.
        :type buffersize: int
        """
        metadata_response = self._post(
            path, 'APPEND', expected_status=httplib.TEMPORARY_REDIRECT, **kwargs)
        data_response = self._requests_session.post(
            metadata_response.headers['location'], data=data, **self._requests_kwargs)
        _check_response(data_response)
        assert not data_response.content

    def concat(self, target, sources, **kwargs):
        """Concat existing files together.

        Conditions:

        - The last block in the target file (``path``) must be full.
        - All blocks must be the same size, except possibly the last block.

        :param target: the path to the target destination.
        :param sources: the paths to the sources to use for the concatenation.
        :type sources: list
        """
        if isinstance(sources, basestring):
            raise ValueError("sources should be a list")
        if any(',' in s for s in sources):
            raise NotImplementedError("WebHDFS does not support commas in concat")
        response = self._post(target, 'CONCAT', sources=','.join(sources), **kwargs)
        assert not response.content

    def open(self, path, **kwargs):
        """Return a file-like object for reading the given HDFS path.

        :param offset: The starting byte position.
        :type offset: long
        :param length: The number of bytes to be processed.
        :type length: long
        :param buffersize: The size of the buffer used in transferring data.
        :type buffersize: int
        :rtype: file-like object
        """
        metadata_response = self._get(
            path, 'OPEN', expected_status=httplib.TEMPORARY_REDIRECT, **kwargs)
        data_response = self._requests_session.get(
            metadata_response.headers['location'], stream=True, **self._requests_kwargs)
        _check_response(data_response)
        return data_response.raw

    def mkdirs(self, path, **kwargs):
        """Create a directory with the provided permission.

        The permission of the directory is set to be the provided permission as in setPermission,
        not permission&~umask.

        :param permission: The permission of a file/directory. Any radix-8 integer (leading zeros
            may be omitted.)
        :type permission: octal
        :returns: true if the directory creation succeeds; false otherwise
        :rtype: bool
        """
        return _json(self._put(path, 'MKDIRS', **kwargs))['boolean']

    def create_symlink(self, link, destination, **kwargs):
        """Create a symbolic link at ``link`` pointing to ``destination``.

        :param link: the path to be created that points to target
        :param destination: the target of the symbolic link
        :param createParent: If the parent directories do not exist, should they be created?
        :type createParent: bool
        :raises HdfsUnsupportedOperationException: This feature doesn't actually work, at least on
            CDH 5.3.0.
        """
        response = self._put(link, 'CREATESYMLINK', destination=destination, **kwargs)
        assert not response.content

    def rename(self, path, destination, **kwargs):
        """Renames Path src to Path dst.

        :returns: true if rename is successful
        :rtype: bool
        """
        return _json(self._put(path, 'RENAME', destination=destination, **kwargs))['boolean']

    def delete(self, path, **kwargs):
        """Delete a file.

        :param recursive: If path is a directory and set to true, the directory is deleted else
            throws an exception. In case of a file the recursive can be set to either true or false.
        :type recursive: bool
        :returns: true if delete is successful else false.
        :rtype: bool
        """
        return _json(self._delete(path, 'DELETE', **kwargs))['boolean']

    def get_file_status(self, path, **kwargs):
        """Return a :py:class:`FileStatus` object that represents the path."""
        return FileStatus(**_json(self._get(path, 'GETFILESTATUS', **kwargs))['FileStatus'])

    def list_status(self, path, **kwargs):
        """List the statuses of the files/directories in the given path if the path is a directory.

        :rtype: ``list`` of :py:class:`FileStatus` objects
        """
        return [
            FileStatus(**item) for item in
            _json(self._get(path, 'LISTSTATUS', **kwargs))['FileStatuses']['FileStatus']
        ]

    ################################
    # Other File System Operations #
    ################################

    def get_content_summary(self, path, **kwargs):
        """Return the :py:class:`ContentSummary` of a given Path."""
        return ContentSummary(
            **_json(self._get(path, 'GETCONTENTSUMMARY', **kwargs))['ContentSummary'])

    def get_file_checksum(self, path, **kwargs):
        """Get the checksum of a file.

        :rtype: :py:class:`FileChecksum`
        """
        metadata_response = self._get(
            path, 'GETFILECHECKSUM', expected_status=httplib.TEMPORARY_REDIRECT, **kwargs)
        assert not metadata_response.content
        data_response = self._requests_session.get(
            metadata_response.headers['location'], **self._requests_kwargs)
        _check_response(data_response)
        return FileChecksum(**_json(data_response)['FileChecksum'])

    def get_home_directory(self, **kwargs):
        """Return the current user's home directory in this filesystem."""
        return _json(self._get('/', 'GETHOMEDIRECTORY', **kwargs))['Path']

    def set_permission(self, path, **kwargs):
        """Set permission of a path.

        :param permission: The permission of a file/directory. Any radix-8 integer (leading zeros
            may be omitted.)
        :type permission: octal
        """
        response = self._put(path, 'SETPERMISSION', **kwargs)
        assert not response.content

    def set_owner(self, path, **kwargs):
        """Set owner of a path (i.e. a file or a directory).

        The parameters owner and group cannot both be null.

        :param owner: user
        :param group: group
        """
        response = self._put(path, 'SETOWNER', **kwargs)
        assert not response.content

    def set_replication(self, path, **kwargs):
        """Set replication for an existing file.

        :param replication: new replication
        :type replication: short
        :returns: true if successful; false if file does not exist or is a directory
        :rtype: bool
        """
        return _json(self._put(path, 'SETREPLICATION', **kwargs))['boolean']

    def set_times(self, path, **kwargs):
        """Set access time of a file.

        :param modificationtime: Set the modification time of this file. The number of milliseconds
            since Jan 1, 1970.
        :type modificationtime: long
        :param accesstime: Set the access time of this file. The number of milliseconds since Jan 1
            1970.
        :type accesstime: long
        """
        response = self._put(path, 'SETTIMES', **kwargs)
        assert not response.content

    ##########################################
    # Extended Attributes(XAttrs) Operations #
    ##########################################

    def set_xattr(self, path, xattr_name, xattr_value, flag, **kwargs):
        """Set an xattr of a file or directory.

        :param xattr_name: The name must be prefixed with the namespace followed by ``.``. For
            example, ``user.attr``.
        :param flag: ``CREATE`` or ``REPLACE``
        """
        kwargs['xattr.name'] = xattr_name
        kwargs['xattr.value'] = xattr_value
        response = self._put(path, 'SETXATTR', flag=flag, **kwargs)
        assert not response.content

    def remove_xattr(self, path, xattr_name, **kwargs):
        """Remove an xattr of a file or directory."""
        kwargs['xattr.name'] = xattr_name
        response = self._put(path, 'REMOVEXATTR', **kwargs)
        assert not response.content

    def get_xattrs(self, path, xattr_name=None, encoding='text', **kwargs):
        """Get one or more xattr values for a file or directory.

        :param xattr_name: ``str`` to get one attribute, ``list`` to get multiple attributes,
            ``None`` to get all attributes.
        :param encoding: ``text`` | ``hex`` | ``base64``, defaults to ``text``

        :returns: Dictionary mapping xattr name to value. With text encoding, the value will be a
            unicode string. With hex or base64 encoding, the value will be a byte array.
        :rtype: dict
        """
        kwargs['xattr.name'] = xattr_name
        json = _json(self._get(path, 'GETXATTRS', encoding=encoding, **kwargs))['XAttrs']
        # Decode the result
        result = {}
        for attr in json:
            k = attr['name']
            v = attr['value']
            if v is None:
                result[k] = None
            elif encoding == 'text':
                assert attr['value'].startswith('"') and attr['value'].endswith('"')
                result[k] = v[1:-1]
            elif encoding == 'hex':
                assert attr['value'].startswith('0x')
                # older python demands bytes, so we have to ascii encode
                result[k] = binascii.unhexlify(v[2:].encode('ascii'))
            elif encoding == 'base64':
                assert attr['value'].startswith('0s')
                # older python demands bytes, so we have to ascii encode
                result[k] = base64.b64decode(v[2:].encode('ascii'))
            else:
                warnings.warn("Unexpected encoding {}".format(encoding))
                result[k] = v
        return result

    def list_xattrs(self, path, **kwargs):
        """Get all of the xattr names for a file or directory.

        :rtype: list
        """
        return simplejson.loads(_json(self._get(path, 'LISTXATTRS', **kwargs))['XAttrNames'])

    #######################
    # Snapshot Operations #
    #######################

    def create_snapshot(self, path, **kwargs):
        """Create a snapshot

        :param path: The directory where snapshots will be taken
        :param snapshotname: The name of the snapshot
        :returns: the snapshot path
        """
        return _json(self._put(path, 'CREATESNAPSHOT', **kwargs))['Path']

    def delete_snapshot(self, path, snapshotname, **kwargs):
        """Delete a snapshot of a directory"""
        response = self._delete(path, 'DELETESNAPSHOT', snapshotname=snapshotname, **kwargs)
        assert not response.content

    def rename_snapshot(self, path, oldsnapshotname, snapshotname, **kwargs):
        """Rename a snapshot"""
        response = self._put(path, 'RENAMESNAPSHOT',
                             oldsnapshotname=oldsnapshotname, snapshotname=snapshotname, **kwargs)
        assert not response.content

    ######################################################
    # Convenience Methods                                #
    # These are intended to mimic python / hdfs features #
    ######################################################

    def listdir(self, path, **kwargs):
        """Return a list containing names of files in the given path"""
        statuses = self.list_status(path, **kwargs)
        if len(statuses) == 1 and statuses[0].pathSuffix == '' and statuses[0].type == 'FILE':
            raise NotADirectoryError('Not a directory: {!r}'.format(path))
        return [f.pathSuffix for f in statuses]

    def exists(self, path, **kwargs):
        """Return true if the given path exists"""
        try:
            self.get_file_status(path, **kwargs)
            return True
        except HdfsFileNotFoundException:
            return False

    def walk(self, top, topdown=True, onerror=None, **kwargs):
        """See ``os.walk`` for documentation"""
        try:
            listing = self.list_status(top, **kwargs)
        except HdfsException as e:
            if onerror is not None:
                onerror(e)
            return

        dirnames, filenames = [], []
        for f in listing:
            if f.type == 'DIRECTORY':
                dirnames.append(f.pathSuffix)
            elif f.type == 'FILE':
                filenames.append(f.pathSuffix)
            else:  # pragma: no cover
                raise AssertionError("Unexpected type {}".format(f.type))

        if topdown:
            yield top, dirnames, filenames
        for name in dirnames:
            new_path = posixpath.join(top, name)
            for x in self.walk(new_path, topdown, onerror, **kwargs):
                yield x
        if not topdown:
            yield top, dirnames, filenames

    def copy_from_local(self, localsrc, dest, **kwargs):
        """Copy a single file from the local file system to ``dest``

        Takes all arguments that :py:meth:`create` takes.
        """
        with io.open(localsrc, 'rb') as f:
            self.create(dest, f, **kwargs)

    def copy_to_local(self, src, localdest, **kwargs):
        """Copy a single file from ``src`` to the local file system

        Takes all arguments that :py:meth:`open` takes.
        """
        with self.open(src, **kwargs) as fsrc:
            with io.open(localdest, 'wb') as fdst:
                shutil.copyfileobj(fsrc, fdst)

    def get_active_namenode(self, max_staleness=None):
        """Return the address of the currently active NameNode.

        :param max_staleness: This function caches the active NameNode. If this age of this cached
            result is less than ``max_staleness`` seconds, return it. Otherwise, or if this
            parameter is None, do a lookup.
        :type max_staleness: float
        :raises HdfsNoServerException: can't find an active NameNode
        """
        if (max_staleness is None or
                self._last_time_recorded_active is None or
                self._last_time_recorded_active < time.time() - max_staleness):
            # Make a cheap request and rely on the reordering in self._record_last_active
            self.get_file_status('/')
        return self.hosts[0]


def _transform_user_name_key(kw_dict):
    """Convert user_name to user.name for convenience with python kwargs"""
    if 'user_name' in kw_dict:
        kw_dict['user.name'] = kw_dict['user_name']
        del kw_dict['user_name']


def _json(response):
    try:
        return response.json()
    except simplejson.scanner.JSONDecodeError:
        raise HdfsException(
            "Expected JSON. Is WebHDFS enabled? Got {!r}".format(response.text))


def _check_response(response, expected_status=httplib.OK):
    if response.status_code == expected_status:
        return
    remote_exception = _json(response)['RemoteException']
    exception_name = remote_exception['exception']
    python_name = 'Hdfs' + exception_name
    # Sanity check that we'll be constructing one of the Hdfs[...]Exception classes
    if exception_name.endswith('Exception') and python_name in globals():
        cls = globals()[python_name]
    else:
        cls = HdfsHttpException
        # prefix the message with the exception name since we're not using a fancy class
        remote_exception['message'] = exception_name + ' - ' + remote_exception['message']
    raise cls(status_code=response.status_code, **remote_exception)
