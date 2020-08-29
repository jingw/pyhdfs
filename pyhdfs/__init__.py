"""WebHDFS client with support for NN HA and automatic error checking

For details on the WebHDFS endpoints, see the Hadoop documentation:

- https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
- https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html
- https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/filesystem.html
"""  # noqa: E501
import base64
import binascii
import getpass
import logging
import os
import posixpath
import random
import re
import shutil
import time
import warnings
from http import HTTPStatus
from typing import Any
from typing import Callable
from typing import Dict
from typing import IO
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import Union
from typing import cast
from urllib.parse import quote as url_quote
from urllib.parse import urlsplit

import requests.api
import requests.exceptions
import simplejson
import simplejson.scanner

DEFAULT_PORT = 50070
WEBHDFS_PATH = "/webhdfs/v1"

__version__ = "0.3.1"
_logger = logging.getLogger(__name__)

_PossibleArgumentTypes = Union[str, int, None, List[str]]


class HdfsException(Exception):
    """Base class for all errors while communicating with WebHDFS server"""


class HdfsNoServerException(HdfsException):
    """The client was not able to reach any of the given servers"""


class HdfsHttpException(HdfsException):
    """The client was able to talk to the server but got a HTTP error code.

    :param message: Exception message
    :param exception: Name of the exception
    :param javaClassName: Java class name of the exception
    :param status_code: HTTP status code
    :type status_code: int
    :param kwargs: any extra attributes in case Hadoop adds more stuff
    """

    _expected_status_code: Optional[int] = None

    def __init__(
        self, message: str, exception: str, status_code: int, **kwargs: object
    ) -> None:
        assert (
            self._expected_status_code is None
            or self._expected_status_code == status_code
        ), "Expected status {} for {}, got {}".format(
            self._expected_status_code, exception, status_code
        )
        super().__init__(message)
        self.message = message
        self.exception = exception
        self.status_code = status_code
        self.__dict__.update(kwargs)


# NOTE: the following exceptions are referenced using globals() to build _EXCEPTION_CLASSES


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


_EXCEPTION_CLASSES: Dict[str, Type[HdfsHttpException]] = {
    name: member
    for name, member in globals().items()
    if isinstance(member, type) and issubclass(member, HdfsHttpException)
}


class _BoilerplateClass(Dict[str, object]):
    """Turns a dictionary into a nice looking object with a pretty repr.

    Unlike namedtuple, this class is very lenient. It will not error out when it gets extra
    attributes. This lets us tolerate new HDFS features without any code change at the expense of
    higher chance of error / more black magic.
    """

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.__dict__ = self

    def __repr__(self) -> str:
        kvs = ["{}={!r}".format(k, v) for k, v in self.items()]
        return "{}({})".format(self.__class__.__name__, ", ".join(kvs))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, self.__class__) and dict.__eq__(self, other)

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)


class TypeQuota(_BoilerplateClass):
    """
    :param consumed: The storage type space consumed.
    :type consumed: int
    :param quota: The storage type quota.
    :type quota: int
    """

    consumed: int
    quota: int


class ContentSummary(_BoilerplateClass):
    """
    :param directoryCount: The number of directories.
    :type directoryCount: int
    :param fileCount: The number of files.
    :type fileCount: int
    :param length: The number of bytes used by the content.
    :type length: int
    :param quota: The namespace quota of this directory.
    :type quota: int
    :param spaceConsumed: The disk space consumed by the content.
    :type spaceConsumed: int
    :param spaceQuota: The disk space quota.
    :type spaceQuota: int
    :param typeQuota: Quota usage for ARCHIVE, DISK, SSD
    :type typeQuota: Dict[str, TypeQuota]
    """

    directoryCount: int
    fileCount: int
    length: int
    quota: int
    spaceConsumed: int
    spaceQuota: int
    typeQuota: Dict[str, TypeQuota]


class FileChecksum(_BoilerplateClass):
    """
    :param algorithm: The name of the checksum algorithm.
    :type algorithm: str
    :param bytes: The byte sequence of the checksum in hexadecimal.
    :type bytes: str
    :param length: The length of the bytes (not the length of the string).
    :type length: int
    """

    algorithm: str
    bytes: str
    length: int


class FileStatus(_BoilerplateClass):
    """
    :param accessTime: The access time.
    :type accessTime: int
    :param blockSize: The block size of a file.
    :type blockSize: int
    :param group: The group owner.
    :type group: str
    :param length: The number of bytes in a file.
    :type length: int
    :param modificationTime: The modification time.
    :type modificationTime: int
    :param owner: The user who is the owner.
    :type owner: str
    :param pathSuffix: The path suffix.
    :type pathSuffix: str
    :param permission: The permission represented as a octal string.
    :type permission: str
    :param replication: The number of replication of a file.
    :type replication: int
    :param symlink: The link target of a symlink.
    :type symlink: Optional[str]
    :param type: The type of the path object.
    :type type: str
    :param childrenNum: How many children this directory has, or 0 for files.
    :type childrenNum: int
    """

    accessTime: int
    blockSize: int
    group: str
    length: int
    modificationTime: int
    owner: str
    pathSuffix: str
    permission: str
    replication: int
    symlink: Optional[str]
    type: str
    childrenNum: int


class HdfsClient(object):
    """HDFS client backed by WebHDFS.

    All functions take arbitrary query parameters to pass to WebHDFS, in addition to any documented
    keyword arguments. In particular, any function will accept ``user.name``, which for convenience
    may be passed as ``user_name``.

    If multiple HA NameNodes are given, all functions submit HTTP requests to both NameNodes until
    they find the active NameNode.

    :param hosts: List of NameNode HTTP host:port strings, either as ``list`` or a comma separated
        string. Port defaults to 50070 if left unspecified. Note that in Hadoop 3, the default
        NameNode HTTP port changed to 9870; the old default of 50070 is left as-is for backwards
        compatibility.
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

    def __init__(
        self,
        hosts: Union[str, Iterable[str]] = "localhost",
        randomize_hosts: bool = True,
        user_name: Optional[str] = None,
        timeout: float = 20,
        max_tries: int = 2,
        retry_delay: float = 5,
        requests_session: Optional[requests.Session] = None,
        requests_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
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
        self.user_name = user_name or os.environ.get(
            "HADOOP_USER_NAME", getpass.getuser()
        )
        self._last_time_recorded_active: Optional[float] = None
        self._requests_session = requests_session or cast(
            requests.Session, requests.api
        )
        self._requests_kwargs = requests_kwargs or {}
        for k in ("method", "url", "data", "timeout", "stream", "params"):
            if k in self._requests_kwargs:
                raise ValueError("Cannot override requests argument {}".format(k))

    def _parse_hosts(self, hosts: Union[str, Iterable[str]]) -> List[str]:
        host_list = re.split(r",|;", hosts) if isinstance(hosts, str) else list(hosts)
        for i, host in enumerate(host_list):
            if ":" not in host:
                host_list[i] = "{:s}:{:d}".format(host, DEFAULT_PORT)
        if self.randomize_hosts:
            random.shuffle(host_list)
        return host_list

    def _parse_path(self, path: str) -> Tuple[List[str], str]:
        """Return (hosts, path) tuple"""
        # Support specifying another host via hdfs://host:port/path syntax
        # We ignore the scheme and piece together the query and fragment
        # Note that HDFS URIs are not URL encoded, so a '?' or a '#' in the URI is part of the
        # path
        parts = urlsplit(path, allow_fragments=False)
        if not parts.path.startswith("/"):
            raise ValueError("Path must be absolute, was given {}".format(path))
        if parts.scheme not in ("", "hdfs", "hftp", "webhdfs"):
            warnings.warn("Unexpected scheme {}".format(parts.scheme))
        assert not parts.fragment
        path = parts.path
        if parts.query:
            path += "?" + parts.query
        if parts.netloc:
            hosts = self._parse_hosts(parts.netloc)
        else:
            hosts = self.hosts
        return hosts, path

    def _record_last_active(self, host: str) -> None:
        """Put host first in our host list, so we try it first next time

        The implementation of get_active_namenode relies on this reordering.
        """
        # this check is for when user passes a host at request time
        if host in self.hosts:
            # Keep this thread safe: set hosts atomically and update it before the timestamp
            self.hosts = [host] + [h for h in self.hosts if h != host]
            self._last_time_recorded_active = time.time()

    def _request(
        self,
        method: str,
        path: str,
        op: str,
        expected_status: HTTPStatus,
        **kwargs: _PossibleArgumentTypes,
    ) -> requests.Response:
        """Make a WebHDFS request against the NameNodes

        This function handles NameNode failover and error checking.
        All kwargs are passed as query params to the WebHDFS server.
        """
        hosts, path = self._parse_path(path)
        _transform_user_name_key(kwargs)
        kwargs.setdefault("user.name", self.user_name)

        formatted_args = " ".join("{}={}".format(*t) for t in kwargs.items())
        _logger.info("%s %s %s %s", op, path, formatted_args, ",".join(hosts))
        kwargs["op"] = op
        for i in range(self.max_tries):
            log_level = logging.DEBUG if i < self.max_tries - 1 else logging.WARNING
            for host in hosts:
                try:
                    response = self._requests_session.request(
                        method,
                        "http://{}{}{}".format(
                            host, WEBHDFS_PATH, url_quote(path.encode("utf-8"))
                        ),
                        params=kwargs,  # type: ignore
                        timeout=self.timeout,
                        allow_redirects=False,
                        **self._requests_kwargs,
                    )
                except (
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                ):
                    _logger.log(
                        log_level,
                        "Failed to reach to %s (attempt %d/%d)",
                        host,
                        i + 1,
                        self.max_tries,
                        exc_info=True,
                    )
                    continue
                try:
                    _check_response(response, expected_status)
                except (HdfsRetriableException, HdfsStandbyException):
                    _logger.log(
                        log_level,
                        "%s is in startup or standby mode (attempt %d/%d)",
                        host,
                        i + 1,
                        self.max_tries,
                        exc_info=True,
                    )
                    continue
                # Note: standby NN can still return basic validation errors, so non-StandbyException
                # does not necessarily mean we have the active NN.
                self._record_last_active(host)
                return response
            if i != self.max_tries - 1:
                time.sleep(self.retry_delay)
        raise HdfsNoServerException("Could not use any of the given hosts")

    def _get(
        self,
        path: str,
        op: str,
        expected_status: Any = HTTPStatus.OK,
        **kwargs: _PossibleArgumentTypes,
    ) -> requests.Response:
        return self._request("get", path, op, expected_status, **kwargs)

    def _put(
        self,
        path: str,
        op: str,
        expected_status: Any = HTTPStatus.OK,
        **kwargs: _PossibleArgumentTypes,
    ) -> requests.Response:
        return self._request("put", path, op, expected_status, **kwargs)

    def _post(
        self,
        path: str,
        op: str,
        expected_status: Any = HTTPStatus.OK,
        **kwargs: _PossibleArgumentTypes,
    ) -> requests.Response:
        return self._request("post", path, op, expected_status, **kwargs)

    def _delete(
        self,
        path: str,
        op: str,
        expected_status: Any = HTTPStatus.OK,
        **kwargs: _PossibleArgumentTypes,
    ) -> requests.Response:
        return self._request("delete", path, op, expected_status, **kwargs)

    #################################
    # File and Directory Operations #
    #################################

    def create(
        self,
        path: str,
        data: Union[IO[bytes], bytes],
        **kwargs: _PossibleArgumentTypes,
    ) -> None:
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
            path, "CREATE", expected_status=HTTPStatus.TEMPORARY_REDIRECT, **kwargs
        )
        assert not metadata_response.content
        data_response = self._requests_session.put(
            metadata_response.headers["location"], data=data, **self._requests_kwargs
        )
        _check_response(data_response, expected_status=HTTPStatus.CREATED)
        assert not data_response.content

    def append(
        self,
        path: str,
        data: Union[bytes, IO[bytes]],
        **kwargs: _PossibleArgumentTypes,
    ) -> None:
        """Append to the given file.

        :param data: ``bytes`` or a ``file``-like object
        :param buffersize: The size of the buffer used in transferring data.
        :type buffersize: int
        """
        metadata_response = self._post(
            path, "APPEND", expected_status=HTTPStatus.TEMPORARY_REDIRECT, **kwargs
        )
        data_response = self._requests_session.post(
            metadata_response.headers["location"], data=data, **self._requests_kwargs
        )
        _check_response(data_response)
        assert not data_response.content

    def concat(
        self, target: str, sources: List[str], **kwargs: _PossibleArgumentTypes
    ) -> None:
        """Concat existing files together.

        For preconditions, see
        https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/filesystem.html#void_concatPath_p_Path_sources

        :param target: the path to the target destination.
        :param sources: the paths to the sources to use for the concatenation.
        :type sources: list
        """
        if not isinstance(sources, list):
            raise ValueError("sources should be a list")
        if any("," in s for s in sources):
            raise NotImplementedError("WebHDFS does not support commas in concat")
        response = self._post(target, "CONCAT", sources=",".join(sources), **kwargs)
        assert not response.content

    def open(self, path: str, **kwargs: _PossibleArgumentTypes) -> IO[bytes]:
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
            path, "OPEN", expected_status=HTTPStatus.TEMPORARY_REDIRECT, **kwargs
        )
        data_response = self._requests_session.get(
            metadata_response.headers["location"], stream=True, **self._requests_kwargs
        )
        _check_response(data_response)
        return data_response.raw  # type: ignore

    def mkdirs(self, path: str, **kwargs: _PossibleArgumentTypes) -> bool:
        """Create a directory with the provided permission.

        The permission of the directory is set to be the provided permission as in setPermission,
        not permission&~umask.

        :param permission: The permission of a file/directory. Any radix-8 integer (leading zeros
            may be omitted.)
        :type permission: octal
        :returns: true if the directory creation succeeds; false otherwise
        :rtype: bool
        """
        response = _json(self._put(path, "MKDIRS", **kwargs))["boolean"]
        assert isinstance(response, bool), type(response)
        return response

    def create_symlink(
        self, link: str, destination: str, **kwargs: _PossibleArgumentTypes
    ) -> None:
        """Create a symbolic link at ``link`` pointing to ``destination``.

        :param link: the path to be created that points to target
        :param destination: the target of the symbolic link
        :param createParent: If the parent directories do not exist, should they be created?
        :type createParent: bool
        :raises HdfsUnsupportedOperationException: This feature doesn't actually work, at least on
            CDH 5.3.0.
        """
        response = self._put(link, "CREATESYMLINK", destination=destination, **kwargs)
        assert not response.content

    def rename(
        self, path: str, destination: str, **kwargs: _PossibleArgumentTypes
    ) -> bool:
        """Renames Path src to Path dst.

        :returns: true if rename is successful
        :rtype: bool
        """
        response = _json(self._put(path, "RENAME", destination=destination, **kwargs))[
            "boolean"
        ]
        assert isinstance(response, bool), type(response)
        return response

    def delete(self, path: str, **kwargs: _PossibleArgumentTypes) -> bool:
        """Delete a file.

        :param recursive: If path is a directory and set to true, the directory is deleted else
            throws an exception. In case of a file the recursive can be set to either true or false.
        :type recursive: bool
        :returns: true if delete is successful else false.
        :rtype: bool
        """
        response = _json(self._delete(path, "DELETE", **kwargs))["boolean"]
        assert isinstance(response, bool), type(response)
        return response

    def get_file_status(
        self, path: str, **kwargs: _PossibleArgumentTypes
    ) -> FileStatus:
        """Return a :py:class:`FileStatus` object that represents the path."""
        return FileStatus(
            **_json(self._get(path, "GETFILESTATUS", **kwargs))["FileStatus"]
        )

    def list_status(
        self, path: str, **kwargs: _PossibleArgumentTypes
    ) -> List[FileStatus]:
        """List the statuses of the files/directories in the given path if the path is a directory.

        :rtype: ``list`` of :py:class:`FileStatus` objects
        """
        return [
            FileStatus(**item)
            for item in _json(self._get(path, "LISTSTATUS", **kwargs))["FileStatuses"][
                "FileStatus"
            ]
        ]

    ################################
    # Other File System Operations #
    ################################

    def get_content_summary(
        self, path: str, **kwargs: _PossibleArgumentTypes
    ) -> ContentSummary:
        """Return the :py:class:`ContentSummary` of a given Path."""
        data = _json(self._get(path, "GETCONTENTSUMMARY", **kwargs))["ContentSummary"]
        if "typeQuota" in data:
            data["typeQuota"] = {
                k: TypeQuota(**v) for k, v in data["typeQuota"].items()
            }
        return ContentSummary(**data)

    def get_file_checksum(
        self, path: str, **kwargs: _PossibleArgumentTypes
    ) -> FileChecksum:
        """Get the checksum of a file.

        :rtype: :py:class:`FileChecksum`
        """
        metadata_response = self._get(
            path,
            "GETFILECHECKSUM",
            expected_status=HTTPStatus.TEMPORARY_REDIRECT,
            **kwargs,
        )
        assert not metadata_response.content
        data_response = self._requests_session.get(
            metadata_response.headers["location"], **self._requests_kwargs
        )
        _check_response(data_response)
        return FileChecksum(**_json(data_response)["FileChecksum"])

    def get_home_directory(self, **kwargs: _PossibleArgumentTypes) -> str:
        """Return the current user's home directory in this filesystem."""
        response = _json(self._get("/", "GETHOMEDIRECTORY", **kwargs))["Path"]
        assert isinstance(response, str), type(response)
        return response

    def set_permission(self, path: str, **kwargs: _PossibleArgumentTypes) -> None:
        """Set permission of a path.

        :param permission: The permission of a file/directory. Any radix-8 integer (leading zeros
            may be omitted.)
        :type permission: octal
        """
        response = self._put(path, "SETPERMISSION", **kwargs)
        assert not response.content

    def set_owner(self, path: str, **kwargs: _PossibleArgumentTypes) -> None:
        """Set owner of a path (i.e. a file or a directory).

        The parameters owner and group cannot both be null.

        :param owner: user
        :param group: group
        """
        response = self._put(path, "SETOWNER", **kwargs)
        assert not response.content

    def set_replication(self, path: str, **kwargs: _PossibleArgumentTypes) -> bool:
        """Set replication for an existing file.

        :param replication: new replication
        :type replication: short
        :returns: true if successful; false if file does not exist or is a directory
        :rtype: bool
        """
        response = _json(self._put(path, "SETREPLICATION", **kwargs))["boolean"]
        assert isinstance(response, bool), type(response)
        return response

    def set_times(self, path: str, **kwargs: _PossibleArgumentTypes) -> None:
        """Set access time of a file.

        :param modificationtime: Set the modification time of this file. The number of milliseconds
            since Jan 1, 1970.
        :type modificationtime: long
        :param accesstime: Set the access time of this file. The number of milliseconds since Jan 1
            1970.
        :type accesstime: long
        """
        response = self._put(path, "SETTIMES", **kwargs)
        assert not response.content

    ##########################################
    # Extended Attributes(XAttrs) Operations #
    ##########################################

    def set_xattr(
        self,
        path: str,
        xattr_name: str,
        xattr_value: Optional[str],
        flag: str,
        **kwargs: _PossibleArgumentTypes,
    ) -> None:
        """Set an xattr of a file or directory.

        :param xattr_name: The name must be prefixed with the namespace followed by ``.``. For
            example, ``user.attr``.
        :param flag: ``CREATE`` or ``REPLACE``
        """
        kwargs["xattr.name"] = xattr_name
        kwargs["xattr.value"] = xattr_value
        response = self._put(path, "SETXATTR", flag=flag, **kwargs)
        assert not response.content

    def remove_xattr(
        self, path: str, xattr_name: str, **kwargs: _PossibleArgumentTypes
    ) -> None:
        """Remove an xattr of a file or directory."""
        kwargs["xattr.name"] = xattr_name
        response = self._put(path, "REMOVEXATTR", **kwargs)
        assert not response.content

    def get_xattrs(
        self,
        path: str,
        xattr_name: Union[str, List[str], None] = None,
        encoding: str = "text",
        **kwargs: _PossibleArgumentTypes,
    ) -> Dict[str, Union[bytes, str, None]]:
        """Get one or more xattr values for a file or directory.

        :param xattr_name: ``str`` to get one attribute, ``list`` to get multiple attributes,
            ``None`` to get all attributes.
        :param encoding: ``text`` | ``hex`` | ``base64``, defaults to ``text``

        :returns: Dictionary mapping xattr name to value. With text encoding, the value will be a
            unicode string. With hex or base64 encoding, the value will be a byte array.
        :rtype: dict
        """
        kwargs["xattr.name"] = xattr_name
        json: List[Dict[str, Optional[str]]] = _json(
            self._get(path, "GETXATTRS", encoding=encoding, **kwargs)
        )["XAttrs"]
        # Decode the result
        result: Dict[str, Union[bytes, str, None]] = {}
        for attr in json:
            k = attr["name"]
            assert k is not None
            v = attr["value"]
            if v is None:
                result[k] = None
            elif encoding == "text":
                assert v.startswith('"') and v.endswith('"')
                result[k] = v[1:-1]
            elif encoding == "hex":
                assert v.startswith("0x")
                result[k] = binascii.unhexlify(v[2:])
            elif encoding == "base64":
                assert v.startswith("0s")
                result[k] = base64.b64decode(v[2:])
            else:
                warnings.warn("Unexpected encoding {}".format(encoding))
                result[k] = v
        return result

    def list_xattrs(self, path: str, **kwargs: _PossibleArgumentTypes) -> List[str]:
        """Get all of the xattr names for a file or directory.

        :rtype: list
        """
        result = simplejson.loads(
            _json(self._get(path, "LISTXATTRS", **kwargs))["XAttrNames"]
        )
        assert isinstance(result, list), type(result)
        return result

    #######################
    # Snapshot Operations #
    #######################

    def create_snapshot(self, path: str, **kwargs: _PossibleArgumentTypes) -> str:
        """Create a snapshot

        :param path: The directory where snapshots will be taken
        :param snapshotname: The name of the snapshot
        :returns: the snapshot path
        """
        response = _json(self._put(path, "CREATESNAPSHOT", **kwargs))["Path"]
        assert isinstance(response, str), type(response)
        return response

    def delete_snapshot(
        self,
        path: str,
        snapshotname: str,
        **kwargs: _PossibleArgumentTypes,
    ) -> None:
        """Delete a snapshot of a directory"""
        response = self._delete(
            path, "DELETESNAPSHOT", snapshotname=snapshotname, **kwargs
        )
        assert not response.content

    def rename_snapshot(
        self,
        path: str,
        oldsnapshotname: str,
        snapshotname: str,
        **kwargs: _PossibleArgumentTypes,
    ) -> None:
        """Rename a snapshot"""
        response = self._put(
            path,
            "RENAMESNAPSHOT",
            oldsnapshotname=oldsnapshotname,
            snapshotname=snapshotname,
            **kwargs,
        )
        assert not response.content

    ######################################################
    # Convenience Methods                                #
    # These are intended to mimic python / hdfs features #
    ######################################################

    def listdir(self, path: str, **kwargs: _PossibleArgumentTypes) -> List[str]:
        """Return a list containing names of files in the given path"""
        statuses = self.list_status(path, **kwargs)
        if (
            len(statuses) == 1
            and statuses[0].pathSuffix == ""
            and statuses[0].type == "FILE"
        ):
            raise NotADirectoryError("Not a directory: {!r}".format(path))
        return [f.pathSuffix for f in statuses]

    def exists(self, path: str, **kwargs: _PossibleArgumentTypes) -> bool:
        """Return true if the given path exists"""
        try:
            self.get_file_status(path, **kwargs)
            return True
        except HdfsFileNotFoundException:
            return False

    def walk(
        self,
        top: str,
        topdown: bool = True,
        onerror: Optional[Callable[[HdfsException], None]] = None,
        **kwargs: _PossibleArgumentTypes,
    ) -> Iterator[Tuple[str, List[str], List[str]]]:
        """See ``os.walk`` for documentation"""
        try:
            listing = self.list_status(top, **kwargs)
        except HdfsException as e:
            if onerror is not None:
                onerror(e)
            return

        dirnames, filenames = [], []
        for f in listing:
            if f.type == "DIRECTORY":
                dirnames.append(f.pathSuffix)
            elif f.type == "FILE":
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

    def copy_from_local(
        self, localsrc: str, dest: str, **kwargs: _PossibleArgumentTypes
    ) -> None:
        """Copy a single file from the local file system to ``dest``

        Takes all arguments that :py:meth:`create` takes.
        """
        with open(localsrc, "rb") as f:
            self.create(dest, f, **kwargs)

    def copy_to_local(
        self, src: str, localdest: str, **kwargs: _PossibleArgumentTypes
    ) -> None:
        """Copy a single file from ``src`` to the local file system

        Takes all arguments that :py:meth:`open` takes.
        """
        with self.open(src, **kwargs) as fsrc:
            with open(localdest, "wb") as fdst:  # type: IO[bytes]
                shutil.copyfileobj(fsrc, fdst)

    def get_active_namenode(self, max_staleness: Optional[float] = None) -> str:
        """Return the address of the currently active NameNode.

        :param max_staleness: This function caches the active NameNode. If this age of this cached
            result is less than ``max_staleness`` seconds, return it. Otherwise, or if this
            parameter is None, do a lookup.
        :type max_staleness: float
        :raises HdfsNoServerException: can't find an active NameNode
        """
        if (
            max_staleness is None
            or self._last_time_recorded_active is None
            or self._last_time_recorded_active < time.time() - max_staleness
        ):
            # Make a cheap request and rely on the reordering in self._record_last_active
            self.get_file_status("/")
        return self.hosts[0]


def _transform_user_name_key(kw_dict: Dict[str, _PossibleArgumentTypes]) -> None:
    """Convert user_name to user.name for convenience with python kwargs"""
    if "user_name" in kw_dict:
        kw_dict["user.name"] = kw_dict["user_name"]
        del kw_dict["user_name"]


def _json(response: requests.Response) -> Dict[str, Any]:
    try:
        js = response.json()
        assert isinstance(js, dict), type(js)
        return js
    except simplejson.scanner.JSONDecodeError:
        raise HdfsException(
            "Expected JSON. Is WebHDFS enabled? Got {!r}".format(response.text)
        )


def _check_response(
    response: requests.Response,
    expected_status: HTTPStatus = HTTPStatus.OK,
) -> None:
    if response.status_code == expected_status:
        return
    remote_exception: Dict[str, str] = _json(response)["RemoteException"]
    exception_name = remote_exception["exception"]
    python_name = "Hdfs" + exception_name
    if python_name in _EXCEPTION_CLASSES:
        cls = _EXCEPTION_CLASSES[python_name]
    else:
        cls = HdfsHttpException
        # prefix the message with the exception name since we're not using a fancy class
        remote_exception["message"] = (
            exception_name + " - " + remote_exception["message"]
        )
    raise cls(status_code=response.status_code, **remote_exception)
