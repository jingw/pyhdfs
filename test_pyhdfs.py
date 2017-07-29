# encoding: utf-8
from __future__ import absolute_import, print_function, unicode_literals

import logging
import posixpath
import subprocess
import tempfile
import unittest

import mock
import os
import requests
from requests.api import request as original_request

from pyhdfs import FileChecksum
from pyhdfs import FileStatus
from pyhdfs import HdfsAccessControlException
from pyhdfs import HdfsClient
from pyhdfs import HdfsException
from pyhdfs import HdfsFileAlreadyExistsException
from pyhdfs import HdfsFileNotFoundException
from pyhdfs import HdfsHttpException
from pyhdfs import HdfsIOException
from pyhdfs import HdfsIllegalArgumentException
from pyhdfs import HdfsInvalidPathException
from pyhdfs import HdfsNoServerException
from pyhdfs import HdfsPathIsNotEmptyDirectoryException
from pyhdfs import HdfsSnapshotException
from pyhdfs import HdfsUnsupportedOperationException


try:
    # Python 3
    unittest.TestCase.assertCountEqual
except AttributeError:  # pragma: no cover
    # Python 2
    unittest.TestCase.assertCountEqual = unittest.TestCase.assertItemsEqual

try:
    NotADirectoryError
except NameError:  # pragma: no cover
    # Python 2
    NotADirectoryError = OSError

TEST_DIR = '/tmp/pyhdfs_test'
TEST_FILE = posixpath.join(TEST_DIR, 'some file')
FILE_CONTENTS = b'lorem ipsum dolor sit amet'
FILE_CONTENTS2 = b'some stuff'

# Exclude control characters and special path characters
PATHOLOGICAL_NAME = ''.join(
    chr(n) for n in range(32, 128) if chr(n) not in {'/', ':', ';'}
) + '\b\t\n\f\r中文'


def make_client(*args, **kwargs):
    return HdfsClient(*args, retry_delay=0.1, **kwargs)


def _hacked_request(method, url, **kwargs):
    """Hack to "redirect" hostnames that the VM knows to localhost"""
    url = url.replace('quickstart.cloudera', 'localhost')
    return original_request(method, url, **kwargs)


hack_quickstart_cloudera_to_local = mock.patch('requests.api.request', _hacked_request)


def _standby_response():
    resp = mock.Mock()
    resp.status_code = 403
    resp.json.return_value = {'RemoteException': {
        'exception': 'StandbyException',
        'message': 'blah',
    }}
    return resp


@hack_quickstart_cloudera_to_local
class TestWebHDFS(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)

    def _make_empty_dir(self, client):
        # Get an empty dir
        client.delete(TEST_DIR, recursive=True, user_name='hdfs')
        self.assertFalse(client.delete(TEST_DIR, recursive=True))
        self.assertTrue(client.mkdirs(TEST_DIR))

    def _make_dir_and_file(self, client):
        self._make_empty_dir(client)
        client.create(TEST_FILE, FILE_CONTENTS)

    def test_basic_operations(self):
        """Test all the basics"""
        client = make_client()

        self._make_empty_dir(client)

        # Doesn't error out if we make it again
        self.assertTrue(client.mkdirs(TEST_DIR))

        # Get its status
        status = client.get_file_status(TEST_DIR)
        self.assertEqual(status.childrenNum, 0)
        self.assertEqual(status.length, 0)
        self.assertEqual(status.type, 'DIRECTORY')
        # Get listing
        self.assertEqual(client.list_status(TEST_DIR), [])
        # Get content summary
        content_summary = client.get_content_summary(TEST_DIR)
        self.assertEqual(content_summary.length, 0)

        # Checksumming a folder shouldn't work
        self.assertRaises(HdfsFileNotFoundException, lambda: client.get_file_checksum(TEST_DIR))

        # Make a file
        client.create(TEST_FILE, FILE_CONTENTS)

        # Redo metadata queries on TEST_DIR
        status = client.get_file_status(TEST_DIR)
        self.assertEqual(status.childrenNum, 1)
        self.assertEqual(status.length, 0)
        self.assertEqual(status.type, 'DIRECTORY')
        listing = client.list_status(TEST_DIR)
        self.assertEqual(len(listing), 1)
        self.assertEqual(listing[0].type, 'FILE')
        self.assertEqual(listing[0].pathSuffix, posixpath.basename(TEST_FILE))
        content_summary = client.get_content_summary(TEST_DIR)
        self.assertEqual(content_summary.length, len(FILE_CONTENTS))

        # Metadata queries on TEST_FILE
        status = client.get_file_status(TEST_FILE)
        self.assertEqual(status.childrenNum, 0)
        self.assertEqual(status.length, len(FILE_CONTENTS))
        self.assertEqual(status.type, 'FILE')
        listing = client.list_status(TEST_FILE)
        self.assertEqual(len(listing), 1)
        self.assertEqual(listing[0].type, 'FILE')
        content_summary = client.get_content_summary(TEST_FILE)
        self.assertEqual(content_summary.length, len(FILE_CONTENTS))
        checksum = client.get_file_checksum(TEST_FILE)
        self.assertTrue(checksum.bytes)
        self.assertTrue(checksum.length)
        self.assertTrue(checksum.algorithm)

        # Read back the test file
        with client.open(TEST_FILE) as f:
            self.assertEqual(f.read(), FILE_CONTENTS)

        # Append to the file and test the result
        client.append(TEST_FILE, FILE_CONTENTS2)
        with client.open(TEST_FILE) as f:
            self.assertEqual(f.read(), FILE_CONTENTS + FILE_CONTENTS2)

        # Clean up
        self.assertRaises(HdfsPathIsNotEmptyDirectoryException, lambda: client.delete(TEST_DIR))
        self.assertTrue(client.delete(TEST_DIR, recursive=True))
        self.assertFalse(client.delete(TEST_DIR, recursive=True))

    def test_list_file(self):
        client = make_client()
        self._make_dir_and_file(client)
        self.assertEqual(client.list_status(TEST_FILE), [client.get_file_status(TEST_FILE)])
        self.assertRaises(NotADirectoryError, lambda: client.listdir(TEST_FILE))

    def test_open_offset(self):
        client = make_client()
        self._make_dir_and_file(client)
        with client.open(TEST_FILE, offset=2, length=3) as f:
            self.assertEqual(f.read(), FILE_CONTENTS[2:5])

    def test_rename(self):
        client = make_client()
        self._make_dir_and_file(client)
        self.assertTrue(client.rename(TEST_FILE, posixpath.join(TEST_DIR, 'renamed')))
        self.assertEqual(client.listdir(TEST_DIR), ['renamed'])

    def test_get_home_directory(self):
        client = make_client(user_name='foo')
        self.assertEqual(client.get_home_directory(), '/user/foo')
        self.assertEqual(client.get_home_directory(user_name='bar'), '/user/bar')

    def test_set_replication(self):
        client = make_client()
        self._make_dir_and_file(client)
        self.assertFalse(client.set_replication(TEST_DIR))
        replication = client.get_file_status(TEST_FILE).replication
        self.assertTrue(client.set_replication(TEST_FILE))
        self.assertEqual(client.get_file_status(TEST_FILE).replication, replication)
        self.assertTrue(client.set_replication(TEST_FILE, replication=replication + 1))
        self.assertEqual(client.get_file_status(TEST_FILE).replication, replication + 1)

    def test_set_permission(self):
        client = make_client()
        self._make_empty_dir(client)
        client.set_permission(TEST_DIR, permission=777)
        self.assertEqual(client.get_file_status(TEST_DIR).permission, '777')
        client.set_permission(TEST_DIR, permission=500)
        self.assertEqual(client.get_file_status(TEST_DIR).permission, '500')

    def test_set_owner(self):
        client = make_client(user_name='hdfs')
        self._make_empty_dir(client)
        client.set_times(TEST_DIR, modificationtime=1234)
        self.assertEqual(client.get_file_status(TEST_DIR).modificationTime, 1234)
        client.set_times(TEST_DIR, accesstime=5678)
        self.assertEqual(client.get_file_status(TEST_DIR).accessTime, 5678)

    def test_set_times(self):
        client = make_client(user_name='hdfs')
        self._make_empty_dir(client)
        client.set_owner(TEST_DIR, owner='some_new_user')
        self.assertEqual(client.get_file_status(TEST_DIR).owner, 'some_new_user')
        client.set_owner(TEST_DIR, group='some_new_group')
        self.assertEqual(client.get_file_status(TEST_DIR).group, 'some_new_group')

    def test_misc_exceptions(self):
        client = make_client()
        self.assertRaises(HdfsFileNotFoundException,
                          lambda: client.get_file_status('/does_not_exist'))
        self.assertRaises(HdfsFileAlreadyExistsException, lambda: client.create('/tmp', ''))
        self.assertRaises(HdfsAccessControlException,
                          lambda: client.set_owner('/', owner='blah', user_name='blah'))
        self.assertRaises(HdfsIllegalArgumentException,
                          lambda: client._request('PUT', '/', 'blah'))

    def test_funny_characters(self):
        client = make_client(user_name='hdfs')
        self._make_empty_dir(client)
        ugly_dir = posixpath.join(TEST_DIR, PATHOLOGICAL_NAME)
        ugly_file = posixpath.join(TEST_DIR, PATHOLOGICAL_NAME, PATHOLOGICAL_NAME)
        client.mkdirs(ugly_dir)
        client.create(ugly_file, FILE_CONTENTS)
        self.assertEqual(client.listdir(TEST_DIR), [PATHOLOGICAL_NAME])
        client.get_file_status(ugly_dir)
        client.get_file_status(ugly_file)
        with client.open(ugly_file) as f:
            self.assertEqual(f.read(), FILE_CONTENTS)

        self.assertRaises(HdfsInvalidPathException, lambda: client.mkdirs('/:'))

    def test_bad_host(self):
        client = make_client('does_not_exist1,does_not_exist2')
        self.assertRaises(HdfsNoServerException, lambda: client.get_file_status('/'))

    def test_standby_failure(self):
        """Request should fail if all nodes are standby"""
        client = make_client('a,b')

        def mock_request(method, url, **kwargs):
            mock_request.call_count += 1
            return _standby_response()
        mock_request.call_count = 0
        with mock.patch('requests.api.request', mock_request):
            self.assertRaises(HdfsNoServerException, lambda: client.get_file_status('/'))
        self.assertEqual(mock_request.call_count, client.max_tries * 2)

    def test_standby_success(self):
        """Request should succeed if one node is good"""
        client = make_client('standby,localhost', randomize_hosts=False)
        self.assertEqual(client.hosts, ['standby:50070', 'localhost:50070'])

        def mock_request(method, url, **kwargs):
            if 'standby' in url:
                mock_request.standby_count += 1
                return _standby_response()
            elif 'localhost' in url:
                mock_request.active_count += 1
                return original_request(method, url, **kwargs)
            else:
                self.fail("Unexpected url {}".format(url))  # pragma: no cover
        mock_request.standby_count = 0
        mock_request.active_count = 0

        with mock.patch('requests.api.request', mock_request):
            client.get_file_status('/')
        # should have rearranged itself
        self.assertEqual(client.hosts, ['localhost:50070', 'standby:50070'])
        self.assertEqual(mock_request.standby_count, 1)
        self.assertEqual(mock_request.active_count, 1)
        # Now it should just talk to the active
        with mock.patch('requests.api.request', mock_request):
            client.get_file_status('/')
        self.assertEqual(mock_request.standby_count, 1)
        self.assertEqual(mock_request.active_count, 2)

    def test_invalid_construction(self):
        self.assertRaises(ValueError, lambda: HdfsClient([]))
        self.assertRaises(ValueError, lambda: HdfsClient(retry_delay=-1))
        self.assertRaises(ValueError, lambda: HdfsClient(max_tries=0))

    def test_not_absolute(self):
        client = make_client()
        self.assertRaises(ValueError, lambda: client.list_status('not_absolute'))

    def test_webhdfs_off(self):
        """Fail with a non-specific exception when webhdfs is off"""
        client = make_client()
        # simulate being off by patching the URL
        with mock.patch('pyhdfs.WEBHDFS_PATH', '/foobar'):
            try:
                client.get_file_status('/does_not_exist')
            except HdfsException as e:
                self.assertIs(type(e), HdfsException)
            else:
                self.fail("should have raised something")  # pragma: no cover

    def test_unrecognized_exception(self):
        """An exception that we don't have a python class for should fall back to HdfsHttpException
        """
        client = make_client()

        def mock_request(*args, **kwargs):
            resp = mock.Mock()
            resp.status_code = 500
            resp.json.return_value = {'RemoteException': {
                'exception': 'SomeUnknownException',
                'message': 'some_test_msg',
                'newThing': '1',
            }}
            return resp

        with mock.patch('requests.api.request', mock_request):
            try:
                client.get_file_status('/')
            except HdfsHttpException as e:
                self.assertIs(type(e), HdfsHttpException)
                self.assertIn('SomeUnknownException', e.args[0])
                self.assertEqual(e.status_code, 500)
                self.assertEqual(e.exception, 'SomeUnknownException')
                self.assertEqual(e.message, 'SomeUnknownException - some_test_msg')
                self.assertEqual(e.newThing, '1')
            else:
                self.fail("should have thrown")  # pragma: no cover

    def test_host_in_request(self):
        """Client should support specifying host afterwards"""
        client = make_client('does_not_exist')
        self.assertRaises(HdfsNoServerException, lambda: client.get_file_status('/'))
        client.get_file_status('//localhost/')
        client.get_file_status('hdfs://localhost:50070/')
        client.get_file_status('foobar://localhost:50070/')
        self.assertEqual(client.hosts, ['does_not_exist:50070'])

    def test_concat(self):
        MIN_BLOCK_SIZE = 1024 * 1024
        client = make_client(user_name='hdfs')
        self._make_empty_dir(client)
        p1 = posixpath.join(TEST_DIR, PATHOLOGICAL_NAME)
        # Commas not supported
        p2 = posixpath.join(TEST_DIR, 'f2' + PATHOLOGICAL_NAME.replace(',', ''))
        p3 = posixpath.join(TEST_DIR, 'f3' + PATHOLOGICAL_NAME.replace(',', ''))
        a = b'a' * MIN_BLOCK_SIZE
        b = b'b' * MIN_BLOCK_SIZE * 2
        c = b'c'
        client.create(p1, a, blocksize=MIN_BLOCK_SIZE)
        client.create(p2, b, blocksize=MIN_BLOCK_SIZE)
        client.create(p3, c)
        client.concat(p1, [p2, p3])
        with client.open(p1) as f:
            self.assertTrue(f.read() == a + b + c)
        # Original files should be gone
        self.assertEqual(client.listdir(TEST_DIR), [posixpath.basename(p1)])

    def test_concat_invalid(self):
        client = make_client()
        self.assertRaises(ValueError, lambda: client.concat('/a', 'b'))
        self.assertRaises(NotImplementedError, lambda: client.concat('/a', ['/,']))

    def test_create_symlink(self):
        client = make_client()
        self._make_empty_dir(client)
        symlink = posixpath.join(TEST_DIR, 'cycle')
        self.assertRaises(HdfsUnsupportedOperationException,
                          lambda: client.create_symlink(symlink, destination=TEST_DIR))

    def test_snapshots(self):
        client = make_client()
        self._make_empty_dir(client)
        self.assertRaises(HdfsSnapshotException, lambda: client.create_snapshot(TEST_DIR))

        # WebHDFS doesn't support the dfsadmin command to enable snapshots, so this test makes
        # some assumptions to turn them on.
        subprocess.check_call(
            ['hdfs', 'dfsadmin', '-allowSnapshot', TEST_DIR],
            env={'HADOOP_USER_NAME': 'hdfs'},
        )

        path = client.create_snapshot(TEST_DIR, snapshotname='x')
        self.assertEqual(path, posixpath.join(TEST_DIR, '.snapshot', 'x'))
        client.rename_snapshot(TEST_DIR, 'x', 'y')
        client.delete_snapshot(TEST_DIR, 'y')

    def test_xattrs(self):
        self.maxDiff = None
        client = make_client()
        self._make_empty_dir(client)

        attr1 = 'user.' + ''.join(map(chr, range(32, 128))) + '\b\t\f中文'
        attr2 = 'user.blah'
        attr3 = 'user.empty'
        # Something replaces non-ASCII characters with the replacement character U+FFFD, so we need
        # to stop at 128
        binary_value = ''.join(map(chr, range(128))) + '中文'

        client.set_xattr(TEST_DIR, attr1, '1', 'CREATE')
        client.set_xattr(TEST_DIR, attr2, '2', 'CREATE')
        self.assertRaises(HdfsAccessControlException,
                          lambda: client.set_xattr(TEST_DIR, 'system.foo', 'blah', 'CREATE'))
        self.assertRaises(HdfsIOException,
                          lambda: client.set_xattr(TEST_DIR, attr1, '123', 'CREATE'))
        client.set_xattr(TEST_DIR, attr1, binary_value, 'REPLACE')
        client.set_xattr(TEST_DIR, attr3, None, 'CREATE')

        attrs = {
            attr1: binary_value.encode('utf-8'),
            attr2: b'2',
            attr3: None,
        }
        self.assertEqual(client.get_xattrs(TEST_DIR, encoding='base64'), attrs)
        self.assertEqual(client.get_xattrs(TEST_DIR, encoding='hex'), attrs)
        self.assertEqual(
            client.get_xattrs(TEST_DIR, encoding='base64', xattr_name=[attr2, attr1]),
            {k: v for k, v in attrs.items() if k != attr3},
        )
        self.assertEqual(
            client.get_xattrs(TEST_DIR, encoding='text', xattr_name=[attr2]),
            {attr2: '2'},
        )
        self.assertEqual(
            client.get_xattrs(TEST_DIR, encoding='text', xattr_name=attr2),
            {attr2: '2'},
        )

        self.assertCountEqual(client.list_xattrs(TEST_DIR), [attr1, attr2, attr3])
        client.remove_xattr(TEST_DIR, attr1)
        self.assertCountEqual(client.list_xattrs(TEST_DIR), [attr2, attr3])

    def test_exists(self):
        client = make_client()
        self.assertTrue(client.exists('/tmp'))
        self.assertFalse(client.exists('/does_not_exist'))
        self.assertRaises(HdfsInvalidPathException, lambda: client.exists('/:'))

    def _setup_walk(self, client):
        def path(*args):
            return posixpath.join(TEST_DIR, *args)
        self._make_empty_dir(client)
        client.create(path('f1'), '')
        client.mkdirs(path('a1', 'b1'))
        client.create(path('a1', 'b1', 'f2'), '')
        client.mkdirs(path('a1', 'b2'))
        client.mkdirs(path('a2'))
        return path

    def test_walk(self):
        client = make_client()
        path = self._setup_walk(client)
        self.assertEqual(
            list(client.walk(TEST_DIR)),
            [
                (path(), ['a1', 'a2'], ['f1']),
                (path('a1'), ['b1', 'b2'], []),
                (path('a1', 'b1'), [], ['f2']),
                (path('a1', 'b2'), [], []),
                (path('a2'), [], []),
            ]
        )
        prefix = '//localhost'
        self.assertEqual(
            list(client.walk(prefix + TEST_DIR, topdown=False)),
            [
                (prefix + path('a1', 'b1'), [], ['f2']),
                (prefix + path('a1', 'b2'), [], []),
                (prefix + path('a1'), ['b1', 'b2'], []),
                (prefix + path('a2'), [], []),
                (prefix + path(), ['a1', 'a2'], ['f1']),
            ]
        )

    def test_walk_error(self):
        client = make_client()
        path = self._setup_walk(client)
        # Make a directory unreadable
        # This test requires setting dfs.permissions.enabled / dfs.permissions to true
        client.set_permission(path('a1'), permission=700, recursive=True)
        visible = [
            (path(), ['a1', 'a2'], ['f1']),
            (path('a2'), [], []),
        ]
        self.assertEqual(
            list(client.walk(TEST_DIR, user_name='this_user_has_no_permissions')),
            visible,
        )

        def error_handler(e):
            error_handler.called = True
            self.assertIsInstance(e, HdfsAccessControlException)
        error_handler.called = False
        self.assertEqual(
            list(client.walk(TEST_DIR, onerror=error_handler, user_name='no_permissions')),
            visible,
        )
        self.assertTrue(error_handler.called)

    def test_copy_local(self):
        client = make_client()
        self._make_empty_dir(client)
        target = posixpath.join(TEST_DIR, 'f')
        original = os.urandom(1000)

        with tempfile.NamedTemporaryFile() as f:
            f.write(original)
            f.flush()
            client.copy_from_local(f.name, target)

        with tempfile.NamedTemporaryFile('rb') as f2:
            client.copy_to_local(target, f2.name)
            contents = f2.read()

        self.assertEqual(original, contents)

    def test_get_active_namenode(self):
        client = make_client('does_not_exist,localhost')
        # No cached result
        self.assertEqual(client.get_active_namenode(100), 'localhost:50070')

        # should hit cache
        with mock.patch('requests.api.request') as request:
            self.assertEqual(client.get_active_namenode(100), 'localhost:50070')
            self.assertFalse(request.called)

        # should make request
        def wrapped_request(*args, **kwargs):
            wrapped_request.called = True
            return original_request(*args, **kwargs)
        wrapped_request.called = False
        with mock.patch('requests.api.request', wrapped_request):
            self.assertEqual(client.get_active_namenode(-1), 'localhost:50070')
            self.assertTrue(wrapped_request.called)

        bad_server_client = make_client('does_not_exist')
        self.assertRaises(HdfsNoServerException, lambda: bad_server_client.get_active_namenode())

    def test_invalid_requests_kwargs(self):
        """some kwargs are reserved"""
        self.assertRaisesRegexp(
            ValueError, 'Cannot override', lambda: HdfsClient(requests_kwargs={'url': 'test'})
        )

    def test_requests_kwargs(self):
        client = make_client(requests_kwargs={'proxies': {'http': 'localhost:99999'}})
        self.assertRaises(HdfsNoServerException, lambda: client.get_file_status('/'))

    def test_requests_session(self):
        with requests.Session() as session:
            client = make_client(requests_session=session)
            assert client.exists('/tmp')


class TestBoilerplateClass(unittest.TestCase):
    def test_repr(self):
        x = FileStatus(owner='somebody', length=5)
        r = repr(x)
        self.assertIn('somebody', r)
        self.assertEqual(eval(r), x)

    def test_equality(self):
        x = FileStatus(x=1)
        y = FileChecksum(x=1)
        z = FileChecksum(x=1)
        self.assertFalse(x == y)
        self.assertTrue(x != y)
        self.assertTrue(y == z)
        self.assertFalse(y != z)
