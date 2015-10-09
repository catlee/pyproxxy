#!/usr/bin/env python
import mock
import boto3
import urllib.parse
import pytest
import asyncio
import aiohttp.web

# Set up dummy credentials for boto
boto3.setup_default_session(
    aws_access_key_id='KEY_ID',
    aws_secret_access_key='ACCESS_KEY',
)

import proxxy


def test_object_url_get():
    expires_in = 60
    with mock.patch('time.time') as time_func:
        time_func.return_value = 100000000
        expires = time_func() + expires_in
        url = proxxy.make_object_url('bucket1', 'mybackend/object1', 'GET', expires_in)

    bits = urllib.parse.urlparse(url)
    assert bits.netloc == 'bucket1.s3.amazonaws.com'
    assert bits.scheme == 'https'
    assert bits.path == '/mybackend/object1'

    query_args = urllib.parse.parse_qs(bits.query)
    assert query_args['AWSAccessKeyId'] == ['KEY_ID']
    assert query_args['Expires'] == [str(expires)]


def test_object_url_put():
    expires_in = 60
    with mock.patch('time.time') as time_func:
        time_func.return_value = 100000000
        expires = time_func() + expires_in
        url = proxxy.make_object_url('bucket1', 'mybackend/object1', 'PUT', expires_in)

    bits = urllib.parse.urlparse(url)
    assert bits.netloc == 'bucket1.s3.amazonaws.com'
    assert bits.scheme == 'https'
    assert bits.path == '/mybackend/object1'

    query_args = urllib.parse.parse_qs(bits.query)
    assert query_args['AWSAccessKeyId'] == ['KEY_ID']
    assert query_args['Expires'] == [str(expires)]


@pytest.yield_fixture
def p(request):
    p = proxxy.Proxxy('bucket1')
    p.suffix = '.hostname.domainname'

    yield p

    p.close()


def test_proxxy(p):
    assert p.bucket_name == 'bucket1'

    p.add_backend('ftp', 'https://ftp.mozilla.org')
    assert p.backends == {'ftp': 'https://ftp.mozilla.org'}

    assert p.make_object_url('foo/bar') == proxxy.make_object_url('bucket1', 'foo/bar', 'GET', p.expiry_time)


class FakeRequest:
    host = 'ftp.hostname.domainname'
    path_qs = '/foo/bar'


def test_request_parsing(p):
    request = FakeRequest()
    p.add_backend('ftp', 'https://ftp.mozilla.org')
    b = p.get_backend(request)
    assert b == 'https://ftp.mozilla.org'

    u = p.get_backend_url(request)
    assert u == 'https://ftp.mozilla.org/foo/bar'

    o = p.get_object_name(request)
    assert o == 'ftp/foo/bar'


class FakeRequestSession:
    def __init__(self, status, text):
        self.status = status
        self.text = text

    @asyncio.coroutine
    def request(self, *args, **kwargs):
        self.request_args = args
        self.request_kwargs = kwargs
        return self

    def close(self):
        self.closed = True


@pytest.mark.asyncio
def test_is_cached(p):
    # Mock out request_session...
    with mock.patch.object(p, 'request_session', FakeRequestSession(200, 'OK')):
        result = yield from p.is_cached('/foo/bar')
        assert result is True

    with mock.patch.object(p, 'request_session', FakeRequestSession(404, 'Not Found')):
        result = yield from p.is_cached('/foo/bar')
        assert result is False

    with mock.patch.object(p, 'request_session', FakeRequestSession(500, 'ISE')):
        result = yield from p.is_cached('/foo/bar')
        assert result is False
