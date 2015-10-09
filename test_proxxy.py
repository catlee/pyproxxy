#!/usr/bin/env python
import mock
import boto3
import urllib.parse

boto3.setup_default_session(
    aws_access_key_id='KEY_ID',
    aws_secret_access_key='ACCESS_KEY',
)

import proxxy


def test_object_url():
    expires_in = 60
    with mock.patch('time.time') as time_func:
        time_func.return_value = 100000000
        expires = time_func() + expires_in
        url = proxxy.make_object_url('bucket1', 'object1', 'GET', expires_in)

    bits = urllib.parse.urlparse(url)
    assert bits.netloc == 'bucket1.s3.amazonaws.com'
    assert bits.scheme == 'https'
    assert bits.path == '/object1'

    query_args = urllib.parse.parse_qs(bits.query)
    assert query_args['AWSAccessKeyId'] == ['KEY_ID']
    assert query_args['Expires'] == [str(expires)]
