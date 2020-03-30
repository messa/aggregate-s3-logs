from datetime import datetime, timezone
import os
from pytest import fixture, skip, mark
from socket import getfqdn

from aggregate_s3_logs.s3_client import S3ClientWrapper


@fixture
def test_bucket_name():
    return 'messa-tests'


@fixture
def s3_client_wrapper():
    if getfqdn() != 'pm-mb.local':
        skip('Sorry :)')
    os.environ['AWS_PROFILE'] = 'messa'
    return S3ClientWrapper()


@mark.asyncio
async def test_list_objects(s3_client_wrapper, test_bucket_name):
    items = await s3_client_wrapper.list_objects(
        Bucket=test_bucket_name,
        Delimiter='/',
        Prefix='aggregate-s3-logs/sample_directory')
    assert items == []
    items = await s3_client_wrapper.list_objects(
        Bucket=test_bucket_name,
        Delimiter='/',
        Prefix='aggregate-s3-logs/sample_directory/')
    assert items == [
        {
            'ETag': '"d41d8cd98f00b204e9800998ecf8427e"',
            'Key': 'aggregate-s3-logs/sample_directory/file1',
            'LastModified': datetime(2020, 3, 30, 10, 23, 32, tzinfo=timezone.utc),
            'Size': 0,
            'StorageClass': 'STANDARD',
        }, {
            'ETag': '"d41d8cd98f00b204e9800998ecf8427e"',
            'Key': 'aggregate-s3-logs/sample_directory/file2',
            'LastModified': datetime(2020, 3, 30, 10, 23, 37, tzinfo=timezone.utc),
            'Size': 0,
            'StorageClass': 'STANDARD',
        },
    ]
