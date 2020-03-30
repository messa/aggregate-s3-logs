from asyncio import Semaphore
import boto3
from logging import getLogger
from pathlib import Path
from shutil import copyfileobj
import threading
from time import sleep as sleep_sync

from .util import get_running_loop, run_in_thread


logger = getLogger(__name__)


class S3ClientWrapper:

    max_concurrent_downloads = 16
    max_concurrent_uploads = 16

    def __init__(self):
        assert get_running_loop() # I've learned not to create asyncio primitives outside the loop :)
        self._download_sem = Semaphore(self.max_concurrent_downloads)
        self._upload_sem = Semaphore(self.max_concurrent_uploads)

    async def list_objects(self, **kwargs):
        async with self._download_sem:
            return await run_in_thread(self._list_objects_sync, kwargs)

    def _list_objects_sync(self, paginate_kwargs):
        assert paginate_kwargs['Bucket']
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        response_iterator = paginator.paginate(**paginate_kwargs)
        # See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Paginator.ListObjectsV2.paginate
        items = []
        for n, response in enumerate(response_iterator, start=1):
            logger.info(
                'Retrieved list_objects_v2 page %d with %d items (%d total)',
                n, len(response['Contents']), len(items) + len(response['Contents']))
            for item in response['Contents']:
                items.append(item)
        return items

    async def download_file(self, bucket_name, key, download_path):
        async with self._download_sem:
            return await run_in_thread(self._download_file_sync, bucket_name, key, download_path)

    def _download_file_sync(self, bucket_name, key, download_path):
        assert isinstance(bucket_name, str)
        assert isinstance(key, str)
        assert isinstance(download_path, Path)
        s3_client = boto3.client('s3')
        logger.debug('Downloading %s %s to %s', bucket_name, key, download_path)
        res = s3_client.get_object(Bucket=bucket_name, Key=key)
        with download_path.open(mode='wb') as f:
            copyfileobj(res['Body'], f)

    async def upload_file(self, bucket_name, key, src_path, content_type):
        async with self._upload_sem:
            return await run_in_thread(self._upload_file_sync, bucket_name, key, src_path, content_type)

    def _upload_file_sync(self, bucket_name, key, src_path, content_type):
        assert isinstance(bucket_name, str)
        assert isinstance(key, str)
        assert isinstance(src_path, Path)
        assert isinstance(content_type, str)
        s3_client = boto3.client('s3')
        logger.debug('Uploading %s to %s %s', src_path, bucket_name, key)
        with src_path.open(mode='rb') as f:
            res = s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=f,
                ACL='private',
                StorageClass='STANDARD_IA',
                ContentType=content_type)

    async def delete_objects(self, bucket_name, keys):
        async with self._upload_sem:
            return await run_in_thread(self._delete_objects_sync, bucket_name, keys)

    def _delete_objects_sync(self, bucket_name, keys):
        assert isinstance(bucket_name, str)
        assert isinstance(keys, list)
        assert [isinstance(key, str) for key in keys]
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_objects
        s3_client = boto3.client('s3')
        chunks = split(keys, 500)
        for n, chunk in enumerate(chunks, start=1):
            logger.debug('Deleting %d keys in %s (chunk %d/%d): %r', len(chunk), bucket_name, n, len(chunks), keys)
            try_count = 0
            while True:
                try_count += 1
                if try_count > 1:
                    logger.debug('Trying again to delete %d keys in %s', len(chunk), bucket_name)
                try:
                    res = s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={
                            'Quiet': False,
                            'Objects': [{'Key': key} for key in chunk],
                        })
                except Exception as e:
                    if try_count >= 5:
                        raise e
                    sleep_duration = 1 + 2**try_count
                    logger.exception('delete_objects failed: %r; trying again in %d s...', e, sleep_duration)
                    sleep_sync(sleep_duration)
                    continue

                logger.debug('delete_objects result: %r', res)
                if res.get('Errors'):
                    raise Exception('delete_objects returned Errors: {}'.format(res['Errors']))
                del res
                break


def split(items, chunk_size):
    chunks = []
    chunk = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            chunks.append(chunk)
            chunk = []
    if chunk:
        chunks.append(chunks)
    return chunks


assert split('foobar', 10) == ['f', 'o', 'o', 'b', 'a', 'r']
assert split('foobar', 6) == ['f', 'o', 'o', 'b', 'a', 'r']
assert split('foobar', 5) == ['f', 'o', 'o', 'b', 'a'], ['r']
assert split('foobar', 3) == ['f', 'o', 'o'], ['b', 'a', 'r']
