from asyncio import Semaphore
import boto3
from logging import getLogger
from pathlib import Path
from shutil import copyfileobj
import threading

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
            return await run_in_thread(self.list_objects_sync, kwargs)

    def list_objects_sync(self, paginate_kwargs):
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
            return await run_in_thread(self.download_file_sync, bucket_name, key, download_path)

    def download_file_sync(self, bucket_name, key, download_path):
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
            return await run_in_thread(self.upload_file_sync, bucket_name, key, src_path, content_type)

    def upload_file_sync(self, bucket_name, key, src_path, content_type):
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

    async def delete_object(self, bucket_name, key):
        async with self._upload_sem:
            return await run_in_thread(self.delete_object_sync, bucket_name, key)

    def delete_object_sync(self, bucket_name, key):
        assert isinstance(bucket_name, str)
        assert isinstance(key, str)
        s3_client = boto3.client('s3')
        logger.debug('Deleting %s %s', bucket_name, key)
        res = s3_client.put_object(
            Bucket=bucket_name,
            Key=key)
