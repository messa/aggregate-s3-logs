from asyncio import Event, sleep
import gzip
from pytest import mark
import re

from aggregate_s3_logs.aggregate import aggregate_s3_logs


class DummyS3Wrapper:

    def __init__(self):
        self.files = {}

    async def list_objects(self, Bucket, Delimiter, Prefix):
        assert Delimiter == '/'
        assert not Prefix.startswith('/')
        assert Bucket == 'b1'
        await sleep(0.01)
        return [{'Key': k, 'StorageClass': 'STANDARD'} for k in sorted(self.files.keys()) if k.startswith(Prefix)]

    async def download_file(self, bucket_name, key, download_path):
        assert bucket_name == 'b1'
        download_path.write_bytes(self.files[key])
        await sleep(0.01)

    async def upload_file(self, bucket_name, key, src_path, content_type):
        assert bucket_name == 'b1'
        self.files[key] = src_path.read_bytes()
        await sleep(0.01)

    async def delete_object(self, bucket_name, key):
        assert bucket_name == 'b1'
        del self.files[key]
        await sleep(0.01)


@mark.asyncio
async def test_aggregate(temp_dir):
    dummy_s3 = DummyS3Wrapper()
    dummy_s3.files['foo/2020-03-01-12-00-00-ABCD'] = b'This file should not be processed'
    dummy_s3.files['prefix/foo.txt'] = b'This file should not be processed'
    dummy_s3.files['prefix/2020-02-01-12-10-00-ABCD'] = b'Hello, World!\n'
    dummy_s3.files['prefix/2020-02-01-12-20-00-CDEF'] = b'This file has no newline at the end'
    dummy_s3.files['prefix/2020-02-01-12-30-00-1234'] = b'line 1\nline 2\nline 3\n'
    dummy_s3.files['prefix/2020-02-02-14-15-30-1234'] = b'Another day\n'
    dummy_s3.files['prefix/2099-01-01-14-15-30-1234'] = b'This file is too fresh\n'
    await aggregate_s3_logs(
        bucket_name='b1',
        prefix='prefix/',
        s3_client_wrapper=dummy_s3,
        temp_dir=temp_dir,
        stop_event=Event(),
        force=True,
        delay_days=3)
    assert len(dummy_s3.files.keys()) == 5
    assert sorted(dummy_s3.files.keys())[0] == 'foo/2020-03-01-12-00-00-ABCD'
    assert re.match(r'^prefix/2020-02-01-aggregated-[0-9a-f]+.gz$', sorted(dummy_s3.files.keys())[1])
    assert re.match(r'^prefix/2020-02-02-aggregated-[0-9a-f]+.gz$', sorted(dummy_s3.files.keys())[2])
    assert sorted(dummy_s3.files.keys())[3] == 'prefix/2099-01-01-14-15-30-1234'
    assert sorted(dummy_s3.files.keys())[4] == 'prefix/foo.txt'
    filename_1 = sorted(dummy_s3.files.keys())[1]
    filename_2 = sorted(dummy_s3.files.keys())[2]
    assert gzip.decompress(dummy_s3.files[filename_1]) == (
        b'# prefix/2020-02-01-12-10-00-ABCD\n'
        b'Hello, World!\n'
        b'# prefix/2020-02-01-12-20-00-CDEF\n'
        b'This file has no newline at the end\n'
        b'# prefix/2020-02-01-12-30-00-1234\n'
        b'line 1\n'
        b'line 2\n'
        b'line 3\n'
    )
    assert gzip.decompress(dummy_s3.files[filename_2]) == (
        b'# prefix/2020-02-02-14-15-30-1234\n'
        b'Another day\n'
    )

