from asyncio import create_task, wait, FIRST_EXCEPTION, CancelledError, sleep
from collections import defaultdict
from datetime import datetime, timedelta
from functools import partial
import gzip
import hashlib
from logging import getLogger
from operator import itemgetter
import re
from reprlib import repr as smart_repr
from shutil import copyfileobj
from uuid import uuid4

from .util import run_in_thread


logger = getLogger(__name__)


re_filename = re.compile(r'^(?P<day>20[12][0-9]-[012][0-9]-[0-3][0-9])-[012][0-9]-[0-5][0-9]-[0-6][0-9]-[0-9A-F]+$')

assert re_filename.match('2019-05-27-12-16-57-674B2D6256BF2FC3')

day_worker_count = 8


async def aggregate_s3_logs(bucket_name, prefix, s3_client_wrapper, temp_dir, stop_event, force, delay_days=3):
    items = await s3_client_wrapper.list_objects(Bucket=bucket_name, Delimiter='/', Prefix=prefix)
    items.sort(key=itemgetter('Key'))
    if not items:
        logger.warning('No objects found with prefix %r', prefix)
        return
    logger.debug('Retrieved %d keys: %s - %s', len(items), items[0]['Key'], items[-1]['Key'])
    groups = group_s3_items_by_day(items, delay_days=delay_days)
    for day in sorted(groups.keys()):
        glacier_keys = [x['Key'] for x in groups[day] if x['StorageClass'] in ('GLACIER', 'DEEP_ARCHIVE')]
        if glacier_keys:
            logger.warning(
                'Skipping day %s - object(s) would have to be restored from GLACIER or DEEP_ARCHIVE: %s',
                day, ' '.join(glacier_keys))
            del groups[day]
    if not groups:
        logger.info('No files to be processed')
        return
    logger.info(
        '%d objects will be aggregated into %d day archives (%s - %s)',
        len(items), len(groups), min(groups.keys()), max(groups.keys()))
    await process_queue([
        partial(
            process_group,
                day, groups[day],
                stop_event=stop_event,
                temp_dir=temp_dir,
                bucket_name=bucket_name,
                force=force,
                s3_client_wrapper=s3_client_wrapper)
        for day in sorted(groups.keys())
    ])


async def process_queue(queue, worker_count=8):
    queue = list(reversed(queue))

    async def worker():
        while queue:
            f = queue.pop()
            await f()

    tasks = [create_task(worker()) for i in range(worker_count)]
    done, pending = await wait(tasks, return_when=FIRST_EXCEPTION)
    for t in pending:
        t.cancel()
    for t in tasks:
        try:
            await t
        except CancelledError:
            pass


async def process_group(group_id, s3_items, stop_event, temp_dir, bucket_name, s3_client_wrapper, force):
    assert isinstance(group_id, str)
    assert isinstance(s3_items, list)
    assert isinstance(force, bool)
    if stop_event.is_set():
        return
    logger.info('[%s] Aggregating %d files', group_id, len(s3_items))
    s3_keys = [s3_item['Key'] for s3_item in s3_items]
    download_paths = [temp_dir / k.split('/')[-1] for k in s3_keys]
    logger.debug('[%s] s3_keys: %r', group_id, s3_keys)
    logger.debug('[%s] download_paths: %r', group_id, download_paths)
    result_path = temp_dir / '{}-{}.gz'.format(group_id, uuid4().hex)
    try:
        assert len(download_paths) == len(s3_items)
        assert all(not p.exists() for p in download_paths)
        await process_queue([
            partial(s3_client_wrapper.download_file, bucket_name, s3_item['Key'], dp)
            for s3_item, dp in zip(s3_items, download_paths)
        ])
        with gzip.open(result_path, mode='wb') as f_res:
            await concatenate_files(s3_keys, download_paths, f_res)
        logger.debug('result_path: %s (%.2f kB)', result_path, result_path.stat().st_size / 1024)
        result_hash = await get_file_sha1_hex(result_path)
        result_filename = '{}-aggregated-{}'.format(group_id, result_hash[:7])
        result_key = s3_keys[0].rsplit('/', 1)[0] + '/' + result_filename
        if not force:
            logger.info('Would upload %s', result_key)
            for k in s3_keys:
                logger.info('Would delete %s', k)
        else:
            await s3_client_wrapper.upload_file(bucket_name, result_key, result_path, content_type='application/gzip')
            await process_queue([
                partial(s3_client_wrapper.delete_object, bucket_name, s3_key)
                for s3_key in s3_keys
            ])
    except CancelledError as e:
        logger.info('[%s] Cancelled', group_id)
        raise e
    except Exception as e:
        logger.exception('[%s] Failed: %r', group_id, e)
        raise Exception('Group {} failed: {!r}'.format(group_id, e)) from None
    finally:
        if result_path.exists():
            result_path.unlink()
        for p in download_paths:
            if p.exists():
                p.unlink()


async def concatenate_files(s3_keys, download_paths, f_res):
    assert len(s3_keys) == len(download_paths)
    for s3_key, dp in zip(s3_keys, download_paths):
        f_res.write('# {key}\n'.format(key=s3_key).encode('UTF-8'))
        with dp.open(mode='rb') as f_src:
            chunk = f_src.read(90)
            try:
                chunk.decode('ascii')
            except Exception as e:
                raise Exception('File {} beginning is not in ASCII: {!r}'.format(s3_key, chunk))
            f_res.write(chunk)
            await run_in_thread(copyfileobj, f_src, f_res)


async def get_file_sha1_hex(path):
    return await run_in_thread(get_file_sha1_hex_sync, path)


def get_file_sha1_hex_sync(path):
    h = hashlib.sha1()
    with path.open(mode='rb') as f:
        while True:
            chunk = f.read(65536)
            if chunk == b'':
                break
            h.update(chunk)
    return h.hexdigest()


def group_s3_items_by_day(items, delay_days):
    groups = defaultdict(list)
    for item in items:
        filename = item['Key'].rsplit('/', 1)[-1]
        if 'aggregated' in filename:
            continue
        m = re_filename.match(filename)
        if not m:
            logger.debug('Unrecognized filename: %s (full key: %r)', filename, item['Key'])
            continue
        day_str, = m.groups()
        day_date = datetime.strptime(day_str, '%Y-%m-%d').date()
        if day_date >= (datetime.utcnow() - timedelta(days=delay_days)).date():
            logger.debug('Skipping - too fresh: %s', item['Key'])
            continue
        groups[day_str].append(item)
    return dict(groups)