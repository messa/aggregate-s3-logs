from argparse import ArgumentParser
import asyncio
from asyncio import Event
from contextlib import ExitStack
from logging import getLogger
from pathlib import Path
import re
from signal import SIGTERM, SIGINT
import sys
from tempfile import TemporaryDirectory

from .aggregate import aggregate_s3_logs
from .s3_client import S3ClientWrapper
from .util import get_running_loop


logger = getLogger(__name__)


def aggregate_s3_logs_main():
    p = ArgumentParser()
    p.add_argument('--verbose', '-v', action='store_true')
    p.add_argument('--log-file')
    p.add_argument('--temp-dir')
    p.add_argument('--force', '-f', action='store_true', default=False)
    p.add_argument('s3_url')
    args = p.parse_args()
    setup_logging(verbose=args.verbose)
    if args.log_file:
        setup_log_file(args.log_file)
    bucket_name, prefix = parse_s3_url(args.s3_url)
    try:
        with ExitStack() as stack:
            if args.temp_dir:
                temp_dir = args.temp_dir
            else:
                temp_dir = stack.enter_context(TemporaryDirectory(prefix='aggregate_s3_logs.'))
            asyncio.run(async_main(
                bucket_name=bucket_name,
                prefix=prefix,
                temp_dir=Path(temp_dir),
                force=args.force,
            ))
    except Exception as e:
        logger.exception('Failed: %r', e)
        sys.exit(repr(e))


async def async_main(bucket_name, prefix, temp_dir, force):
    stop_event = Event()
    loop = get_running_loop()
    loop.add_signal_handler(SIGTERM, lambda: stop_event.set())
    #loop.add_signal_handler(SIGINT, lambda: stop_event.set())
    await aggregate_s3_logs(
        bucket_name=bucket_name,
        prefix=prefix,
        temp_dir=Path(temp_dir),
        force=force,
        stop_event=stop_event,
        s3_client_wrapper=S3ClientWrapper())


def parse_s3_url(s3_url):
    m = re.match(r'^s3://([a-z0-9-]+)/([^?]+)$', s3_url)
    bucket_name, prefix = m.groups()
    return (bucket_name, prefix)


log_format = '%(asctime)s [%(threadName)s] %(name)s %(levelname)5s: %(message)s'


def setup_logging(verbose):
    from logging import getLogger, StreamHandler, Formatter, DEBUG, INFO
    getLogger('').setLevel(DEBUG)
    getLogger('botocore').setLevel(INFO)
    getLogger('urllib3.connectionpool').setLevel(INFO)
    h = StreamHandler()
    h.setLevel(DEBUG if verbose else INFO)
    h.setFormatter(Formatter(log_format))
    getLogger('').addHandler(h)


def setup_log_file(log_file_path):
    from logging import getLogger, Formatter, DEBUG
    from logging.handlers import WatchedFileHandler
    h = WatchedFileHandler(log_file_path)
    h.setLevel(DEBUG)
    h.setFormatter(Formatter(log_format))
    getLogger('').addHandler(h)
