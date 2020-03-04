from logging import getLogger, DEBUG
from pathlib import Path
from pytest import fixture


getLogger('').setLevel(DEBUG)


@fixture
def temp_dir(tmpdir):
    return Path(tmpdir)
