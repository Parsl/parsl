import random
import zipfile

import pytest

import parsl
from parsl.config import Config
from parsl.data_provider.files import File
from parsl.data_provider.zip import ZipAuthorityError, ZipFileStaging
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider
from parsl.tests.configs.htex_local import fresh_config as local_config


@parsl.python_app
def count_lines(file):
    with open(file, "r") as f:
        return len(f.readlines())


@pytest.mark.local
def test_zip_in(tmpd_cwd):
    # basic test of zip file stage-in
    zip_path = tmpd_cwd / "container.zip"
    file_base = "data.txt"
    zip_file = File(f"zip:{zip_path / file_base}")

    # create a zip file containing one file with some abitrary number of lines
    n_lines = random.randint(0, 1000)

    with zipfile.ZipFile(zip_path, mode='w') as z:
        with z.open(file_base, mode='w') as f:
            for _ in range(n_lines):
                f.write(b'someline\n')

    app_future = count_lines(zip_file)

    assert app_future.result() == n_lines
