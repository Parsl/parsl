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
def generate_lines(n: int, *, outputs):
    with open(outputs[0], "w") as f:
        for x in range(n):
            # write numbered lines
            f.write(str(x) + "\n")


@parsl.python_app
def count_lines(file):
    with open(file, "r") as f:
        return len(f.readlines())


@pytest.mark.local
def test_zip_pipeline(tmpd_cwd):
    # basic test of zip file stage-in
    zip_path = tmpd_cwd / "container.zip"
    file_base = "data.txt"
    zip_file = File(f"zip:{zip_path / file_base}")

    n_lines = random.randint(0, 1000)
    generate_fut = generate_lines(n_lines, outputs=[zip_file])
    n_lines_out = count_lines(generate_fut.outputs[0]).result()

    assert n_lines == n_lines_out
