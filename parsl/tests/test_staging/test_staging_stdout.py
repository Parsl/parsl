import logging
import os
import zipfile

import pytest

import parsl
from parsl.app.futures import DataFuture
from parsl.data_provider.files import File
from parsl.tests.configs.htex_local import fresh_config as local_config


@parsl.bash_app
def output_to_stds(*, stdout=parsl.AUTO_LOGNAME, stderr=parsl.AUTO_LOGNAME):
    return "echo hello ; echo goodbye >&2"


@pytest.mark.staging_required
def test_stdout_staging_file(tmpd_cwd, caplog):
    basename = str(tmpd_cwd) + "/stdout.txt"
    stdout_file = File("file://" + basename)

    app_future = output_to_stds(stdout=stdout_file)

    assert isinstance(app_future.stdout, DataFuture)
    app_future.stdout.result()

    assert os.path.exists(basename)

    for record in caplog.records:
        assert record.levelno < logging.ERROR


@pytest.mark.staging_required
def test_stdout_stderr_staging_zip(tmpd_cwd, caplog):
    zipfile_name = str(tmpd_cwd) + "/staging.zip"
    stdout_relative_path = "somewhere/test-out.txt"
    stdout_file = File("zip:" + zipfile_name + "/" + stdout_relative_path)

    stderr_relative_path = "somewhere/test-error.txt"
    stderr_file = File("zip:" + zipfile_name + "/" + stderr_relative_path)

    app_future = output_to_stds(stdout=stdout_file, stderr=stderr_file)

    assert isinstance(app_future.stdout, DataFuture)
    app_future.stdout.result()

    # check the file exists as soon as possible
    assert os.path.exists(zipfile_name)
    with zipfile.ZipFile(zipfile_name) as z:
        with z.open(stdout_relative_path) as f:
            assert f.readlines() == [b'hello\n']

    assert isinstance(app_future.stderr, DataFuture)
    app_future.stderr.result()
    with zipfile.ZipFile(zipfile_name) as z:
        with z.open(stderr_relative_path) as f:
            # The last line of stderr should be goodbye, but Parsl will write
            # other Parsl-specific into to stderr before that, so only assert
            # the behaviour of the final line.
            assert f.readlines()[-1] == b'goodbye\n'

    for record in caplog.records:
        assert record.levelno < logging.ERROR
