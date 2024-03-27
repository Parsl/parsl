import os
import parsl
import pytest

from parsl.tests.configs.htex_local_alternate import fresh_config as local_config
from parsl.data_provider.files import File


@parsl.bash_app
def output_something(*, stdout=parsl.AUTO_LOGNAME, stderr=parsl.AUTO_LOGNAME):
    return "echo hello ; echo goodbye >&2"


@pytest.mark.local
def test_stdout_staging_file(tmpd_cwd):
    basename = str(tmpd_cwd) + "/benc-stdout.txt"
    stdout_file = File("file://" + basename)

    app_future = output_something(stdout=stdout_file)

    # there's no way to get the stdout staging future at the moment. In the
    # case of no-op file: staging, that's fine because the file is created
    # in-place at the destination.
    # TODO: this is a bug/missing feature in implementation

    app_future.result()
    # TODO: output_file_future.result()

    assert os.path.exists(basename)


@pytest.mark.local
def test_stdout_stderr_staging_zip(tmpd_cwd):
    zipfile_name = str(tmpd_cwd) + "/staging.zip"
    stdout_relative_path = "somewhere/test-out.txt"
    stdout_file = File("zip:" + zipfile_name + "/" + stdout_relative_path)

    stderr_relative_path = "somewhere/test-error.txt"
    stderr_file = File("zip:" + zipfile_name + "/" + stderr_relative_path)

    app_future = output_something(stdout=stdout_file, stderr=stderr_file)

    app_future.stdout_future.result()
    app_future.stderr_future.result()

    assert os.path.exists(zipfile_name)
    # TODO: should check inside the zipfile for content
