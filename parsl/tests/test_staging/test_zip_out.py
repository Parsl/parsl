import parsl
import pytest
import zipfile

from parsl.data_provider.files import File

from parsl.data_provider.data_manager import default_staging
from parsl.data_provider.zip import ZipFileStaging

###
from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.launchers import SimpleLauncher

from parsl.config import Config
from parsl.executors import HighThroughputExecutor
####


def local_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_local",
                storage_access=[ZipFileStaging()] + default_staging)
        ],
        strategy='none',
    )


@pytest.mark.local
def test_zip_path_split():
    from parsl.data_provider.zip import zip_path_split
    assert zip_path_split("/tmp/foo/this.zip/inside/here.txt") == ("/tmp/foo/this.zip", "inside/here.txt")


@parsl.bash_app
def output_something(outputs=()):
    return f"echo hello > {outputs[0]}"


@pytest.mark.local
def test_zip_out(tmpd_cwd):
    zip_path = str(tmpd_cwd) + "container.zip"
    file_base = "data.txt"
    of = File("zip:" + zip_path + "/" + file_base)

    app_future = output_something(outputs=[of])
    output_file_future = app_future.outputs[0]

    app_future.result()
    output_file_future.result()

    assert zipfile.is_zipfile(zip_path)

    with zipfile.ZipFile(zip_path) as z:
        assert file_base in z.namelist()
        assert len(z.namelist()) == 1


@pytest.mark.local
def test_zip_out_path(tmpd_cwd):
    # tests relative path handling inside a zip URL
    zip_path = str(tmpd_cwd) + "container.zip"

    # multiple levels of directory here to check that deep
    # creation of directories works
    relative_file_path = "a/b/c/data.txt"
    of = File("zip:" + zip_path + "/" + relative_file_path)

    app_future = output_something(outputs=[of])
    output_file_future = app_future.outputs[0]

    app_future.result()
    output_file_future.result()

    assert zipfile.is_zipfile(zip_path)

    with zipfile.ZipFile(zip_path) as z:
        assert relative_file_path in z.namelist()
        assert len(z.namelist()) == 1  # TODO: make a multi-file testcase staging into the same zipfile
