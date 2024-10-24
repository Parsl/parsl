import zipfile

import pytest

import parsl
from parsl.config import Config
from parsl.data_provider.data_manager import default_staging
from parsl.data_provider.files import File
from parsl.data_provider.zip import ZipAuthorityError, ZipFileStaging
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider
from parsl.tests.configs.htex_local import fresh_config as local_config


@pytest.mark.local
def test_zip_path_split():
    from parsl.data_provider.zip import zip_path_split
    assert zip_path_split("/tmp/foo/this.zip/inside/here.txt") == ("/tmp/foo/this.zip", "inside/here.txt")


@parsl.bash_app
def output_something(outputs=()):
    """This should output something into every specified output file:
    the position in the output sequence will be written into the
    corresponding output file.
    """
    cmds = []
    for n in range(len(outputs)):
        cmds.append(f"echo {n} > {outputs[n]}")

    return "; ".join(cmds)


@pytest.mark.local
def test_zip_out(tmpd_cwd):
    # basic test of zip file stage-out
    zip_path = tmpd_cwd / "container.zip"
    file_base = "data.txt"
    of = File(f"zip:{zip_path / file_base}")

    app_future = output_something(outputs=[of])
    output_file_future = app_future.outputs[0]

    app_future.result()
    output_file_future.result()

    assert zipfile.is_zipfile(zip_path)

    with zipfile.ZipFile(zip_path) as z:
        assert file_base in z.namelist()
        assert len(z.namelist()) == 1
        with z.open(file_base) as f:
            assert f.readlines() == [b'0\n']


@pytest.mark.local
def test_zip_out_multi(tmpd_cwd):
    # tests multiple files, multiple zip files and multiple
    # sub-paths

    zip_path_1 = tmpd_cwd / "container1.zip"
    zip_path_2 = tmpd_cwd / "container2.zip"

    relative_file_path_1 = "a/b/c/data.txt"
    relative_file_path_2 = "something.txt"
    relative_file_path_3 = "a/d/other.txt"
    of1 = File(f"zip:{zip_path_1 / relative_file_path_1}")
    of2 = File(f"zip:{zip_path_1 / relative_file_path_2}")
    of3 = File(f"zip:{zip_path_2 / relative_file_path_3}")

    app_future = output_something(outputs=[of1, of2, of3])

    for f in app_future.outputs:
        f.result()

    app_future.result()

    assert zipfile.is_zipfile(zip_path_1)

    with zipfile.ZipFile(zip_path_1) as z:
        assert relative_file_path_1 in z.namelist()
        assert relative_file_path_2 in z.namelist()
        assert len(z.namelist()) == 2
        with z.open(relative_file_path_1) as f:
            assert f.readlines() == [b'0\n']
        with z.open(relative_file_path_2) as f:
            assert f.readlines() == [b'1\n']

    assert zipfile.is_zipfile(zip_path_2)

    with zipfile.ZipFile(zip_path_2) as z:
        assert relative_file_path_3 in z.namelist()
        assert len(z.namelist()) == 1
        with z.open(relative_file_path_3) as f:
            assert f.readlines() == [b'2\n']


@pytest.mark.local
def test_zip_bad_authority(tmpd_cwd):
    # tests that there's an exception when staging a ZIP url with an authority
    # section specified, rather than silently ignoring it. This simulates a
    # user who misunderstands what that piece of what a zip: URL means.

    zip_path = tmpd_cwd / "container.zip"
    file_base = "data.txt"
    of = File(f"zip://someauthority/{zip_path / file_base}")

    with pytest.raises(ZipAuthorityError):
        output_something(outputs=[of])
