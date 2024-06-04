import logging
import zipfile
from functools import partial

import pytest

import parsl
from parsl.app.futures import DataFuture
from parsl.data_provider.files import File
from parsl.executors import ThreadPoolExecutor


@parsl.bash_app
def app_stdout(stdout=parsl.AUTO_LOGNAME):
    return "echo hello"


def const_str(cpath, task_record, err_or_out):
    return cpath


def const_with_cpath(autopath_specifier, content_path, caplog):
    with parsl.load(parsl.Config(std_autopath=partial(const_str, autopath_specifier))):
        fut = app_stdout()

        # we don't have to wait for a result to check this attributes
        assert fut.stdout is autopath_specifier

        # there is no DataFuture to wait for in the str case: the model is that
        # the stdout will be immediately available on task completion.
        fut.result()

    with open(content_path, "r") as file:
        assert file.readlines() == ["hello\n"]

    for record in caplog.records:
        assert record.levelno < logging.ERROR


@pytest.mark.local
def test_std_autopath_const_str(caplog, tmpd_cwd):
    """Tests str and tuple mode autopaths with constant autopath, which should
    all be passed through unmodified.
    """
    cpath = str(tmpd_cwd / "CONST")
    const_with_cpath(cpath, cpath, caplog)


@pytest.mark.local
def test_std_autopath_const_pathlike(caplog, tmpd_cwd):
    cpath = tmpd_cwd / "CONST"
    const_with_cpath(cpath, cpath, caplog)


@pytest.mark.local
def test_std_autopath_const_tuples(caplog, tmpd_cwd):
    file = tmpd_cwd / "CONST"
    cpath = (file, "w")
    const_with_cpath(cpath, file, caplog)


class URIFailError(Exception):
    pass


def fail_uri(task_record, err_or_out):
    raise URIFailError("Deliberate failure in std stream filename generation")


@pytest.mark.local
def test_std_autopath_fail(caplog):
    with parsl.load(parsl.Config(std_autopath=fail_uri)):
        with pytest.raises(URIFailError):
            app_stdout()


@parsl.bash_app
def app_both(stdout=parsl.AUTO_LOGNAME, stderr=parsl.AUTO_LOGNAME):
    return "echo hello; echo goodbye >&2"


def zip_uri(base, task_record, err_or_out):
    """Should generate Files in base.zip like app_both.0.out or app_both.123.err"""
    zip_path = base / "base.zip"
    file = f"{task_record['func_name']}.{task_record['id']}.{task_record['try_id']}.{err_or_out}"
    return File(f"zip:{zip_path}/{file}")


@pytest.mark.local
def test_std_autopath_zip(caplog, tmpd_cwd):
    with parsl.load(parsl.Config(run_dir=str(tmpd_cwd),
                                 executors=[ThreadPoolExecutor(working_dir=str(tmpd_cwd))],
                                 std_autopath=partial(zip_uri, tmpd_cwd))):
        futs = []

        for _ in range(10):
            fut = app_both()

            # assertions that should hold after submission
            assert isinstance(fut.stdout, DataFuture)
            assert fut.stdout.file_obj.url.startswith("zip")

            futs.append(fut)

        # Barrier for all the stageouts to complete so that we can
        # poke at the zip file.
        [(fut.stdout.result(), fut.stderr.result()) for fut in futs]

        with zipfile.ZipFile(tmpd_cwd / "base.zip") as z:
            for fut in futs:

                assert fut.done(), "AppFuture should be done if stageout is done"

                stdout_relative_path = f"app_both.{fut.tid}.0.stdout"
                with z.open(stdout_relative_path) as f:
                    assert f.readlines() == [b'hello\n']

                stderr_relative_path = f"app_both.{fut.tid}.0.stderr"
                with z.open(stderr_relative_path) as f:
                    assert f.readlines()[-1] == b'goodbye\n'

    for record in caplog.records:
        assert record.levelno < logging.ERROR
