import os
import random
import re

import pytest

import parsl
from parsl import File
from parsl.app.app import bash_app


@bash_app
def echo_to_file(inputs=(), outputs=(), stderr=None, stdout=None):
    res = ""
    for o in outputs:
        for i in inputs:
            res += "echo {} >& {}".format(i, o)
    return res


@bash_app
def foo(x, y, z=10, stdout=None, label=None):
    return f"echo {x} {y} {z}"


@pytest.mark.issue363
def test_command_format_1(tmpd_cwd):
    """Testing command format for BashApps"""

    outdir = tmpd_cwd / "outputs"
    outdir.mkdir()
    stdout = outdir / "foo-std.out"

    foo_future = foo(1, 4, stdout=str(stdout))
    assert foo_future.result() == 0, "BashApp had non-zero exit code"

    so_content = stdout.read_text().strip()
    assert so_content == "1 4 10"


@pytest.mark.issue363
def test_auto_log_filename_format():
    """Testing auto log filename format for BashApps
    """
    app_label = "label_test_auto_log_filename_format"
    rand_int = random.randint(1000, 1000000000)

    foo_future = foo(1, rand_int, stdout=parsl.AUTO_LOGNAME, label=app_label)

    assert foo_future.result() == 0, "BashApp exited with an error code : {0}".format(
        foo_future.result())

    log_fpath = foo_future.stdout
    log_pattern = fr".*/task_\d+_foo_{app_label}"
    assert re.match(log_pattern, log_fpath), 'Output file "{0}" does not match pattern "{1}"'.format(
        log_fpath, log_pattern)
    assert os.path.exists(log_fpath), 'Output file does not exist "{0}"'.format(log_fpath)
    with open(log_fpath, 'r') as stdout_f:
        contents = stdout_f.read()

    assert contents == '1 {0} 10\n'.format(rand_int), \
        'Output does not match expected string "1 {0} 10", Got: "{1}"'.format(rand_int, contents)


@pytest.mark.issue363
def test_parallel_for(tmpd_cwd, n=3):
    """Testing a simple parallel for loop"""
    outdir = tmpd_cwd / "outputs/test_parallel"
    outdir.mkdir(parents=True)
    futs = [
        echo_to_file(
            inputs=[f"Hello World {i}"],
            outputs=[File(str(outdir / f"out.{i}.txt"))],
            stdout=str(outdir / f"std.{i}.out"),
            stderr=str(outdir / f"std.{i}.err"),
        )
        for i in range(n)
    ]

    for f in futs:
        f.result()

    stdout_file_count = len(list(outdir.glob("*.out")))
    assert stdout_file_count == n, sorted(outdir.iterdir())
