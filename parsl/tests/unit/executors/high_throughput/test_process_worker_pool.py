import sys
from argparse import ArgumentError

import pytest

from parsl.executors.high_throughput import process_worker_pool

if sys.version_info < (3, 12):
    # exit_on_error bug; see https://github.com/python/cpython/issues/121018
    # "argparse.ArgumentParser.parses_args does not honor exit_on_error=False when
    #  given unrecognized arguments"
    pytest.skip(allow_module_level=True, reason="exit_on_error argparse bug")

# due to above pytest.skip, mypy on < Py312 thinks this is unreachable.  :facepalm:
_known_required = (  # type: ignore[unreachable, unused-ignore]
    "--cert_dir",
    "--cpu-affinity",
    "--result_port",
    "--task_port",
    "--addresses",
)


@pytest.mark.local
def test_arg_parser_exits_on_error():
    p = process_worker_pool.get_arg_parser()
    assert p.exit_on_error


@pytest.mark.local
def test_arg_parser_known_required():
    p = process_worker_pool.get_arg_parser()
    reqd = [a for a in p._actions if a.required]
    for a in reqd:
        assert a.option_strings[-1] in _known_required, "Update _known_required?"


@pytest.mark.local
@pytest.mark.parametrize("req", _known_required)
def test_arg_parser_required(req, capsys):
    p = process_worker_pool.get_arg_parser()
    p.exit_on_error = False
    with pytest.raises(SystemExit):
        p.parse_args([])

    captured = capsys.readouterr()
    assert req in captured.err


@pytest.mark.local
@pytest.mark.parametrize("valid,val", (
        (True, "NoNe"),
        (True, "none"),
        (True, "block"),
        (True, "alternating"),
        (True, "block-reverse"),
        (True, "list"),
        (False, "asdf"),
        (False, ""),
))
def test_arg_parser_validates_cpu_affinity(valid, val):
    reqd_args = []
    reqd_args.extend(("--cert_dir", "/some/path"))
    reqd_args.extend(("--result_port", "123"))
    reqd_args.extend(("--task_port", "123"))
    reqd_args.extend(("--addresses", "asdf"))
    reqd_args.extend(("--cpu-affinity", val))

    p = process_worker_pool.get_arg_parser()
    p.exit_on_error = False
    if valid:
        p.parse_args(reqd_args)
    else:
        with pytest.raises(ArgumentError) as pyt_exc:
            p.parse_args(reqd_args)
        assert "must be one of" in pyt_exc.value.args[1]
