import os
import pickle
import sys
from argparse import ArgumentError
from unittest import mock

import pytest

from parsl.app.errors import RemoteExceptionWrapper
from parsl.executors.high_throughput import process_worker_pool
from parsl.executors.high_throughput.process_worker_pool import worker
from parsl.multiprocessing import SpawnContext
from parsl.serialize.facade import deserialize

if sys.version_info < (3, 12):
    # exit_on_error bug; see https://github.com/python/cpython/issues/121018
    # "argparse.ArgumentParser.parses_args does not honor exit_on_error=False when
    #  given unrecognized arguments"
    pytest.skip(allow_module_level=True, reason="exit_on_error argparse bug")

# due to above pytest.skip, mypy on < Py312 thinks this is unreachable.  :facepalm:
_known_required = (  # type: ignore[unreachable, unused-ignore]
    "--cert_dir",
    "--cpu-affinity",
    "--port",
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
def test_arg_parser_required(req):
    p = process_worker_pool.get_arg_parser()
    p.exit_on_error = False
    with pytest.raises(ArgumentError) as pyt_exc:
        p.parse_args([])

    e_msg = pyt_exc.value.args[1]
    assert req in e_msg


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
    reqd_args.extend(("--port", "123"))
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


def _always_raise(*a, **k):
    raise ArithmeticError(f"{a=}\n{k=}")


@pytest.mark.local
def test_worker_dynamic_import_happy_path(tmpd_cwd):
    import_str = f"{_always_raise.__module__}.{_always_raise.__name__}"
    task_exec = {
        "f": import_str,
        "a": (1, 2),
        "k": {"a": "b"},
    }
    req = {
        "task_id": 15,
        "context": {"task_executor": task_exec},
        "buffer": b"some serialized value"
    }

    try:
        task_args = [req["buffer"]]
        task_args.extend(task_exec["a"])
        _always_raise(*task_args, **task_exec["k"])
    except Exception as e:
        exp_exc = e
    else:
        raise RuntimeError("Test failure; this branch should not run")

    q = mock.Mock(side_effect=(req, MemoryError("intentional test error")))
    q.get = q

    block_id = "bid"
    worker_id = 1
    pool = 1
    (tmpd_cwd / f"block-{block_id}/{worker_id}").mkdir(parents=True)
    with pytest.raises(MemoryError):
        worker(
            worker_id,
            pool_id=str(pool),
            pool_size=pool,
            task_queue=q,
            result_queue=q,
            monitoring_queue=None,
            ready_worker_count=SpawnContext.Value("i", 0),
            tasks_in_progress={},
            cpu_affinity="none",
            accelerator=None,
            block_id=block_id,
            task_queue_timeout=0,
            manager_pid=os.getpid(),
            logdir=str(tmpd_cwd),
            debug=True,
            mpi_launcher="",
        )
    (result_pkl,), _ = q.put.call_args
    r = pickle.loads(result_pkl)
    assert "exception" in r
    wrapped_exc: RemoteExceptionWrapper = deserialize(r["exception"])
    exc = wrapped_exc.get_exception()
    assert isinstance(exc, type(exp_exc)), "Approximate equality"
    assert str(exp_exc) == str(exc), "Approximate equality; all args, kwargs conveyed"


@pytest.mark.local
def test_worker_bad_dynamic_import(tmpd_cwd):
    req = {
        "task_id": 15,
        "context": {
            "task_executor": {
                "f": "parsl.some.not_existing.module.__nope",
                "a": (1, 2),
                "k": {"a": "b"},
            },
        },
        "buffer": b"some serialized value"
    }

    q = mock.Mock(side_effect=(req, MemoryError("intentional test error")))
    q.get = q

    block_id = "bid"
    worker_id = 1
    pool = 1
    (tmpd_cwd / f"block-{block_id}/{worker_id}").mkdir(parents=True)
    with pytest.raises(MemoryError):
        worker(
            worker_id,
            pool_id=str(pool),
            pool_size=pool,
            task_queue=q,
            result_queue=q,
            monitoring_queue=None,
            ready_worker_count=SpawnContext.Value("i", 0),
            tasks_in_progress={},
            cpu_affinity="none",
            accelerator=None,
            block_id=block_id,
            task_queue_timeout=0,
            manager_pid=os.getpid(),
            logdir=str(tmpd_cwd),
            debug=True,
            mpi_launcher="",
        )
    (result_pkl,), _ = q.put.call_args
    r = pickle.loads(result_pkl)
    assert "exception" in r
    wrapped_exc: RemoteExceptionWrapper = deserialize(r["exception"])
    exc = wrapped_exc.get_exception()
    assert isinstance(exc, ModuleNotFoundError)
    assert "No module named" in str(exc)
