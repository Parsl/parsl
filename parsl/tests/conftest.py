import importlib.util
import itertools
import logging
import os
import pathlib
import random
import re
import shutil
import signal
import string
import sys
import tempfile
import threading
import time
import traceback
import types
import typing as t
from datetime import datetime
from glob import glob
from itertools import chain

import _pytest.runner as runner
import pytest

import parsl
from parsl.dataflow.dflow import DataFlowKernelLoader
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


def dumpstacks(sig, frame):
    s = ''
    try:
        thread_names = {thread.ident: thread.name for thread in threading.enumerate()}
        tf = sys._current_frames()
        for thread_id, frame in tf.items():
            s += '\n\nThread: %s (%d)' % (thread_names[thread_id], thread_id)
            s += ''.join(traceback.format_stack(frame))
    except Exception:
        s = traceback.format_exc()
    with open(os.getenv('HOME') + '/parsl_stack_dump.txt', 'w') as f:
        f.write(s)
    print(s)


def pytest_sessionstart(session):
    signal.signal(signal.SIGUSR1, dumpstacks)


@pytest.fixture(scope="session")
def tmpd_cwd_session(pytestconfig):
    def _chmod(path: pathlib.Path, mode: int):
        try:
            path.lchmod(mode)  # support BSD and derivatives
        except NotImplementedError:
            path.chmod(mode)

    config = re.sub(r"[^A-z0-9_-]+", "_", pytestconfig.getoption('config')[0])
    cwd = pathlib.Path(os.getcwd())
    pytest_dir = cwd / "pytest-parsl"
    pytest_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

    test_dir_prefix = "parsltest-"
    link = pytest_dir / f"{test_dir_prefix}current"
    link.unlink(missing_ok=True)
    n = datetime.now().strftime('%Y%m%d.%H%M%S')
    tmpd = tempfile.mkdtemp(dir=pytest_dir, prefix=f"{test_dir_prefix}{n}-{config}-")
    tmpd = pathlib.Path(tmpd)
    link.symlink_to(tmpd.name)
    yield link

    try:
        preserve = int(os.getenv("PARSL_TEST_PRESERVE_NUM_RUNS", "3"))
    except Exception:
        preserve = 3

    test_runs = sorted(
        d for d in pytest_dir.glob(f"{test_dir_prefix}*")
        if d.is_dir() and not d.is_symlink()
    )
    for run_to_remove in test_runs[:-preserve]:
        run_to_remove.chmod(0o700)
        for root, subdirnames, fnames in os.walk(run_to_remove):
            rpath = pathlib.Path(root)
            for d in subdirnames:
                _chmod(rpath / d, 0o700)
            for f in fnames:
                _chmod(rpath / f, 0o600)
        shutil.rmtree(run_to_remove)


@pytest.fixture
def tmpd_cwd(tmpd_cwd_session, request: pytest.FixtureRequest):
    node_name = re.sub(r'[^\w\s-]', '_', request.node.name)
    prefix = f"{node_name}-"
    tmpd = tempfile.mkdtemp(dir=tmpd_cwd_session, prefix=prefix)
    yield pathlib.Path(tmpd)


def pytest_addoption(parser):
    """Add parsl-specific command-line options to pytest.
    """
    parser.addoption(
        '--config',
        action='store',
        metavar='CONFIG',
        type=str,
        nargs=1,
        required=True,
        help="run with parsl CONFIG; use 'local' to run locally-defined config"
    )


def pytest_configure(config):
    """Configure help for parsl-specific pytest decorators.

    This help is returned by `pytest --markers`.
    """
    config.addinivalue_line(
        'markers',
        'whitelist(config1, config2, ..., reason=None): mark test to run only on named configs. '
        'Wildcards (*) are accepted. If `reason` is supplied, it will be included in the report.'
    )
    config.addinivalue_line(
        'markers',
        'blacklist(config1, config2, ..., reason=None): mark test to skip named configs. '
        'Wildcards (*) are accepted. If `reason` is supplied, it will be included in the report.'
    )
    config.addinivalue_line(
        'markers',
        'local: mark test to only run locally-defined config.'
    )
    config.addinivalue_line(
        'markers',
        'local(reason): mark test to only run locally-defined config; report will include supplied reason.'
    )
    config.addinivalue_line(
        'markers',
        'cleannet: Enable tests that require a clean network connection (such as for testing FTP)'
    )
    config.addinivalue_line(
        'markers',
        'staging_required: Marks tests that require a staging provider, when there is no sharedFS'
    )
    config.addinivalue_line(
        'markers',
        'multiple_cores_required: Marks tests that require multiple cores, such as htex affinity'
    )
    config.addinivalue_line(
        'markers',
        'unix_filesystem_permissions_required: Marks tests that require unix-level filesystem permission enforcement'
    )
    config.addinivalue_line(
        'markers',
        'issue3328: Marks tests broken by issue #3328'
    )
    config.addinivalue_line(
        'markers',
        'executor_supports_std_stream_tuples: Marks tests that require tuple support for stdout/stderr'
    )
    config.addinivalue_line(
        'markers',
        'shared_fs: Marks tests that require a shared_fs between the workers are the test client'
    )
    config.addinivalue_line(
        'markers',
        'issue_3620: Marks tests that do not work correctly on GlobusComputeExecutor (ref: issue 3620)'
    )


@pytest.fixture(autouse=True, scope='session')
def load_dfk_session(request, pytestconfig, tmpd_cwd_session):
    """Load a dfk around entire test suite, except in local mode.

    The special path `local` indicates that configuration will not come
    from a pytest managed configuration file; in that case, see
    load_dfk_local_module for module-level configuration management.
    """

    RepresentationMixin._validate_repr = True

    config = pytestconfig.getoption('config')[0]

    if config != 'local':
        spec = importlib.util.spec_from_file_location('', config)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if DataFlowKernelLoader._dfk is not None:
            raise RuntimeError("DFK didn't start as None - there was a DFK from somewhere already")

        if hasattr(module, 'config'):
            parsl_conf = module.config
        elif hasattr(module, 'fresh_config'):
            parsl_conf = module.fresh_config()
        else:
            raise RuntimeError("Config module does not define config or fresh_config")

        if parsl_conf.run_dir == "runinfo":  # the default
            parsl_conf.run_dir = tmpd_cwd_session / parsl_conf.run_dir
        dfk = parsl.load(parsl_conf)

        for ex in dfk.executors.values():
            ex.working_dir = tmpd_cwd_session

        yield

        if parsl.dfk() != dfk:
            raise RuntimeError("DFK changed unexpectedly during test")
        dfk.cleanup()
        assert DataFlowKernelLoader._dfk is None
    else:
        yield


@pytest.fixture(autouse=True, scope='module')
def load_dfk_local_module(request, pytestconfig, tmpd_cwd_session):
    """Load the dfk around test modules, in local mode.

    If local_config is specified in the test module, it will be loaded using
    parsl.load. It should be a Callable that returns a parsl Config object.

    If local_setup and/or local_teardown are callables (such as functions) in
    the test module, they will be invoked before/after the tests. This can
    be used to perform more interesting DFK initialisation not possible with
    local_config.
    """
    RepresentationMixin._validate_repr = True

    config = pytestconfig.getoption('config')[0]

    if config == 'local':
        local_setup = getattr(request.module, "local_setup", None)
        local_teardown = getattr(request.module, "local_teardown", None)
        local_config = getattr(request.module, "local_config", None)

        if local_config:
            assert callable(local_config)
            c = local_config()
            assert isinstance(c, parsl.Config)

            if c.run_dir == "runinfo":  # the default
                c.run_dir = tmpd_cwd_session / c.run_dir

            dfk = parsl.load(c)

            for ex in dfk.executors.values():
                ex.working_dir = tmpd_cwd_session

        if callable(local_setup):
            local_setup()

        yield

        if callable(local_teardown):
            local_teardown()
            assert DataFlowKernelLoader._dfk is None, "Expected teardown to clear DFK"

        if local_config:
            if parsl.dfk() != dfk:
                raise RuntimeError("DFK changed unexpectedly during test")
            dfk.cleanup()
            assert DataFlowKernelLoader._dfk is None

    else:
        yield


@pytest.fixture(autouse=True)
def apply_masks(request, pytestconfig):
    """Apply whitelist, blacklist, and local markers.

    These ensure that if a whitelist decorator is applied to a test, configs which are
    not in the whitelist are skipped. Similarly, configs in a blacklist are skipped,
    and configs which are not `local` are skipped if the `local` decorator is applied.
    """
    config = pytestconfig.getoption('config')[0]
    m = request.node.get_closest_marker('whitelist')
    if m is not None:
        if os.path.abspath(config) not in chain.from_iterable([glob(x) for x in m.args]):
            if 'reason' not in m.kwargs:
                pytest.skip("config '{}' not in whitelist".format(config))
            else:
                pytest.skip(m.kwargs['reason'])
    m = request.node.get_closest_marker('blacklist')
    if m is not None:
        if os.path.abspath(config) in chain.from_iterable([glob(x) for x in m.args]):
            if 'reason' not in m.kwargs:
                pytest.skip("config '{}' is in blacklist".format(config))
            else:
                pytest.skip(m.kwargs['reason'])
    m = request.node.get_closest_marker('local')
    if m is not None:  # is marked as local
        if config != 'local':
            if len(m.args) == 0:
                pytest.skip('intended for local config')
            else:
                pytest.skip(m.args[0])
    else:  # is not marked as local
        if config == 'local':
            pytest.skip('intended for explicit config')


@pytest.fixture
def setup_data(tmpd_cwd):
    data_dir = tmpd_cwd / "data"
    data_dir.mkdir()

    (data_dir / "test1.txt").write_text("1\n")
    (data_dir / "test2.txt").write_text("2\n")
    return data_dir


@pytest.fixture(autouse=True, scope='function')
def assert_no_outstanding_tasks(pytestconfig):
    """If we're in a config-file based mode, wait for task completion between
       each test. This will detect early on (by hanging) if particular test
       tasks are not finishing, rather than silently falling off the end of
       the test run with tasks still in progress.
       In local mode, this fixture does nothing, as there isn't anything
       reasonable to assume about DFK behaviour here.
    """
    config = pytestconfig.getoption('config')[0]
    yield
    if config != 'local':
        logger.info("Checking no outstanding tasks")
        for task_record in parsl.dfk().tasks.values():
            fut = task_record['app_fu']
            assert fut.done(), f"Incomplete task found, task id {task_record['id']}"
        logger.info("No outstanding tasks found")


def pytest_make_collect_report(collector):
    call = runner.CallInfo.from_call(lambda: list(collector.collect()), 'collect')
    longrepr = None
    if not call.excinfo:
        outcome = "passed"
    else:
        from _pytest.outcomes import Skipped
        skip_exceptions = (Skipped,)
        if call.excinfo.errisinstance(KeyError):
            outcome = "skipped"
            r = collector._repr_failure_py(call.excinfo, "line").reprcrash
            message = "{} not configured in user_opts.py".format(r.message.split()[-1])
            longrepr = (str(r.path), r.lineno, message)
        elif call.excinfo.errisinstance(skip_exceptions):
            outcome = "skipped"
            r = collector._repr_failure_py(call.excinfo, "line").reprcrash
            longrepr = (str(r.path), r.lineno, r.message)
        else:
            outcome = "failed"
            errorinfo = collector.repr_failure(call.excinfo)
            if not hasattr(errorinfo, "toterminal"):
                errorinfo = runner.CollectErrorRepr(errorinfo)
            longrepr = errorinfo
    rep = runner.CollectReport(collector.nodeid, outcome, longrepr, getattr(call, 'result', None))
    rep.call = call  # see collect_one_node
    return rep


def pytest_ignore_collect(path):
    if 'integration' in path.strpath:
        return True
    elif 'manual_tests' in path.strpath:
        return True
    elif 'scaling_tests/test_scale' in path.strpath:
        return True
    else:
        return False


def create_traceback(start: int = 0) -> t.Optional[types.TracebackType]:
    """
    Dynamically create a traceback.

    Builds a traceback from the top of the stack (the currently executing frame) on
    down to the root frame.  Optionally, use start to build from an earlier stack
    frame.

    N.B. uses `sys._getframe`, which I only know to exist in CPython.
    """
    tb = None
    for depth in itertools.count(start + 1, 1):
        try:
            frame = sys._getframe(depth)
            tb = types.TracebackType(tb, frame, frame.f_lasti, frame.f_lineno)
        except ValueError:
            break
    return tb


@pytest.fixture
def try_assert():
    def _impl(
        test_func: t.Callable[[], bool],
        fail_msg: str = "",
        timeout_ms: float = 5000,
        attempts: int = 0,
        check_period_ms: int = 20,
    ):
        tb = create_traceback(start=1)
        timeout_s = abs(timeout_ms) / 1000.0
        check_period_s = abs(check_period_ms) / 1000.0
        if attempts > 0:
            for _attempt_no in range(attempts):
                if test_func():
                    return
                time.sleep(check_period_s)
            else:
                att_fail = (
                    f"\n  (Still failing after attempt limit [{attempts}], testing"
                    f" every {check_period_ms}ms)"
                )
                exc = AssertionError(f"{str(fail_msg)}{att_fail}".strip())
                raise exc.with_traceback(tb)

        elif timeout_s > 0:
            end = time.monotonic() + timeout_s
            while time.monotonic() < end:
                if test_func():
                    return
                time.sleep(check_period_s)
            att_fail = (
                f"\n  (Still failing after timeout [{timeout_ms}ms], with attempts "
                f"every {check_period_ms}ms)"
            )
            exc = AssertionError(f"{str(fail_msg)}{att_fail}".strip())
            raise exc.with_traceback(tb)

        else:
            raise AssertionError("Bad assert call: no attempts or timeout period")

    yield _impl


@pytest.fixture
def randomstring():
    def func(length=5, alphabet=string.ascii_letters):
        return "".join(random.choice(alphabet) for _ in range(length))

    return func
