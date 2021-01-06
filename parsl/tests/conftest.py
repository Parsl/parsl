import importlib.util
import logging
import os
from glob import glob
from itertools import chain
import signal
import sys
import threading
import traceback

import pytest
import _pytest.runner as runner

import parsl
from parsl.dataflow.dflow import DataFlowKernelLoader

logger = logging.getLogger('parsl')


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


def pytest_addoption(parser):
    """Add parsl-specific command-line options to pytest.
    """
    parser.addoption(
        '--config',
        action='store',
        metavar='CONFIG',
        type='string',
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
        'noci: mark test to be unsuitable for running during automated tests'
    )

    config.addinivalue_line(
        'markers',
        'cleannet: Enable tests that require a clean network connection (such as for testing FTP)'
    )
    config.addinivalue_line(
        'markers',
        'issue363: Marks tests that require a shared filesystem for stdout/stderr - see issue #363'
    )
    config.addinivalue_line(
        'markers',
        'staging_required: Marks tests that require a staging provider, when there is no sharedFS)'
    )


@pytest.fixture(autouse=True, scope='session')
def load_dfk_session(request, pytestconfig):
    """Load a dfk around entire test suite, except in local mode.

    The special path `local` indicates that configuration will not come
    from a pytest managed configuration file; in that case, see
    load_dfk_local_module for module-level configuration management.
    """

    config = pytestconfig.getoption('config')[0]

    if config != 'local':
        spec = importlib.util.spec_from_file_location('', config)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if DataFlowKernelLoader._dfk is not None:
            raise ValueError("DFK didn't start as None - there was a DFK from somewhere already")

        dfk = parsl.load(module.config)

        yield

        if(parsl.dfk() != dfk):
            raise ValueError("DFK changed unexpectedly during test")
        dfk.cleanup()
        parsl.clear()
    else:
        yield


@pytest.fixture(autouse=True, scope='module')
def load_dfk_local_module(request, pytestconfig):
    """Load the dfk around test modules, in local mode.

    If local_config is specified in the test module, it will be loaded using
    parsl.load. It should be a parsl Config() object.

    If local_setup and/or local_teardown are callables (such as functions) in
    the test module, they they will be invoked before/after the tests. This
    can be used to perform more interesting DFK initialisation not possible
    with local_config.
    """

    config = pytestconfig.getoption('config')[0]

    if config == 'local':
        local_setup = getattr(request.module, "local_setup", None)
        local_teardown = getattr(request.module, "local_teardown", None)
        local_config = getattr(request.module, "local_config", None)

        if(local_config):
            dfk = parsl.load(local_config)

        if(callable(local_setup)):
            local_setup()

        yield

        if(callable(local_teardown)):
            local_teardown()

        if(local_config):
            if(parsl.dfk() != dfk):
                raise ValueError("DFK changed unexpectedly during test")
            dfk.cleanup()
            parsl.clear()

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
                pytest.skip('skipping non-local config')
            else:
                pytest.skip(m.args[0])
    else:  # is not marked as local
        if config == 'local':
            pytest.skip('skipping local config')


@pytest.fixture()
def setup_data():
    import os
    if not os.path.isdir('data'):
        os.mkdir('data')

    with open("data/test1.txt", 'w') as f:
        f.write("1\n")
    with open("data/test2.txt", 'w') as f:
        f.write("2\n")


@pytest.fixture(autouse=True, scope='function')
def wait_for_task_completion(pytestconfig):
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
        parsl.dfk().wait_for_current_tasks()


def pytest_make_collect_report(collector):
    call = runner.CallInfo.from_call(lambda: list(collector.collect()), 'collect')
    longrepr = None
    if not call.excinfo:
        outcome = "passed"
    else:
        from _pytest import nose
        from _pytest.outcomes import Skipped
        skip_exceptions = (Skipped,) + nose.get_skip_exceptions()
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
    elif 'workqueue_tests/test_scale' in path.strpath:
        return True
    else:
        return False
