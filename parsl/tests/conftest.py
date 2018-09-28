import importlib.util
import logging
import os
import shutil
import subprocess
from glob import glob
from itertools import chain

import pytest
import _pytest.runner as runner
from pytest_forked import forked_run_report

import parsl
from parsl.tests.utils import get_rundir

logger = logging.getLogger('parsl')


def pytest_addoption(parser):
    """Add parsl-specific command-line options to pytest.
    """
    parser.addoption(
        '--configs',
        action='store',
        metavar='CONFIG',
        nargs='*',
        help="only run parsl CONFIG; use 'local' to run locally-defined config"
    )
    parser.addoption(
        '--basic', action='store_true', default=False, help='only run basic configs (local, local_ipp and local_threads)'
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
        'forked: mark test to only run in a subprocess'
    )


def pytest_generate_tests(metafunc):
    """Assemble the list of configs to test.
    """
    config_dir = os.path.join(os.path.dirname(__file__), 'configs')

    configs = metafunc.config.getoption('configs')
    basic = metafunc.config.getoption('basic')
    if basic:
        configs = ['local'] + [os.path.join(config_dir, x) for x in ['local_threads.py', 'local_ipp.py']]
    elif configs is None:
        configs = ['local']
        for dirpath, _, filenames in os.walk(config_dir):
            for fn in filenames:
                path = os.path.join(dirpath, fn)
                if ('pycache' not in path) and path.endswith('.py'):
                    configs += [path]

    metafunc.parametrize('config', configs, scope='session')


@pytest.fixture(scope='session')
def setup_docker():
    """Set up containers for docker tests.

    Rather than installing Parsl from PyPI, the current state of the source is
    copied into the container. In this way we ensure that what we are testing
    stays synced with the current state of the code.
    """
    if shutil.which('docker') is not None:
        subprocess.call(['docker', 'pull', 'python'])
        pdir = os.path.join(os.path.dirname(os.path.dirname(parsl.__file__)))
        template = """
        FROM python:3.6
        WORKDIR {home}
        COPY ./parsl .
        COPY ./requirements.txt .
        COPY ./setup.py .
        RUN python3 setup.py install
        {add}
        """
        with open(os.path.join(pdir, 'docker', 'Dockerfile'), 'w') as f:
            print(template.format(home=os.environ['HOME'], add=''), file=f)
        cmd = ['docker', 'build', '-t', 'parslbase_v0.1', '-f', 'docker/Dockerfile', '.']
        subprocess.call(cmd, cwd=pdir)
        for app in ['app1', 'app2']:
            with open(os.path.join(pdir, 'docker', app, 'Dockerfile'), 'w') as f:
                add = 'ADD ./docker/{}/{}.py {}'.format(app, app, os.environ['HOME'])
                print(template.format(home=os.environ['HOME'], add=add), file=f)
            cmd = ['docker', 'build', '-t', '{}_v0.1'.format(app), '-f', 'docker/{}/Dockerfile'.format(app), '.']
            subprocess.call(cmd, cwd=pdir)


@pytest.fixture(autouse=True)
def load_dfk(config):
    """Load the dfk before running a test.

    The special path `local` indicates that whatever configuration is loaded
    locally in the test should not be replaced. Otherwise, it is expected that
    the supplied file contains a dictionary called `config`, which will be
    loaded before the test runs.

    Args:
        config (str) : path to config to load (this is a parameterized pytest fixture)
    """
    if config != 'local':
        spec = importlib.util.spec_from_file_location('', config)
        try:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            module.config.run_dir = get_rundir()  # Give unique rundir; needed running with -n=X where X > 1.
            parsl.clear()
            dfk = parsl.load(module.config)
            yield
            dfk.cleanup()
        except KeyError:
            pytest.skip('options in user_opts.py not configured for {}'.format(config))
    else:
        yield


@pytest.fixture(autouse=True)
def apply_masks(request):
    """Apply whitelist, blacklist, and local markers.

    These ensure that if a whitelist decorator is applied to a test, configs which are
    not in the whitelist are skipped. Similarly, configs in a blacklist are skipped,
    and configs which are not `local` are skipped if the `local` decorator is applied.
    """
    config = request.getfixturevalue('config')
    m = request.node.get_marker('whitelist')
    if m is not None:
        if os.path.abspath(config) not in chain.from_iterable([glob(x) for x in m.args]):
            if 'reason' not in m.kwargs:
                pytest.skip("config '{}' not in whitelist".format(config))
            else:
                pytest.skip(m.kwargs['reason'])
    m = request.node.get_marker('blacklist')
    if m is not None:
        if os.path.abspath(config) in chain.from_iterable([glob(x) for x in m.args]):
            if 'reason' not in m.kwargs:
                pytest.skip("config '{}' is in blacklist".format(config))
            else:
                pytest.skip(m.kwargs['reason'])
    m = request.node.get_closest_marker('local')
    if m is not None:
        if config != 'local':
            if len(m.args) == 0:
                pytest.skip('skipping non-local config')
            else:
                pytest.skip(m.args[0])


@pytest.fixture()
def setup_data():
    import os
    if not os.path.isdir('data'):
        os.mkdir('data')

    with open("data/test1.txt", 'w') as f:
        f.write("1\n")
    with open("data/test2.txt", 'w') as f:
        f.write("2\n")


@pytest.mark.tryfirst
def pytest_runtest_protocol(item):
    if 'forked' in item.keywords:
        reports = forked_run_report(item)
        for rep in reports:
            item.ihook.pytest_runtest_logreport(report=rep)
        return True


def pytest_make_collect_report(collector):
    call = runner.CallInfo(lambda: list(collector.collect()), 'collect')
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
