import hashlib
import importlib.util
from itertools import chain
import logging
import os
import pickle
import subprocess
import sys
from glob import glob

import pytest

import parsl
from parsl.tests.util import get_rundir

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
        '--basic', action='store_true', default=False, help='only run basic configs (local_ipp and local_threads)'
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
        'isolate: mark test to only run locally-defined config in a subprocess'
    )


def pytest_generate_tests(metafunc):
    """Assemble the list of configs to test.
    """
    config_dir = os.path.join(os.path.dirname(__file__), 'configs')

    configs = metafunc.config.getoption('configs')
    basic = metafunc.config.getoption('basic')
    if basic:
        configs = [os.path.join(config_dir, x) for x in ['local_threads.py', 'local_ipp.py']]
    elif configs is None:
        configs = ['local']
        for dirpath, _, filenames in os.walk(config_dir):
            for fn in filenames:
                path = os.path.join(dirpath, fn)
                if ('pycache' not in path) and path.endswith('.py'):
                    configs += [path]

    metafunc.parametrize('config', configs, scope='session')


@pytest.fixture(scope='session', autouse=True)
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
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if 'globals' not in module.config:
            module.config['globals'] = {}
        module.config['globals']['runDir'] = get_rundir() # Give unique rundir; needed running with -n=X where X > 1.
        parsl.clear()
        parsl.load(module.config)


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
    m = request.node.get_marker('local')
    if m is not None:
        if config != 'local':
            if len(m.args) == 0:
                pytest.skip('skipping non-local config')
            else:
                pytest.skip(m.args[0])


@pytest.mark.tryfirst
def pytest_runtest_protocol(item):
    if item.get_marker('isolate'):
        if item._genid == 'local':
            hashsum = hashlib.md5((item.function.__name__ + str(item.fspath)).encode('utf-8')).hexdigest()
            report_path = '{}.pkl'.format(hashsum)
            if 'PYTEST_SUBPROCESS_RUN' in os.environ:
                dump_reports(item, report_path)
            else:
                reports = load_reports(item, report_path)
                for rep in reports:
                    item.ihook.pytest_runtest_logreport(report=rep)
        return True


def dump_reports(item, path):
    from pytest_boxed import serialize_report
    from _pytest.runner import runtestprotocol
    try:
        reports = runtestprotocol(item, log=False)
    except KeyboardInterrupt:
        sys.exit(pytest.EXIT_INTERRUPTED)
    with open(path, 'wb') as f:
        pickle.dump([serialize_report(x) for x in reports], f)


def load_reports(item, path):
    from pytest_boxed import unserialize_report
    cmd = ['pytest', str(item.fspath), '-k', item.function.__name__]
    env = os.environ.copy()
    env['PYTEST_SUBPROCESS_RUN'] = '1'
    p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.wait()
    if p.returncode == 0:
        with open(path, 'rb') as f:
            reports = [unserialize_report('testreport', x) for x in pickle.load(f)]
    else:
        reports = [report_process_crash(item, p)]
    try:
        os.remove(path)
    except FileNotFoundError:
        pass

    return reports


def report_process_crash(item, process):
    path, lineno = item._getfslineno()
    info = ('{}:{}: running the test returned non-zero exit code {}'.format(path, lineno, process.returncode))
    from _pytest import runner
    call = runner.CallInfo(lambda x: x, 'unknown')
    call.excinfo = info
    rep = runner.pytest_runtest_makereport(item, call)
    if process.stdout:
        rep.sections.append(('captured stdout', process.stdout))
    if process.stderr:
        rep.sections.append(('captured stderr', process.stderr))
    return rep
