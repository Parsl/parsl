import pytest

import parsl.app.errors as pe
from parsl.app.app import bash_app
from parsl.tests.configs.local_threads import fresh_config as local_config


@pytest.fixture
def test_fn(tmpd_cwd):
    def _wrapper(parsl_app):
        serr = str(tmpd_cwd / "std.err")
        sout = str(tmpd_cwd / "std.out")
        return parsl_app(stdout=sout, stderr=serr)

    return _wrapper


@bash_app
def command_not_found(stderr='std.err', stdout='std.out'):
    cmd_line = 'catdogcat'
    return cmd_line


@bash_app
def bash_misuse(stderr='std.err', stdout='std.out'):
    cmd_line = 'exit 15'
    return cmd_line


@bash_app
def div_0(stderr='std.err', stdout='std.out'):
    cmd_line = '$((5/0))'
    return cmd_line


@bash_app
def not_executable(stderr='std.err', stdout='std.out'):
    cmd_line = '/dev/null'
    return cmd_line


exit_codes = {
    div_0: 1,
    bash_misuse: 15,
    not_executable: 126,
    command_not_found: 127,
}


@pytest.mark.shared_fs
@pytest.mark.parametrize("app", (div_0, bash_misuse, not_executable, command_not_found))
def test_failing_bash_app(test_fn, app):
    err_code = exit_codes[app]
    with pytest.raises(pe.BashExitFailure) as pyt_e:
        test_fn(app).result()
    e: pe.BashExitFailure = pyt_e.value
    assert e.exitcode == err_code, f"[{e}] {app.__name__} expected {err_code=} but got {e.exitcode}"
