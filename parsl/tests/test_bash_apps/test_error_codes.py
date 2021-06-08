import argparse
import os

import pytest

import parsl
from parsl.app.app import bash_app
import parsl.app.errors as pe


from parsl.tests.configs.local_threads import config


local_config = config


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
def invalid_exit(stderr='std.err', stdout='std.out'):
    cmd_line = 'exit 3.141'
    return cmd_line


@bash_app
def not_executable(stderr='std.err', stdout='std.out'):
    cmd_line = '/dev/null'
    return cmd_line


@bash_app
def bad_format(stderr='std.err', stdout='std.out'):
    cmd_line = 'echo {0}'
    return cmd_line


test_matrix = {
    div_0: {
        'exit_code': 1
    },
    bash_misuse: {
        'exit_code': 15
    },
    command_not_found: {
        'exit_code': 127
    },
    invalid_exit: {
        'exit_code': 128
    },
    not_executable: {
        'exit_code': 126
    }
}

whitelist = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs', '*threads*')


# @pytest.mark.whitelist(whitelist, reason='broken in IPP')
@pytest.mark.skip("Broke somewhere between PR #525 and PR #652")
def test_bash_formatting():

    f = bad_format()
    try:
        f.result()
    except Exception as e_:
        print("Caught exception", e_)
        assert isinstance(e_, parsl.app.errors.AppBadFormatting), (
            f"Expected AppBadFormatting got: {e_}")
    return True


# @pytest.mark.whitelist(whitelist, reason='broken in IPP')
@pytest.mark.skip("Broke somewhere between PR #525 and PR #652")
def test_div_0(test_fn=div_0):
    err_code = test_matrix[test_fn]['exit_code']
    f = test_fn()
    try:
        f.result()
    except Exception as e_:
        print("Caught exception", e_)
        assert e_.exitcode == err_code, (f"{test_fn.__name__} "
                                         f"expected err_code:{err_code} "
                                         f"but got {e_.exitcode}")
    print(os.listdir('.'))
    os.remove('std.err')
    os.remove('std.out')
    return True


@pytest.mark.issue363
def test_bash_misuse(test_fn=bash_misuse):
    err_code = test_matrix[test_fn]['exit_code']
    f = test_fn()
    try:
        f.result()
    except pe.BashExitFailure as e_:
        print("Caught expected BashExitFailure", e_)
        assert e_.exitcode == err_code, (f"{test_fn.__name__} "
                                         f"expected err_code:{err_code} "
                                         f"but got {e_.exitcode}")
    os.remove('std.err')
    os.remove('std.out')


@pytest.mark.issue363
def test_command_not_found(test_fn=command_not_found):
    err_code = test_matrix[test_fn]['exit_code']
    f = test_fn()
    try:
        f.result()
    except pe.BashExitFailure as e_:
        print("Caught exception", e_)
        assert e_.exitcode == err_code, (f"{test_fn.__name__} "
                                         f"expected err_code:{err_code} "
                                         f"but got {e_.exitcode}")

    os.remove('std.err')
    os.remove('std.out')
    return True


@pytest.mark.skip('broken')
def test_invalid_exit(test_fn=invalid_exit):
    err_code = test_matrix[test_fn]['exit_code']
    f = test_fn()
    try:
        f.result()
    except Exception as e_:
        print("Caught exception", e_)
        assert e_.exitcode == err_code, (f"{test_fn.__name__} "
                                         f"expected err_code:{err_code} "
                                         f"but got {e_.exitcode}")
    os.remove('std.err')
    os.remove('std.out')
    return True


@pytest.mark.issue363
def test_not_executable(test_fn=not_executable):
    err_code = test_matrix[test_fn]['exit_code']
    f = test_fn()
    try:
        f.result()
    except Exception as e_:
        print("Caught exception", e_)
        assert e_.exitcode == err_code, (f"{test_fn.__name__} "
                                         f"expected err_code:{err_code} "
                                         f"but got {e_.exitcode}")
    os.remove('std.err')
    os.remove('std.out')
    return True


def run_app(test_fn, err_code):
    f = test_fn()
    print(f)
    try:
        f.result()
    except Exception as e_:
        print("Caught exception", e_)
        assert e_.exitcode == err_code, (f"{test_fn.__name__} "
                                         f"expected err_code:{err_code} "
                                         f"but got {e_.exitcode}")
    os.remove('std.err')
    os.remove('std.out')
    return True


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    print(test_bash_formatting())

    exit(0)

    if args.debug:
        parsl.set_stream_logger()

    for test in test_matrix:
        try:
            run_app(test, test_matrix[test]['exit_code'])
        except AssertionError as e:
            print(f"Test {test.__name__:15} [FAILED]")
            print(f"Caught error : {e}")
        else:
            print(f"Test {test.__name__:15} [SUCCESS]")
