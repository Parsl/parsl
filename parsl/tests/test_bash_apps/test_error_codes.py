import argparse
import os

import pytest

import parsl
from parsl.app.app import bash_app
import parsl.app.errors as pe


from parsl.app.errors import BashExitFailure

from parsl.tests.configs.local_threads import fresh_config as local_config


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
    not_executable: {
        'exit_code': 126
    }
}

whitelist = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs', '*threads*')


def test_div_0(test_fn=div_0):
    err_code = test_matrix[test_fn]['exit_code']
    f = test_fn()
    try:
        f.result()
    except BashExitFailure as e:
        print("Caught exception", e)
        assert e.exitcode == err_code, "{0} expected err_code:{1} but got {2}".format(test_fn.__name__,
                                                                                      err_code,
                                                                                      e.exitcode)
    print(os.listdir('.'))
    os.remove('std.err')
    os.remove('std.out')


@pytest.mark.issue363
def test_bash_misuse(test_fn=bash_misuse):
    err_code = test_matrix[test_fn]['exit_code']
    f = test_fn()
    try:
        f.result()
    except pe.BashExitFailure as e:
        print("Caught expected BashExitFailure", e)
        assert e.exitcode == err_code, "{0} expected err_code:{1} but got {2}".format(test_fn.__name__,
                                                                                      err_code,
                                                                                      e.exitcode)
    os.remove('std.err')
    os.remove('std.out')


@pytest.mark.issue363
def test_command_not_found(test_fn=command_not_found):
    err_code = test_matrix[test_fn]['exit_code']
    f = test_fn()
    try:
        f.result()
    except pe.BashExitFailure as e:
        print("Caught exception", e)
        assert e.exitcode == err_code, "{0} expected err_code:{1} but got {2}".format(test_fn.__name__,
                                                                                      err_code,
                                                                                      e.exitcode)

    os.remove('std.err')
    os.remove('std.out')
    return True


@pytest.mark.issue363
def test_not_executable(test_fn=not_executable):
    err_code = test_matrix[test_fn]['exit_code']
    f = test_fn()
    try:
        f.result()
    except BashExitFailure as e:
        print("Caught exception", e)
        assert e.exitcode == err_code, "{0} expected err_code:{1} but got {2}".format(test_fn.__name__,
                                                                                      err_code,
                                                                                      e.exitcode)
    os.remove('std.err')
    os.remove('std.out')
    return True


def run_app(test_fn, err_code):
    f = test_fn()
    print(f)
    try:
        f.result()
    except BashExitFailure as e:
        print("Caught exception", e)
        assert e.exitcode == err_code, "{0} expected err_code:{1} but got {2}".format(test_fn.__name__,
                                                                                      err_code,
                                                                                      e.exitcode)
    os.remove('std.err')
    os.remove('std.out')
    return True
