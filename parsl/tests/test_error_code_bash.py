from parsl import *
import parsl

import os
import time
import shutil
import argparse
workers = ThreadPoolExecutor(max_workers=4)

@App('bash', workers)
def command_not_found(stderr='std.err', stdout='std.out'):
    cmd_line = 'catdogcat'

@App('bash', workers)
def bash_misuse(stderr='std.err', stdout='std.out'):
    cmd_line = 'if else'

@App('bash', workers)
def div_0(stderr='std.err', stdout='std.out'):
    cmd_line = '$((5/0))'

@App('bash', workers)
def invalid_exit(stderr='std.err', stdout='std.out'):
    cmd_line = 'exit 3.141'

@App('bash', workers)
def not_executable(stderr='std.err', stdout='std.out'):
    cmd_line = '/dev/null'

test_matrix = { div_0             : {'exit_code' : 1 },
                bash_misuse       : {'exit_code' : 2 },
                command_not_found : {'exit_code' : 127 },
                invalid_exit      : {'exit_code' : 128 },
                not_executable    : {'exit_code' : 126 } }

def run_test(test_fn, err_code):

    f, _ = test_fn()
    assert f.result() == err_code, "{0} expected err_code:{1} but got {2}".format(test_fn.__name__,
                                                                                  err_code,
                                                                                  f.result())
    os.remove('std.err')
    os.remove('std.out')
    return True

if __name__ == '__main__' :

    parser   = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action='store_true', help="Count of apps to launch")
    args   = parser.parse_args()

    for test in test_matrix:
        try:
            run_test(test, test_matrix[test]['exit_code'])
        except AssertionError as e:
            print("Test {0:15} [FAILED]".format(test.__name__))
            print("Caught error : {0}".format(e))
        else:
            print("Test {0:15} [SUCCESS]".format(test.__name__))
