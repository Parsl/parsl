import argparse
import os
import shutil
import time

import parsl
from parsl.app.app import App

from parsl.tests.configs.local_threads import config


@App('bash')
def foo(x=0, y=1, z=2, stdout=None):
    return """echo {0} {1} {z}
    """


def test_command_format_1():
    """Testing command format for BashApps
    """

    stdout = os.path.abspath('std.out')
    if os.path.exists(stdout):
        os.remove(stdout)

    app_fu = foo(3, 4, stdout=stdout)
    app_fu2 = foo(5, 6, stdout=stdout)
    print("app_fu : ", app_fu)
    print("app_fu2 : ", app_fu2)
    contents = None

    assert app_fu.result() == 0, "BashApp exited with an error code : {0}".format(
        app_fu.result())

    with open(stdout, 'r') as stdout_f:
        contents = stdout_f.read()
        print("Contents : ", contents)

    if os.path.exists('stdout_file'):
        os.remove(stdout)

    assert contents == '3 4 2\n', 'Output does not match expected string "1 4", Got: "{0}"'.format(
        contents)
    return True
