import os
import pytest
from parsl.app.app import bash_app


@bash_app
def foo(z=2, stdout=None):
    return f"""echo {z}
    """


@pytest.mark.issue363
def test_command_format_1():
    """Testing command format for BashApps
    """

    stdout = os.path.abspath('std.out.0')
    if os.path.exists(stdout):
        os.remove(stdout)

    app_fu = foo(stdout=stdout)
    print("app_fu : ", app_fu)
    contents = None

    assert app_fu.result() == 0, (f"BashApp exited with an error code: "
                                  f"{app_fu.result()}")

    with open(stdout, 'r') as stdout_f:
        contents = stdout_f.read()
        print("Contents : ", contents)

    if os.path.exists('stdout_file'):
        os.remove(stdout)

    assert contents == '2\n', (f'Output does not match expected string "2", '
                               f'Got: "{contents}"')

# ===========

    stdout = os.path.abspath('std.out.1')
    if os.path.exists(stdout):
        os.remove(stdout)

    app_fu = foo(z=3, stdout=stdout)
    print("app_fu : ", app_fu)
    contents = None

    assert app_fu.result() == 0, (f"BashApp exited with an error code: "
                                  f"{app_fu.result()}")

    with open(stdout, 'r') as stdout_f:
        contents = stdout_f.read()
        print("Contents : ", contents)

    if os.path.exists('stdout_file'):
        os.remove(stdout)

    assert contents == '3\n', (f'Output does not match expected string "3", '
                               f'Got: "{contents}"')

# ===========
    stdout = os.path.abspath('std.out.2')
    if os.path.exists(stdout):
        os.remove(stdout)

    app_fu = foo(z=4, stdout=stdout)
    print("app_fu : ", app_fu)
    contents = None

    assert app_fu.result() == 0, (f"BashApp exited with an error code: "
                                  f"{app_fu.result()}")

    with open(stdout, 'r') as stdout_f:
        contents = stdout_f.read()
        print("Contents : ", contents)

    if os.path.exists('stdout_file'):
        os.remove(stdout)

    assert contents == '4\n', (f'Output does not match expected string "4", '
                               f'Got: "{contents}"')

# ===========
    stdout = os.path.abspath('std.out.3')
    if os.path.exists(stdout):
        os.remove(stdout)

    app_fu = foo(stdout=stdout)
    print("app_fu : ", app_fu)
    contents = None

    assert app_fu.result() == 0, (f"BashApp exited with an error code: "
                                  f"{app_fu.result()}")

    with open(stdout, 'r') as stdout_f:
        contents = stdout_f.read()
        print("Contents : ", contents)

    if os.path.exists('stdout_file'):
        os.remove(stdout)

    assert contents == '2\n', (f'Output does not match expected string "2", '
                               f'Got: "{contents}"')
    return True
