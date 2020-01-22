'''
Regression tests for issue #69
'''
import pytest

from parsl.app.app import bash_app, python_app
from parsl.tests.configs.local_threads import config


local_config = config


@bash_app
def echo_slow_message(msg, sleep=0, fu=None, outputs=[], stderr='std.err', stdout='std.out'):
    cmd_line = 'sleep {sleep}; echo {0} > {outputs[0]}'
    return cmd_line


@python_app
def sleep(sleep_dur=0.1):
    import time
    time.sleep(sleep_dur)
    return True


@pytest.mark.skip('fails in pytest')
def test_immediate_datafuture():
    """Test DataFuture string representation, for AppFutures launched with parent
    """

    import time
    fu = echo_slow_message("Hello world", sleep=1, outputs=["hello.1.txt"])
    d_fu = fu.outputs[0]

    time.sleep(0.1)
    state_2 = d_fu.__str__()
    print("State_2 : ", state_2, "Fu:", fu.parent)
    assert "running" in state_2, "DataFuture should now be running"

    d_fu.result()
    state_3 = d_fu.__str__()
    print("State_3 : ", state_3, "Fu:", fu.parent)
    assert "finished" in state_3, "DataFuture should now be finished"


@pytest.mark.skip('fails in pytest')
def test_delayed_datafuture():
    """Test DataFuture string representation, for AppFutures with delayed parent
    """

    import time
    sleep_fu = sleep()

    fu = echo_slow_message("Hello world", sleep=1, fu=sleep_fu,
                           outputs=["hello.1.txt"])
    d_fu = fu.outputs[0]
    state_1 = d_fu.__str__()
    print("State_1 : ", state_1, "Fu:", fu.parent)
    assert "pending" in state_1, "DataFuture should now be pending"

    time.sleep(0.2)
    state_2 = d_fu.__str__()
    print("State_2 : ", state_2, "Fu:", fu.parent)
    assert "running" in state_2, "DataFuture should now be running"

    d_fu.result()
    state_3 = d_fu.__str__()
    print("State_3 : ", state_3, "Fu:", fu.parent)
    assert "finished" in state_3, "DataFuture should now be finished"


if __name__ == "__main__":

    test_immediate_datafuture()
    test_delayed_datafuture()
