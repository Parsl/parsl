import parsl


@parsl.python_app
def slow_app(delay):
    import time
    time.sleep(delay)


def test_wait_for_tasks():
    slow_app(5)
    slow_app(10)
    parsl.dfk().wait_for_current_tasks()
    # the regression reported in #1606 is that wait_for_current_tasks
    # fails due to tasks being removed from the DFK tasks dict as they
    # complete, introduced in #1543.
