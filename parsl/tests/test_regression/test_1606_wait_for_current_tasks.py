import parsl


@parsl.python_app
def slow_app(delay):
    import time
    time.sleep(delay)


def test_wait_for_tasks():
    slow_app(5)
    slow_app(10)  # This test has a higher task ID, and runs for a longer period
    slow_app(3)  # This test has a higher task ID, but runs for a shorter period
    parsl.dfk().wait_for_current_tasks()
    # the regression reported in #1606 is that wait_for_current_tasks
    # fails due to tasks being removed from the DFK tasks dict as they
    # complete, introduced in #1543.
