import parsl

@parsl.python_app
def slow_app(delay):
    import time
    time.sleep(delay)

def test_fake_bad():
    slow_app(5)
    slow_app(10)
    parsl.dfk().wait_for_current_tasks()


