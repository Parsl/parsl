'''
Regression tests for issue #69.
'''
import time

import pytest

import parsl
from parsl.app.app import bash_app, python_app
from parsl.data_provider.files import File
from parsl.tests.configs.local_threads import config


@python_app
def double(x):
    import time
    time.sleep(1)
    return x * 2


def test_1():

    x = double(5)
    print(x.done())

    # Testing. Adding explicit block
    x.result()


@python_app
def sleep_double(x):
    import time
    time.sleep(0.2)
    return x * 2


def test_2():

    # doubled_x is an AppFuture
    doubled_x = sleep_double(10)

    # The result() waits till the sleep_double() app is done (2s wait) and then prints
    # the result from the app *10*
    print(doubled_x.result())


@python_app
def wait_sleep_double(x, fu_1, fu_2):
    import time
    time.sleep(0.2)
    return x * 2


@pytest.mark.skip('fails intermittently; too sensitive to machine load')
def test_3():

    start = time.time()

    # Launch two apps, which will execute in parallel
    doubled_x = wait_sleep_double(10, None, None)
    doubled_y = wait_sleep_double(10, None, None)

    # The third depends on the first two :
    #    doubled_x   doubled_y     (2 s)
    #           \     /
    #           doublex_z          (2 s)
    doubled_z = wait_sleep_double(10, doubled_x, doubled_y)

    # doubled_z will be done in ~4s
    print(doubled_z.result())

    end = time.time()

    delta = (end - start) * 10
    print("delta : ", delta)
    assert delta > 4, "Took too little time"
    assert delta < 5, "Took too much time"


@python_app
def bad_divide(x):
    return 6 / x


def test_4():

    doubled_x = bad_divide(0)

    try:
        doubled_x.result()

    except ZeroDivisionError:
        print("Oops! You tried to divide by 0 ")
    except Exception:
        print("Oops! Something really bad happened")


@bash_app
def echo(message, outputs=[]):
    return 'echo {0} &> {outputs[0]}'.format(message, outputs=outputs)

# This app *cat*sthe contents ofthe first file in its inputs[] kwargs to
# the first file in its outputs[] kwargs


@bash_app
def cat(inputs=[], outputs=[], stdout='cat.out', stderr='cat.err'):
    return 'cat {inputs[0]} > {outputs[0]}'.format(inputs=inputs, outputs=outputs)


@pytest.mark.staging_required
def test_5():
    """Testing behavior of outputs """
    # Call echo specifying the outputfile
    hello = echo("Hello World!", outputs=[File('hello1.txt')])

    # the outputs attribute of the AppFuture is a list of DataFutures
    print(hello.outputs)

    # This step *cat*s hello1.txt to hello2.txt
    hello2 = cat(inputs=[hello.outputs[0]], outputs=[File('hello2.txt')])

    hello2.result()
    with open(hello2.outputs[0].result().filepath, 'r') as f:
        print(f.read())


if __name__ == "__main__":
    parsl.clear()
    parsl.load(config)
    test_1()
    test_2()
    test_3()
    test_4()
    test_5()
