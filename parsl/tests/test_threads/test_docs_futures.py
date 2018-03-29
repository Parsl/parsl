"""Testing python apps
"""
from parsl import *
import time

# parsl.set_stream_logger()
workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def double(x):
    import time
    time.sleep(1)
    return x * 2


def test_1():

    x = double(5)
    print(x.done())

    # Testing. Adding explicit block
    x.result()


@App('python', dfk)
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


@App('python', dfk)
def wait_sleep_double(x, fu_1, fu_2):
    import time
    time.sleep(0.2)
    return x * 2


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


@App('python', dfk)
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


data_flow_kernel = dfk
# This app echo'sthe string passed to itto the first file specified in the
# outputslist


@App('bash', data_flow_kernel)
def echo(message, outputs=[]):
    return 'echo {0} &> {outputs[0]}'

# This app *cat*sthe contents ofthe first file in its inputs[] kwargs to
# the first file in its outputs[] kwargs


@App('bash', data_flow_kernel)
def cat(inputs=[], outputs=[], stdout='cat.out', stderr='cat.err'):
    return 'cat {inputs[0]} > {outputs[0]}'


def test_5():
    """Testing behavior of outputs """
    # Call echo specifying the outputfile
    hello = echo("Hello World!", outputs=['hello1.txt'])

    # the outputs attribute of the AppFuture is a list of DataFutures
    print(hello.outputs)

    # This step *cat*s hello1.txt to hello2.txt
    hello2 = cat(inputs=[hello.outputs[0]], outputs=['hello2.txt'])

    hello2.result()
    with open(hello2.outputs[0].result().filepath, 'r') as f:
        print(f.read())


if __name__ == "__main__":
    test_1()
    test_2()
    test_3()
    test_4()
    test_5()
