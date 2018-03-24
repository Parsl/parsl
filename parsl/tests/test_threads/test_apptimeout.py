"""Testing bash apps
"""
import parsl
from parsl import *

print("Parsl version: ", parsl.__version__)


# parsl.set_stream_logger()
workers = ThreadPoolExecutor(max_workers=8)

# workers = ProcessPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('bash', dfk)
def echo_to_file(inputs=[], outputs=[], stderr='std.err', stdout='std.out', walltime=0.5):
    return """echo "sleeping";
    sleep 1 """


def test_walltime():
    """Testing walltime exceeded exception """
    x = echo_to_file()

    try:
        r = x.result()
        print("Got result : ", r)
    except Exception as e:
        print(e.__class__)
        print("Caught exception ", e)


if __name__ == "__main__":
    test_walltime()
