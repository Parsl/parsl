from parsl import *
import parsl
import libsubmit

print(parsl.__version__)
print(libsubmit.__version__)

# parsl.set_stream_logger()

from parsl.configs.local import localIPP
dfk = DataFlowKernel(config=localIPP)


@App("bash", dfk)
def bash_app(stdout=None, stderr=None):
    return 'echo "Hello from $(uname -a)" ; sleep 2'


def test_bash():
    """Testing basic scaling|Bash 0 -> 1 block """

    import os
    fname = os.path.basename(__file__)

    x = bash_app(stdout="{0}.out".format(fname))
    print("Waiting ....")
    print(x.result())


if __name__ == "__main__":
    test_bash()
