from parsl import *
import parsl
import libsubmit
import os

os.environ["COOLEY_USERNAME"] = "yadunand"
from cooley import multiNode as config
dfk = DataFlowKernel(config=config)

@App("python", dfk)
def run_python():
    import platform
    return "Hello from {0}".format(platform.uname())


def test(N=5):
    results = {}
    for i in range(0,N):

        results[i] = run_python()

    print("Waiting ....")
    print(results[0].result())
