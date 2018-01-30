from parsl import *
import os

os.environ["COOLEY_USERNAME"] = "yadunand"
from cooley import singleNode as config
dfk = DataFlowKernel(config=config)


@App("python", dfk)
def test():
    import platform
    return "Hello from {0}".format(platform.uname())


results = {}
for i in range(0, 5):
    results[i] = test()

print("Waiting ....")
print(results[0].result())
