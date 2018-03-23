from parsl import *
import parsl
import libsubmit
import time

print(parsl.__version__)
print(libsubmit.__version__)

# parsl.set_stream_logger()

from .local import localIPP
dfk = DataFlowKernel(config=localIPP)


@App("python", dfk)
def diamond(sleep=0, inputs=[]):
    import time
    time.sleep(sleep)
    return sum(inputs)


def test_python(width=10):
    """Diamond pattern to scale from 0 -> 1 -> N -> 1 -> 0 """

    stage_1 = [diamond(sleep=60, inputs=[0])]

    stage_2 = []
    for i in range(0, width):
        stage_2.extend([diamond(sleep=20, inputs=stage_1)])

    stage_3 = [diamond(sleep=30, inputs=stage_2)]

    if not stage_3[0].done():
        time.sleep(30)
        for sitename in dfk.executors:
            print(dfk.executors[sitename].status())


if __name__ == "__main__":
    parsl.set_stream_logger()
    test_python()
