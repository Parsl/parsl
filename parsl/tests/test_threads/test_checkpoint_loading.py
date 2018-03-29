"""Testing python apps
"""

import parsl
from parsl import *
import argparse


def test_python_memoization(n=4):
    """Test for memoization from checkpoints. Regression test for issue #95.
    """

    workers = ThreadPoolExecutor(max_workers=4)
    dfk = DataFlowKernel(executors=[workers])

    @App('python', dfk)
    def random_uuid(x, cache=True):
        import uuid
        return str(uuid.uuid4())

    x = random_uuid(0)
    checkpoint = dfk.checkpoint()
    dfk.cleanup()

    workers = ThreadPoolExecutor(max_workers=4)
    dfk = DataFlowKernel(executors=[workers], checkpointFiles=[checkpoint])

    @App('python', dfk)
    def random_uuid(x, cache=True):
        import uuid
        return str(uuid.uuid4())

    y = random_uuid(0)

    assert x.result() != y.result(), "Memoized results were not used"


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_python_memoization()
