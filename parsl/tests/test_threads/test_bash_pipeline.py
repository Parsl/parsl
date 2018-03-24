"""Testing bash apps
"""
import parsl
from parsl import *

import argparse

# parsl.set_stream_logger()

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('bash', dfk)
def increment(inputs=[], outputs=[], stdout=None, stderr=None):
    # Place double braces to avoid python complaining about missing keys for {item = $1}
    cmd_line = """
    x=$(cat {inputs[0]})
    echo $(($x+1)) > {outputs[0]}
    """
    return cmd_line


@App('bash', dfk)
def slow_increment(dur, inputs=[], outputs=[], stdout=None, stderr=None):
    cmd_line = """
    x=$(cat {inputs[0]})
    echo $(($x+1)) > {outputs[0]}
    sleep {0}
    """
    return cmd_line


def test_increment(depth=5):
    """Test simple pipeline A->B...->N
    """
    # Create the first file
    open("test0.txt", 'w').write('0\n')

    # Create the first entry in the dictionary holding the futures
    prev = "test0.txt"
    futs = {}
    for i in range(1, depth):
        print("Launching {0} with {1}".format(i, prev))
        fu = increment(inputs=[prev],  # Depend on the future from previous call
                       # Name the file to be created here
                       outputs=["test{0}.txt".format(i)],
                       stdout="incr{0}.out".format(i),
                       stderr="incr{0}.err".format(i))
        [prev] = fu.outputs
        futs[i] = prev
        print(prev.filepath)

    for key in futs:
        if key > 0:
            fu = futs[key]
            data = open(fu.result().filepath, 'r').read().strip()
            assert data == str(
                key), "[TEST] incr failed for key:{0} got:{1}".format(key, data)


def test_increment_slow(depth=5, dur=0.5):
    """Test simple pipeline slow (sleep.5) A->B...->N
    """
    # Create the first file
    open("test0.txt", 'w').write('0\n')

    # Create the first entry in the dictionary holding the futures
    prev = "test0.txt"
    futs = {}
    print("**************TYpe : ", type(dur), dur)
    for i in range(1, depth):
        print("Launching {0} with {1}".format(i, prev))
        fu = slow_increment(dur,
                            # Depend on the future from previous call
                            inputs=[prev],
                            # Name the file to be created here
                            outputs=["test{0}.txt".format(i)],
                            stdout="incr{0}.out".format(i),
                            stderr="incr{0}.err".format(i))
        [prev] = fu.outputs
        futs[i] = prev
        print(prev.filepath)

    for key in futs:
        if key > 0:
            fu = futs[key]
            data = open(fu.result().filepath, 'r').read().strip()
            assert data == str(
                key), "[TEST] incr failed for key:{0} got:{1}".format(key, data)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--width", default="5",
                        help="width of the pipeline")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    # test_increment(depth=int(args.width))
    # test_increment(depth=int(args.width))
    test_increment_slow(depth=int(args.width))
