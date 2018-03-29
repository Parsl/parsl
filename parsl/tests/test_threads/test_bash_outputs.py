"""Testing bash apps
"""
import parsl
from parsl import *

print("Parsl version: ", parsl.__version__)

import os
import time
import shutil
import argparse

# parsl.set_stream_logger()
workers = ThreadPoolExecutor(max_workers=8)

# workers = ProcessPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('bash', dfk)
def echo_to_file(inputs=[], outputs=[], stderr='std.err', stdout='std.out'):
    cmd_line = 'echo {inputs[0]} > {outputs[0]}'
    return cmd_line


def test_parallel_for(n=10):
    """Testing a bash outputs in a parallel for
    """
    outdir = 'outputs'
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    else:
        shutil.rmtree(outdir)
        os.makedirs(outdir)

    d = {}

    start = time.time()
    for i in range(0, n):
        d[i] = echo_to_file(inputs=['Hello World {0}'.format(i)],
                            outputs=['{0}/out.{1}.txt'.format(outdir, i)],
                            stdout='{0}/std.{1}.out'.format(outdir, i),
                            stderr='{0}/std.{1}.err'.format(outdir, i),
                            )
        # time.sleep(0.01)

    assert len(
        d.keys()) == n, "Only {0}/{1} keys in dict".format(len(d.keys()), n)

    [d[i].result() for i in d]
    print([d[i].outputs for i in d])
    print("Duration : {0}s".format(time.time() - start))
    stdout_file_count = len(
        [item for item in os.listdir(outdir) if item.endswith('.out')])
    assert stdout_file_count == n, "Only {0}/{1} files in '{1}' ".format(len(os.listdir('outputs/')),
                                                                         n, outdir)
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return d


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_parallel_for(int(args.count))
    # raise_error(0)
