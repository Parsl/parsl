"""Testing python outputs
"""
from parsl import *
import os
import shutil
import argparse

# parsl.set_stream_logger()
workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def double(x, outputs=[]):
    with open(outputs[0], 'w') as f:
        f.write(x * 5)
    return x * 5


def launch_apps(n, dirpath):

    outdir = dirpath
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    else:
        shutil.rmtree(outdir)
        os.makedirs(outdir)

    all_futs = {}
    for i in range(n):

        fus, _ = double(i, outputs=['{0}/{1}.txt'.format(dirpath, i)])
        print(fus.outputs)
        all_futs[fus] = fus

    stdout_file_count = len(
        [item for item in os.listdir(outdir) if item.endswith('.txt')])
    assert stdout_file_count == n, "Only {0}/{1} files in '{1}' ".format(len(os.listdir('outputs/')),
                                                                         n, outdir)
    print("[TEST STATUS] test_parallel_for [SUCCESS]")


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    args = parser.parse_args()

    x = launch_apps(10, "outputs")
    # raise_error(0)
