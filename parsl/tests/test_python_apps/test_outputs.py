import argparse
import os
import shutil

import pytest

from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


local_config = config


@python_app
def double(x, outputs=[]):
    with open(outputs[0], 'w') as f:
        f.write(x * 5)
    return x * 5


whitelist = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs', '*threads*')


# @pytest.mark.whitelist(whitelist, reason='broken in IPP')
@pytest.mark.skip("Broke somewhere between PR #525 and PR #652")
def test_launch_apps(n=2, outdir='outputs'):
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    else:
        shutil.rmtree(outdir)
        os.makedirs(outdir)
    print('outdir is ', outdir)

    all_futs = {}
    for i in range(n):
        fus = double(i, outputs=['{0}/{1}.txt'.format(outdir, i)])
        print(fus.outputs)
        all_futs[fus] = fus

    stdout_file_count = len(
        [item for item in os.listdir(outdir) if item.endswith('.txt')])
    assert stdout_file_count == n, "Only {}/{} files in '{}' ".format(
            len(os.listdir('outputs/')), n, os.listdir(outdir))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    args = parser.parse_args()

    x = test_launch_apps(2, "outputs")
    # raise_error(0)
