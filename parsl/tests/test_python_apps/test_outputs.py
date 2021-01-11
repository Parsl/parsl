import argparse
import os
import pytest
import shutil

from concurrent.futures import wait

from parsl import File, python_app
from parsl.tests.configs.local_threads import config


local_config = config


@python_app
def double(x, outputs=[]):
    with open(outputs[0], 'w') as f:
        f.write(x * 5)
    return x * 5


whitelist = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs', '*threads*')


@pytest.mark.issue363
def test_launch_apps(n=2, outdir='outputs'):
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    else:
        shutil.rmtree(outdir)
        os.makedirs(outdir)
    print('outdir is ', outdir)

    all_futs = []
    for i in range(n):
        fus = double(i, outputs=[File('{0}/{1}.txt'.format(outdir, i))])
        all_futs.append(fus)

    wait(all_futs)

    stdout_file_count = len(
        [item for item in os.listdir(outdir) if item.endswith('.txt')])
    assert stdout_file_count == n, "Only {}/{} files in '{}' ".format(
            len(os.listdir('outputs/')), n, os.listdir(outdir))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    args = parser.parse_args()

    x = test_launch_apps(2, "outputs")
    # raise_error(0)
