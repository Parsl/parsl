import os
from concurrent.futures import wait

import pytest

from parsl import File, python_app


@python_app
def double(x, outputs=[]):
    with open(outputs[0], 'w') as f:
        f.write(x * 5)
    return x * 5


whitelist = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs', '*threads*')


@pytest.mark.issue363
def test_launch_apps(tmpd_cwd, n=2):
    outdir = tmpd_cwd / "outputs"
    outdir.mkdir()

    futs = [double(i, outputs=[File(str(outdir / f"{i}.txt"))]) for i in range(n)]
    wait(futs)

    stdout_file_count = len(list(outdir.glob("*.txt")))
    assert stdout_file_count == n, sorted(outdir.glob("*.txt"))
