import os
import subprocess
import shlex

import parsl
from parsl.version import VERSION


def get_version():
    if 'site-packages' in __file__:
        version = parsl.__version__
    else:
        git_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.git')
        env = {'GIT_DIR': git_dir}
        cmd = shlex.split('git rev-parse --short HEAD')
        head = subprocess.check_output(cmd, env=env).strip().decode('utf-8')
        diff = subprocess.check_output(shlex.split('git diff HEAD'), env=env)
        status = 'dirty' if diff else 'clean'
        version = '{v}-{head}-{status}'.format(v=VERSION, head=head, status=status)
    return version
