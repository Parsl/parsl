import logging
import os
import shlex
import subprocess

import parsl
from parsl.version import VERSION

logger = logging.getLogger(__name__)


def get_version():
    version = parsl.__version__
    if 'site-packages' not in __file__:
        work_tree = os.path.dirname(os.path.dirname(__file__))
        git_dir = os.path.join(work_tree, '.git')
        env = {'GIT_WORK_TREE': work_tree, 'GIT_DIR': git_dir}
        try:
            cmd = shlex.split('git rev-parse --short HEAD')
            head = subprocess.check_output(cmd, env=env).strip().decode('utf-8')
            diff = subprocess.check_output(shlex.split('git diff HEAD'), env=env)
            status = 'dirty' if diff else 'clean'
            version = '{v}-{head}-{status}'.format(v=VERSION, head=head, status=status)
        except Exception as e:
            logger.exception("Unable to determine code state")

    return version
