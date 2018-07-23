import logging
import os
import shlex
import subprocess
import threading
import time
from contextlib import contextmanager
from functools import wraps

from typing import List

import parsl
from parsl.version import VERSION

logger = logging.getLogger(__name__)


def get_version():
    # type: () -> str
    version = parsl.__version__
    work_tree = os.path.dirname(os.path.dirname(__file__))
    git_dir = os.path.join(work_tree, '.git')
    if os.path.exists(git_dir):
        env = {'GIT_WORK_TREE': work_tree, 'GIT_DIR': git_dir}
        try:
            cmd = shlex.split('git rev-parse --short HEAD')
            head = subprocess.check_output(cmd, env=env).strip().decode('utf-8')
            diff = subprocess.check_output(shlex.split('git diff HEAD'), env=env)
            status = 'dirty' if diff else 'clean'
            version = '{v}-{head}-{status}'.format(v=VERSION, head=head, status=status)
        except Exception as e:
            pass

    return version


def get_all_checkpoints(rundir="runinfo"):
    # type: (str) -> List[str]
    """Finds the checkpoints from all last runs.

    Note that checkpoints are incremental, and this helper will not find
    previous checkpoints from earlier than the most recent run. It probably
    should be made to do so.

    Kwargs:
       - rundir(str) : Path to the runinfo directory

    Returns:
       - a list suitable for the checkpointFiles parameter of DataFlowKernel
         constructor

    """

    if(not(os.path.isdir(rundir))):
        return []

    dirs = sorted(os.listdir(rundir))

    checkpoints = []

    for runid in dirs:

        checkpoint = os.path.abspath('{}/{}/checkpoint'.format(rundir, runid))

        if(os.path.isdir(checkpoint)):
            checkpoints.append(checkpoint)

    return checkpoints


def get_last_checkpoint(rundir="runinfo"):
    # type: (str) -> List[str]
    """Finds the checkpoint from the last run, if one exists.

    Note that checkpoints are incremental, and this helper will not find
    previous checkpoints from earlier than the most recent run. It probably
    should be made to do so.

    Kwargs:
       - rundir(str) : Path to the runinfo directory

    Returns:
     - a list suitable for checkpointFiles parameter of DataFlowKernel
       constructor, with 0 or 1 elements

    """

    if(not(os.path.isdir(rundir))):
        return []

    dirs = sorted(os.listdir(rundir))

    if(len(dirs) == 0):
        return []

    last_runid = dirs[-1]
    last_checkpoint = os.path.abspath('{}/{}/checkpoint'.format(rundir, last_runid))

    if(not(os.path.isdir(last_checkpoint))):
        return []

    return [last_checkpoint]


def timeout(seconds=None):
    def decorator(func, *args, **kwargs):
        @wraps(func)
        def wrapper(*args, **kwargs):
            t = threading.Thread(target=func, args=args, kwargs=kwargs)
            t.start()
            result = t.join(seconds)
            if t.is_alive():
                raise RuntimeError('timed out in {}'.format(func))
            return result
        return wrapper
    return decorator


@contextmanager
def wait_for_file(path, seconds=10):
    for i in range(0, int(seconds * 100)):
        time.sleep(seconds / 100.)
        if os.path.exists(path):
            break
    yield


@contextmanager
def time_limited_open(path, mode, seconds=1):
    wait_for_file(path, seconds)

    f = open(path, mode)
    yield f
    f.close()
