import os
import pickle
import signal
import subprocess
import time

import pytest


def kill():
    my_pid = os.getpid()
    os.kill(my_pid, signal.SIGINT)
    print("Killing self")


@pytest.mark.local
@pytest.mark.skip('fails on Travis in pytest')
def test_regress_232_task_exit(count=2):
    """Recovering from a run that was SIGINT'ed with task_exit checkpointing
    """
    cwd = os.path.dirname(os.path.realpath(__file__))
    checkpoint_file = os.path.join(cwd, 'checkpointed.py')

    if os.path.exists("test.txt"):
        os.remove("test.txt")

    proc = subprocess.Popen("python3 {} -n {} -m task_exit".format(checkpoint_file, count),
                            shell=True)

    # Poll for at least 3 seconds
    for i in range(30):
        if os.path.exists("test.txt"):
            break
        time.sleep(0.1)

    proc.send_signal(signal.SIGINT)
    proc.wait()
    # We need to wait after the signal to make sure files were closed and such
    last = os.path.abspath(
        'runinfo/{0}/checkpoint'.format(sorted(os.listdir('runinfo/'))[-1]))
    checkpoint_file = "{}/tasks.pkl".format(last)

    with open(checkpoint_file, 'rb') as f:
        tasks = []
        try:
            while f:
                tasks.append(pickle.load(f))
                print
        except EOFError:
            print("Caught error")
            pass
        print("Tasks from cache : ", tasks)
        assert len(tasks) == count, "Expected {} checkpoint events, got {}".format(
            1, len(tasks))


@pytest.mark.local
@pytest.mark.skip('fails on Travis in pytest')
def test_regress_232_dfk_exit(count=2):
    """Recovering from a run that was SIGINT'ed with dfk_exit checkpointing
    """
    cwd = os.path.dirname(os.path.realpath(__file__))
    checkpoint_file = os.path.join(cwd, 'checkpointed.py')

    proc = subprocess.Popen("python3 {} -n {} -m dkf_exit".format(checkpoint_file, count),
                            shell=True)
    proc.send_signal(signal.SIGINT)
    # We need to wait after the signal to make sure files were closed and such
    proc.wait()

    last = os.path.abspath(
        'runinfo/{0}/checkpoint'.format(sorted(os.listdir('runinfo/'))[-1]))
    checkpoint_file = "{}/tasks.pkl".format(last)

    print(checkpoint_file, "now exists")
    with open(checkpoint_file, 'rb') as f:
        tasks = []
        try:
            while f:
                tasks.append(pickle.load(f))
                print
        except EOFError:
            pass
        print("Tasks from cache : ", tasks)
        assert len(tasks) == count, "Expected {} checkpoint events, got {}".format(
            1, len(tasks))


if __name__ == "__main__":

    test_regress_232_task_exit()
    test_regress_232_dfk_exit()
