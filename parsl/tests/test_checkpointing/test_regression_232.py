import pickle
import time
import subprocess
import signal
import os


def kill():
    my_pid = os.getpid()
    os.kill(my_pid, signal.SIGINT)
    print("Killing self")


def test_regress_232_task_exit(count=2):
    """Recovering from a run that was SIGINT'ed with task_exit checkpointing
    """
    proc = subprocess.Popen("python3 checkpointed.py -n {} -m task_exit".format(count),
                            shell=True)
    time.sleep(0.3)
    proc.send_signal(signal.SIGINT)
    # We need to wait after the signal to make sure files were closed and such
    time.sleep(0.3)
    last = os.path.abspath(
        'runinfo/{0}/checkpoint'.format(sorted(os.listdir('runinfo/'))[-1]))
    checkpoint_file = "{}/tasks.pkl".format(last)

    for i in range(0, 5):
        if os.path.exists(checkpoint_file):
            break
        else:
            time.sleep(0.2)

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


def test_regress_232_dfk_exit(count=2):
    """Recovering from a run that was SIGINT'ed with dfk_exit checkpointing
    """
    proc = subprocess.Popen("python3 checkpointed.py -n {} -m dfk_exit".format(count),
                            shell=True)
    proc.send_signal(signal.SIGINT)
    # We need to wait after the signal to make sure files were closed and such
    time.sleep(1)

    last = os.path.abspath(
        'runinfo/{0}/checkpoint'.format(sorted(os.listdir('runinfo/'))[-1]))
    checkpoint_file = "{}/tasks.pkl".format(last)

    for i in range(0, 10):
        try:
            with open(checkpoint_file, 'rb') as f:
                f.readline()
                break
        except FileNotFoundError:
            time.sleep(2)

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
