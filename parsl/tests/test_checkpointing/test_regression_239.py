from parsl import App, DataFlowKernel, set_stream_logger
import pickle
import time


def run_checkpointed(n=5, mode="task_exit"):
    """ This test runs n apps that will fail with Division by zero error,
    followed by 1 app that will succeed. THe checkpoint should only have 1 task
    """

    set_stream_logger()
    from parsl.configs.local import localThreads as config
    config["globals"]["checkpointMode"] = mode
    dfk = DataFlowKernel(config=config)

    @App('python', dfk, cache=True)
    def cached_rand(x):
        import random
        return random.randint(0, 10000)

    @App('python', dfk, cache=True)
    def cached_failing(x):
        5 / 0
        return 1

    items = []
    for i in range(0, n):
        x = cached_failing(0)
        items.append(x)
        try:
            x.result()
        except Exception as e:
            print("Ignoring failure of task")
            pass

    x = cached_rand(1)
    print(x.result())
    rundir = dfk.rundir
    return rundir


def test_regress_239():
    """Testing to ensure failed tasks are not cached. Tests #239
    Also tests task_exit behavior.
    """

    rundir = run_checkpointed()
    time.sleep(0.5)
    with open("{}/checkpoint/tasks.pkl".format(rundir), 'rb') as f:
        tasks = []
        try:
            while f:
                tasks.append(pickle.load(f))
                print
        except EOFError:
            pass
        print("Tasks from cache : ", tasks)
        assert len(tasks) == 1, "Expected {} checkpoint events, got {}".format(1, len(tasks))


def test_checkpointing_at_dfk_exit():
    """Testing to ensure failed tasks are not cached. Tests #239
    Also tests task_exit behavior.
    """

    rundir = run_checkpointed(mode="dfk_exit")
    print("Back in test fn")
    time.sleep(0.5)
    with open("{}/checkpoint/tasks.pkl".format(rundir), 'rb') as f:
        tasks = []
        try:
            while f:
                tasks.append(pickle.load(f))
                print
        except EOFError:
            pass
        print("Tasks from cache : ", tasks)
        assert len(tasks) == 1, "Expected {} checkpoint events, got {}".format(1, len(tasks))


if __name__ == "__main__":
    test_regress_239()
    test_checkpointing_at_dfk_exit()
