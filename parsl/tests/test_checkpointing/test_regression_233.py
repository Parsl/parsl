from parsl import *
# set_stream_logger()


def run_checkpointed(cpts):
    # set_stream_logger()
    from parsl.configs.local import localThreads as config
    config["globals"]["checkpointMode"] = "task_exit"
    config["globals"]["checkpointFiles"] = cpts
    dfk = DataFlowKernel(config=config)

    @App('python', dfk, cache=True)
    def cached_rand(x):
        import random
        return random.randint(0, 10000)

    items = []
    for i in range(0, 5):
        x = cached_rand(i)
        items.append(x)

    dfk.cleanup()
    return [i.result() for i in items]


def run_race(sleep_dur):

    from parsl.configs.local import localThreads as config
    config["globals"]["checkpointMode"] = "task_exit"
    dfk = DataFlowKernel(config=config)

    @App('python', dfk, cache=True)
    def cached_rand(x, sleep_dur=0):
        import random
        import time
        time.sleep(sleep_dur)
        return random.randint(0, 10000)

    items = []
    for i in range(0, 5):
        x = cached_rand(i, sleep_dur=sleep_dur)
        items.append(x)

    dfk.cleanup()
    return [i.result for i in items]


def test_regress_234():
    """Test task_exit checkpointing with fast tasks"""
    run_race(0)


def test_slower_apps():
    """Test task_exit tests with slow apps"""
    run_race(0.5)


def test_checkpoint_availability():
    import os

    original = run_checkpointed([])
    last_checkpoint = os.path.abspath('runinfo/{0}/checkpoint'.format(sorted(os.listdir('runinfo/'))[-1]))
    print(last_checkpoint)
    cached = run_checkpointed([last_checkpoint])

    print(cached)
    print(original)

    assert cached == original, "All tasks were not cached"


if __name__ == "__main__":

    test_checkpoint_availability()
    # test_regress_234()
    # test_slower_apps()
