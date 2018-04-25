from parsl import *
set_stream_logger()


def run_checkpointed(cpts):

    from parsl.configs.local import localThreads as config
    config["globals"]["checkpointMode"] = "task_exit"
    dfk = DataFlowKernel(config=config, checkpointFiles=cpts)

    @App('python', dfk, cache=True)
    def cached_rand(x):
        import random
        return random.randint(0, 10000)

    items = []
    for i in range(0, 5):
        x = cached_rand(i)
        items.append(x)

    dfk.cleanup()
    return [i.result for i in items]


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
    run_race(0)
    run_race(0.1)


def test_checkpoint_availability():

    original = run_checkpointed([])
    # last_checkpoint = os.path.abspath('runinfo/{0}/checkpoint'.format(sorted(os.listdir('runinfo/'))[-1]))
    # cached = run_checkpointed([last_checkpoint])

    # assert cached == original, "All tasks were not cached"
    print(original)


if __name__ == "__main__":

    # test_checkpoint_availability()
    test_regress_234()
