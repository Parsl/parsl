from parsl import python_app, DataFlowKernel
import time
import argparse


def run_checkpointed(n=2, mode="task_exit", sleep_dur=0):
    """ This test runs n apps that will fail with Division by zero error,
    followed by 1 app that will succeed. The checkpoint should only have 1 task
    """

    from parsl.tests.configs.local_threads import config
    config['globals']['runDir'] = 'runinfo'
    config["globals"]["checkpointMode"] = mode
    dfk = DataFlowKernel(config=config)

    @python_app(data_flow_kernel=dfk, cache=True)
    def cached_rand(x, sleep_dur=0):
        import random
        import time
        time.sleep(sleep_dur)
        return random.randint(0, 10000)

    items = []
    for i in range(0, n):
        x = cached_rand(i, sleep_dur=sleep_dur)
        items.append(x)

    # Barrier
    [i.result() for i in items]
    with open("test.txt", 'w') as f:
        f.write("done")

    time.sleep(10)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", help="DFK checkpointing mode", required=True)
    parser.add_argument("-n", "--niter", default="2",
                        help="Number of iterations")
    parser.add_argument("-s", "--sleep", default="0", help="seconds to sleep")
    parser.add_argument("-d", "--dir", help="Checkpoint dir")
    args = parser.parse_args()

    run_checkpointed(n=int(args.niter),
                     sleep_dur=float(args.sleep),
                     mode=args.mode)
