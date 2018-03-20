from parsl import *
import parsl
import os
import time
import argparse

#parsl.set_stream_logger()
os.environ['MIDWAY_USERNAME'] = 'yadunand'
from midway import multiNode as config
dfk = DataFlowKernel(config=config)


@App("python", dfk)
def python_app():
    import platform
    return "Hello from {0}".format(platform.uname())

@App("python", dfk)
def python_noop():
    return



@App("bash", dfk)
def bash_app(stdout=None, stderr=None):
    return 'echo "Hello from $(uname -a)" ; sleep 2'

def average(l):
    return sum(l)/len(l)


def test_python(count):
    results = {}

    print("Priming the system")
    items = []
    for i in range(0,100):
        items.extend([python_app()])
    print("Launched primer application")
    for i in items:
        i.result()
    print("Primer done")


    latency = []
    rtt = []
    for i in range(0,100):
        pre = time.time()
        results[i] = python_noop()
        post = time.time()
        results[i].result()
        final = time.time()
        print("Latency : ", final-post)
        print("Roundtrip time : ", final-pre)
        latency.append(final-post)
        rtt.append(final-pre)

    print("Latency   |   Min:{0:5} Max:{1:5} Average:{2:5}".format(min(latency),
                                                                   max(latency),
                                                                   average(latency)))

    print("Roundtrip |   Min:{0:5} Max:{1:5} Average:{2:5}".format(min(rtt),
                                                                   max(rtt),
                                                                   average(rtt)))





def test_bash():
    import os
    fname = os.path.basename(__file__)

    x = bash_app(stdout="{0}.out".format(fname))
    print("Waiting ....")
    print(x.result())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    #if args.debug:
    #    parsl.set_stream_logger()

    test_python(int(args.count))
    # test_bash()
