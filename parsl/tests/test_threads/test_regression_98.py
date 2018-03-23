"""Testing python apps
"""
import parsl
from parsl import *
import argparse
import json


def test_immutable_config(n=2):
    """Regression test for immutable config #98
    """

    localThreads = {
        "sites": [
            {"site": "Local_Threads",
             "auth": {"channel": None},
             "execution": {
                 "executor": "threads",
                 "provider": None,
                 "maxThreads": 4
             }
             }],
        "globals": {"lazyErrors": True}
    }

    original = json.dumps(localThreads, sort_keys=True)
    dfk = DataFlowKernel(config=localThreads)
    after = json.dumps(localThreads, sort_keys=True)

    dfk.cleanup()
    assert original == after, "Config modified"


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_immutable_config()
