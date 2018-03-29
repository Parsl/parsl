from parsl import *
# from parsl.data_provider.data_manager import DataManager
from parsl.data_provider.files import File
import os

config = {
    "sites": [
        {
            "site": "Local_Threads",
            "auth": {
                "channel": None
            },
            "execution": {
                "executor": "threads",
                "provider": None,
                "maxThreads": 4
            },
            "data": {
                "globus": {
                    "endpoint_name": os.environ["GLOBUS_ENDPOINT"],
                    "endpoint_path": os.environ["GLOBUS_EP_PATH"],
                    "local_directory": os.environ["GLOBUS_EP_PATH"],
                }
            }
        }
    ],
    "globals": {
        "lazyErrors": True
    }
}

dfk = DataFlowKernel(config=config)


@App('python', dfk)
def sort_strings(inputs=[], outputs=[]):
    with open(inputs[0].filepath, 'r') as u:
        strs = u.readlines()
        strs.sort()
        with open(outputs[0].filepath, 'w') as s:
            for e in strs:
                s.write(e)


def test_explicit_staging():
    """Test explicit staging via globus.

    Create a remote input file that points to unsorted.txt on a publicly shared
    endpoint.
    """
    unsorted_file = File(
        "globus://037f054a-15cf-11e8-b611-0ac6873fc732/unsorted.txt")

    # Create a remote output file that points to sorted.txt on the go#ep1 Globus endpoint

    sorted_file = File(
        "globus://ddb59aef-6d04-11e5-ba46-22000b92c6ec/~/sorted.txt")

    dfu = unsorted_file.stage_in()
    dfu.result()

    f = sort_strings(inputs=[dfu], outputs=[sorted_file])
    f.result()

    fs = sorted_file.stage_out()
    fs.result()


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        set_stream_logger()

    test_explicit_staging()
