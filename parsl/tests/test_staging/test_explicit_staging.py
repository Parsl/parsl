import pytest

import parsl
from parsl.app.app import App
from parsl.data_provider.files import File
from parsl.tests.configs.local_threads_globus import config

parsl.clear()
parsl.load(config)


@App('python')
def sort_strings(inputs=[], outputs=[]):
    with open(inputs[0].filepath, 'r') as u:
        strs = u.readlines()
        strs.sort()
        with open(outputs[0].filepath, 'w') as s:
            for e in strs:
                s.write(e)


@pytest.mark.local
def test_explicit_staging():
    """Test explicit staging via Globus.

    Create a remote input file that points to unsorted.txt on a publicly shared
    endpoint.
    """
    unsorted_file = File("globus://03d7d06a-cb6b-11e8-8c6a-0a1d4c5c824a/unsorted.txt")

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
        parsl.set_stream_logger()

    test_explicit_staging()
