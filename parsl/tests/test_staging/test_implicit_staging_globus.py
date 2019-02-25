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
def test_implicit_staging_in_globus():
    """Test implicit staging in for globus transfer protocols

    Create a remote input file (globus) that points to unsorted.txt.
    """

    unsorted_file = File('globus://03d7d06a-cb6b-11e8-8c6a-0a1d4c5c824a/unsorted.txt')

    # Create a local file for output data
    sorted_file = File('sorted.txt')

    f = sort_strings(inputs=[unsorted_file], outputs=[sorted_file])
    f.result()


@pytest.mark.local
def test_implicit_staging_out_globus():
    """Test implicit staging in and out for globus transfer protocols

    Create a remote input file (globus) that points to unsorted.txt.
    """

    unsorted_file = File('globus://03d7d06a-cb6b-11e8-8c6a-0a1d4c5c824a/unsorted.txt')

    # Create a local file for output data
    sorted_file = File('globus://73be4860-345b-11e9-9836-0262a1f2f698/home/benc/sorted.txt')

    f = sort_strings(inputs=[unsorted_file], outputs=[sorted_file])

    f.outputs[0].result()
    # f.result()


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    test_implicit_staging_in_globus()
