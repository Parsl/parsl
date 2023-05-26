import pytest

import parsl
from parsl.app.app import python_app
from parsl.data_provider.files import File
from parsl.tests.configs.local_threads_globus import config, remote_writeable

local_config = config


@python_app
def sort_strings(inputs=[], outputs=[]):
    with open(inputs[0].filepath, 'r') as u:
        strs = u.readlines()
        strs.sort()
        with open(outputs[0].filepath, 'w') as s:
            for e in strs:
                s.write(e)


@pytest.mark.local
def test_stage_in_globus():
    """Test stage-in for a file coming from a remote Globus endpoint

    Prerequisite:
        unsorted.txt must already exist at the specified endpoint
    """

    unsorted_file = File('globus://03d7d06a-cb6b-11e8-8c6a-0a1d4c5c824a/unsorted.txt')

    # Create a local file for output data
    sorted_file = File('sorted.txt')

    f = sort_strings(inputs=[unsorted_file], outputs=[sorted_file])

    f.result()


@pytest.mark.local
def test_stage_in_out_globus():
    """Test stage-in then stage-out to/from Globus

    Prerequisite:
        unsorted.txt must already exist at the specified endpoint
        the specified output endpoint must be writeable
    """

    unsorted_file = File('globus://03d7d06a-cb6b-11e8-8c6a-0a1d4c5c824a/unsorted.txt')

    # Create a local file for output data
    sorted_file = File(remote_writeable + "/sorted.txt")

    f = sort_strings(inputs=[unsorted_file], outputs=[sorted_file])

    # wait for both the app to complete, and the stageout DataFuture to complete.
    # It isn't clearly defined whether we need to wait for both, or whether
    # waiting for one is sufficient, but at time of writing this test,
    # neither is sufficient (!) - see issue #778 - and instead this test will
    # sometimes pass even though stageout is not working.

    f.result()

    result_file = f.outputs[0].result()

    assert unsorted_file.local_path is None, "Input file on local side has overridden local_path, file: {}".format(repr(unsorted_file))

    # use 'is' rather than '==' because specifically want to check
    # object identity rather than value equality
    assert sorted_file is result_file, "Result file is not the specified input-output file"

    assert result_file.local_path is None, "Result file on local side has overridden local_path, file: {}".format(repr(result_file))
