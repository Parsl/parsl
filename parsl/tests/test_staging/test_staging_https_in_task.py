import pytest

from parsl.app.app import python_app
from parsl.data_provider.files import File

from parsl.tests.configs.local_threads_http_in_task import fresh_config as local_config


@python_app
def sort_strings(inputs=[], outputs=[]):
    with open(inputs[0].filepath, 'r') as u:
        strs = u.readlines()
        strs.sort()
        with open(outputs[0].filepath, 'w') as s:
            for e in strs:
                s.write(e)


@pytest.mark.local
def test_staging_https():
    """Test staging for an http file

    Create a remote input file (https) that points to unsorted.txt.
    """

    # unsorted_file = File('https://testbed.petrel.host/test/public/unsorted.txt')
    unsorted_file = File('https://gist.githubusercontent.com/yadudoc/7f21dd15e64a421990a46766bfa5359c/'
                         'raw/7fe04978ea44f807088c349f6ecb0f6ee350ec49/unsorted.txt')

    # Create a local file for output data
    sorted_file = File('sorted.txt')

    f = sort_strings(inputs=[unsorted_file], outputs=[sorted_file])
    f.result()
