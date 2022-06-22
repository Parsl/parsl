import parsl
from parsl.app.app import python_app
from parsl.data_provider.files import File

import pytest

# This config is for the local test which will adding an executor.
# Most tests in this file should be non-local and use the configuration
# specificed with --config, not this one.
from parsl.tests.configs.htex_local import fresh_config
local_config = fresh_config()


@python_app
def sort_strings(inputs=[], outputs=[]):
    with open(inputs[0].filepath, 'r') as u:
        strs = u.readlines()
        strs.sort()
        with open(outputs[0].filepath, 'w') as s:
            for e in strs:
                s.write(e)


@pytest.mark.cleannet
def test_implicit_staging_https():
    """Test implicit staging for an ftp file

    Create a remote input file (https) that points to unsorted.txt.
    """

    # unsorted_file = File('https://testbed.petrel.host/test/public/unsorted.txt')
    unsorted_file = File('https://gist.githubusercontent.com/yadudoc/7f21dd15e64a421990a46766bfa5359c/'
                         'raw/7fe04978ea44f807088c349f6ecb0f6ee350ec49/unsorted.txt')

    # Create a local file for output data
    sorted_file = File('sorted.txt')

    f = sort_strings(inputs=[unsorted_file], outputs=[sorted_file])
    f.result()


@python_app
def sort_strings_kw(x=None, outputs=[]):
    with open(x.filepath, 'r') as u:
        strs = u.readlines()
        strs.sort()
        with open(outputs[0].filepath, 'w') as s:
            for e in strs:
                s.write(e)


@pytest.mark.cleannet
def test_implicit_staging_https_kwargs():

    # unsorted_file = File('https://testbed.petrel.host/test/public/unsorted.txt')
    unsorted_file = File('https://gist.githubusercontent.com/yadudoc/7f21dd15e64a421990a46766bfa5359c/'
                         'raw/7fe04978ea44f807088c349f6ecb0f6ee350ec49/unsorted.txt')

    # Create a local file for output data
    sorted_file = File('sorted.txt')

    f = sort_strings_kw(x=unsorted_file, outputs=[sorted_file])
    f.result()


@python_app
def sort_strings_arg(x, outputs=[]):
    with open(x.filepath, 'r') as u:
        strs = u.readlines()
        strs.sort()
        with open(outputs[0].filepath, 'w') as s:
            for e in strs:
                s.write(e)


@pytest.mark.cleannet
def test_implicit_staging_https_args():

    # unsorted_file = File('https://testbed.petrel.host/test/public/unsorted.txt')
    unsorted_file = File('https://gist.githubusercontent.com/yadudoc/7f21dd15e64a421990a46766bfa5359c/'
                         'raw/7fe04978ea44f807088c349f6ecb0f6ee350ec49/unsorted.txt')

    # Create a local file for output data
    sorted_file = File('sorted.txt')

    f = sort_strings_arg(unsorted_file, outputs=[sorted_file])
    f.result()


@python_app(executors=['other'])
def sort_strings_additional_executor(inputs=[], outputs=[]):
    with open(inputs[0].filepath, 'r') as u:
        strs = u.readlines()
        strs.sort()
        with open(outputs[0].filepath, 'w') as s:
            for e in strs:
                s.write(e)


@pytest.mark.cleannet
@pytest.mark.local
def test_implicit_staging_https_additional_executor():
    """Test implicit staging for an ftp file

    Create a remote input file (https) that points to unsorted.txt.
    """

    # unsorted_file = File('https://testbed.petrel.host/test/public/unsorted.txt')
    unsorted_file = File('https://gist.githubusercontent.com/yadudoc/7f21dd15e64a421990a46766bfa5359c/'
                         'raw/7fe04978ea44f807088c349f6ecb0f6ee350ec49/unsorted.txt')

    # Create a local file for output data
    sorted_file = File('sorted.txt')

    other_executor = parsl.ThreadPoolExecutor(label='other')

    parsl.dfk().add_executors([other_executor])

    f = sort_strings_additional_executor(inputs=[unsorted_file], outputs=[sorted_file])
    f.result()


if __name__ == "__main__":

    parsl.load()

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    test_implicit_staging_https()
