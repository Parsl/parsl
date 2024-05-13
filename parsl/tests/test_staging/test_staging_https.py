import pytest

import parsl
from parsl.app.app import python_app
from parsl.data_provider.files import File

# This config is for the local test which will adding an executor.
# Most tests in this file should be non-local and use the configuration
# specificed with --config, not this one.
from parsl.tests.configs.htex_local import fresh_config as local_config

"""
Test staging for an http file

Create a remote input file (https) that points to unsorted.txt, then
a local file for output data (sorted.txt).
"""

_unsorted_url = (
    'https://gist.githubusercontent.com/yadudoc/7f21dd15e64a421990a46766bfa5359c/'
    'raw/7fe04978ea44f807088c349f6ecb0f6ee350ec49/unsorted.txt'
)
_exp_sorted = sorted(f"{i}\n" for i in range(1, 101))[:20]


@python_app
def sort_strings(inputs=(), outputs=()):
    from parsl.tests.test_staging import read_sort_write
    read_sort_write(inputs[0].filepath, outputs[0].filepath)


@python_app
def sort_strings_kw(*, x=None, outputs=()):
    from parsl.tests.test_staging import read_sort_write
    read_sort_write(x.filepath, outputs[0].filepath)


@python_app
def sort_strings_arg(x, /, outputs=()):
    from parsl.tests.test_staging import read_sort_write
    read_sort_write(x.filepath, outputs[0].filepath)


@python_app(executors=['other'])
def sort_strings_additional_executor(inputs=(), outputs=()):
    from parsl.tests.test_staging import read_sort_write
    read_sort_write(inputs[0].filepath, outputs[0].filepath)


@pytest.mark.cleannet
@pytest.mark.staging_required
def test_staging_https_cleannet(tmpd_cwd):
    unsorted_file = File(_unsorted_url)
    sorted_file = File(tmpd_cwd / 'sorted.txt')

    sort_strings(inputs=[unsorted_file], outputs=[sorted_file]).result()
    with open(sorted_file) as f:
        assert all(a == b for a, b in zip(f.readlines(), _exp_sorted))


@pytest.mark.local
def test_staging_https_local(tmpd_cwd):
    unsorted_file = File(_unsorted_url)
    sorted_file = File(tmpd_cwd / 'sorted.txt')

    sort_strings(inputs=[unsorted_file], outputs=[sorted_file]).result()
    with open(sorted_file) as f:
        assert all(a == b for a, b in zip(f.readlines(), _exp_sorted))


@pytest.mark.cleannet
@pytest.mark.staging_required
def test_staging_https_kwargs(tmpd_cwd):
    unsorted_file = File(_unsorted_url)
    sorted_file = File(tmpd_cwd / 'sorted.txt')

    sort_strings_kw(x=unsorted_file, outputs=[sorted_file]).result()
    with open(sorted_file) as f:
        assert all(a == b for a, b in zip(f.readlines(), _exp_sorted))


@pytest.mark.cleannet
@pytest.mark.staging_required
def test_staging_https_args(tmpd_cwd):
    unsorted_file = File(_unsorted_url)
    sorted_file = File(tmpd_cwd / 'sorted.txt')

    sort_strings_arg(unsorted_file, outputs=[sorted_file]).result()
    with open(sorted_file) as f:
        assert all(a == b for a, b in zip(f.readlines(), _exp_sorted))


@pytest.mark.cleannet
@pytest.mark.local
def test_staging_https_additional_executor(tmpd_cwd):
    unsorted_file = File(_unsorted_url)
    sorted_file = File(tmpd_cwd / 'sorted.txt')

    other_executor = parsl.ThreadPoolExecutor(label='other')
    other_executor.working_dir = tmpd_cwd

    parsl.dfk().add_executors([other_executor])

    sort_strings_additional_executor(inputs=[unsorted_file], outputs=[sorted_file]).result()
    with open(sorted_file) as f:
        assert all(a == b for a, b in zip(f.readlines(), _exp_sorted))
