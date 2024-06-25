import os

import pytest

from parsl.data_provider.files import File


@pytest.mark.local
def test_files():
    fp = os.path.abspath('test_file.py')
    strings = [{'f': 'file:///test_file.py', 'scheme': 'file', 'path': '/test_file.py'},
               {'f': './test_file.py', 'scheme': 'file', 'path': './test_file.py'},
               {'f': fp, 'scheme': 'file', 'path': fp},
               ]

    for test in strings:
        x = File(test['f'])
        assert x.scheme == test['scheme'], "[TEST] Scheme error. Expected {0} Got {1}".format(
            test['scheme'], x.scheme)
        assert x.filepath == test['path'], "[TEST] Path error. Expected {0} Got {1}".format(
            test['path'], x.path)


@pytest.mark.local
def test_open(tmpd_cwd):
    fpath = tmpd_cwd / 'test-open.txt'
    fpath.write_text('Hello')

    pfile = File(fpath)

    with open(pfile) as opfile:
        assert (opfile.read() == 'Hello')
