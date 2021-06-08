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
        assert x.scheme == test['scheme'], (f"[TEST] Scheme error. "
                                            f"Expected {test['scheme']} "
                                            f"Got { x.scheme}")
        assert x.filepath == test['path'], (f"[TEST] Path error. "
                                            f"Expected {test['path']} "
                                            f"Got {x.path}")


@pytest.mark.local
def test_open():
    with open('test-open.txt', 'w') as tfile:
        tfile.write('Hello')

    pfile = File('test-open.txt')

    with open(str(pfile), 'r') as opfile:
        assert (opfile.readlines()[0] == 'Hello')


if __name__ == '__main__':

    test_files()
