from parsl import *
import os

from parsl.data_provider.files import File


def test_files():
    fp = os.path.abspath('test_file.py')
    strings = [{'f': 'file://test_file.py', 'protocol': 'file', 'path': 'test_file.py'},
               {'f': './test_file.py', 'protocol': 'file', 'path': './test_file.py'},
               {'f': fp, 'protocol': 'file', 'path': fp},
               ]

    for test in strings:
        x = File(test['f'])
        assert x.protocol == test['protocol'], "[TEST] Protocol error. Expected {0} Got {1}".format(test['protocol'], x.protocol)
        assert x.filepath == test['path'], "[TEST] Path error. Expected {0} Got {1}".format(test['path'], x.filepath)


if __name__ == '__main__':

    test_files()
