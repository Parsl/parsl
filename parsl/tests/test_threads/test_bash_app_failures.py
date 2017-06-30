''' Testing bash apps
'''
import parsl
from parsl import *

from nose.tools import nottest
print("Parsl version: ", parsl.__version__)

import os
import time
import shutil
import argparse

#parsl.set_stream_logger()
workers = ThreadPoolExecutor(max_workers=8)

#workers = ProcessPoolExecutor(max_workers=4)
dfk = DataFlowKernel(workers)


@App('bash', dfk)
def fail_missing_executable(x, y, stdout=None, stderr=None):
    cmd_line = 'foobaar {0} {1}'

@App('bash', dfk)
def fail_missing_outputs(x, outputs=[], stdout=None, stderr=None):
    cmd_line = 'echo {0} ; #&> ${outputs[0]}'


def test_fail_1 ():
    ''' Testing command format for BashApps
    '''

    stdout = 'std.out'
    if os.path.exists(stdout):
        os.remove(stdout)

    app_fu = fail_missing_executable(1,2, stdout='std.out', stderr='std.err')
    print("App_fu : ", app_fu)
    try :
        print("App_fu : ", app_fu.result())
    except Exception as e:
        print("Top level exception : ", e)
    contents = None

    return True

def test_fail_2 ():
    ''' Testing command format for BashApps
    '''
    print ("Test fail 2")
    stdout = 'std.out'
    if os.path.exists(stdout):
        os.remove(stdout)

    app_fu, _ = fail_missing_outputs(1, outputs=['foo.txt'],
                                  stdout='std.out', stderr='std.err')
    try :
        print("App_fu result : ", app_fu.result())
    except Exception as e:
        print("Top level exception : ", e)
    contents = None

    return True



def test_fail_depends ():
    ''' Testing command format for BashApps
    '''
    print ("Test fail 2")
    stdout = 'std.out'
    if os.path.exists(stdout):
        os.remove(stdout)

    app_fu, _ = fail_missing_outputs(1, outputs=['foo.txt'],
                                  stdout='std.out', stderr='std.err')

    print("Outputs: ", app_fu.outputs)
    try :
        print("App_fu result : ", app_fu.result())
    except Exception as e:
        print("Top level exception : ", e)
    contents = None

    return True




if __name__ == '__main__' :

    parser   = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10", help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true', help="Count of apps to launch")
    args   = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    #x = test_parallel_for(int(args.count))
    test_fail_1()
    test_fail_2()
    test_fail_depends()
    #raise_error(0)
