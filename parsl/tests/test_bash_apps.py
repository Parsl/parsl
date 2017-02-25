''' Testing bash apps
'''
import parsl
from parsl import *


import os
import time
import shutil
import argparse

workers = ThreadPoolExecutor(max_workers=8)
#parsl.set_stream_logger()
#workers = ProcessPoolExecutor(max_workers=4)
dfk = DataFlowKernel(workers)

@App('bash', dfk)
def echo_to_file(inputs=[], outputs=[], stderr='std.err', stdout='std.out'):
    cmd_line = 'echo {inputs[0]} > {outputs[0]}'

@App('bash', dfk)
def foo(x, y, outputs=[]):
    cmd_line = '''echo {0}
    echo {0} {1} &> {outputs[0]}
    '''

def test_command_format_1 ():

    app_fu, data_fus = foo(1, 4, outputs=['a.txt'])

    result = data_fus[0].result()
    #print("Result in data_fus : ", result)
    contents = open(result, 'r').read()
    #print("Got contents : ", contents)
    assert contents == "1 4\n", 'Output does not match expected string "1 4", Got: "{0}"'.format(contents)
    return True

def test_parallel_for (n=10):

    outdir='outputs'
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    else:
        shutil.rmtree(outdir)
        os.makedirs(outdir)

    d = {}

    start = time.time()
    for i in range(0,n):
        d[i], _ = echo_to_file(inputs=['Hello World {0}'.format(i)],
                            outputs=['{0}/out.{1}.txt'.format(outdir, i)],
                            stdout='d/std.{1}.out'.format(outdir, i),
                            stderr='d/std.{1}.err'.format(outdir, i),
        )
        #time.sleep(0.01)

    assert len(d.keys())   == n , "Only {0}/{1} keys in dict".format(len(d.keys()), n)

    [d[i].result() for i in d]
    print("Duration : {0}s".format(time.time() - start))
    assert len(os.listdir('outputs/')) == n , "Only {0}/{1} files in '{1}' ".format(len(os.listdir('outputs/')),
                                                                                    n, outdir)
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return d


if __name__ == '__main__' :

    parser   = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10", help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true', help="Count of apps to launch")
    args   = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    #x = test_parallel_for(int(args.count))
    y = test_command_format_1()
    #raise_error(0)
