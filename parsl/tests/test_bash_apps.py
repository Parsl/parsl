''' Testing bash apps
'''
from parsl import *
import parsl

import os
import time
import shutil
import argparse

workers = ThreadPoolExecutor(max_workers=1)
#workers = ProcessPoolExecutor(max_workers=4)
#dfk = DataFlowKernel(workers)

@App('bash', workers)
def echo_to_file(inputs=[], outputs=[], stderr='std.err', stdout='std.out'):
    cmd_line = 'echo {inputs[0]} > {outputs[0]}'

def test_parallel_for (n):

    outdir='outputs'
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    else:
        shutil.rmtree(outdir)
        os.makedirs(outdir)

    d = {}

    for i in range(0,n):
        d[i] = echo_to_file(inputs=['Hello World {0}'.format(i)],
                            outputs=['{0}/out.{1}.txt'.format(outdir, i)],
                            stdout='d/std.{1}.out'.format(outdir, i),
                            stderr='d/std.{1}.err'.format(outdir, i),
        )
        #time.sleep(0.01)

    assert len(d.keys())   == n , "Only {0}/{1} keys in dict".format(len(d.keys()), n)
    time.sleep(1)
    #print([d[i][0].done() for i in d] )
    #print([d[i][0].exception() for i in d] )
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

    x = test_parallel_for(int(args.count))

    #raise_error(0)
