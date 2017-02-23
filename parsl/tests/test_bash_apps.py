''' Testing bash apps
'''
import parsl
from parsl import *
import os
import time
import shutil

#workers = ThreadPoolExecutor(max_workers=4)
workers = ProcessPoolExecutor(max_workers=4)
dfk = DataFlowKernel(workers)


@App('bash', dfk)
def echo(inputs=[], stderr='std.err', stdout='std.out'):
    cmd_line = 'echo {inputs[0]} {inputs[1]}'

@App('bash', dfk)
def echo_to_file(inputs=[], outputs=[], stderr='std.err', stdout='std.out'):
    cmd_line = 'echo {inputs} > {outputs[0]}'

@App('bash', dfk)
def sleep_n(t):
    cmd_line = 'sleep {t}'

@App('bash', dfk)
def cats_n_sleep (x, inputs, outputs):
    cmd_line = 'sleep $(($RANDOM % {x})); cat {inputs[0]} > {outputs[0]}'

@App('bash', dfk)
def incr (inputs, outputs):
    cmd_line = 'y=$(cat {inputs[0]}); echo $(($y+1)) > {outputs[0]}'

def test_parallel_for (n):

    outdir='outputs'
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    else:
        shutil.rmtree(outdir)
        os.makedirs(outdir)

    d = {}

    for i in range(0,n):
        d[i] = echo_to_file(inputs=['Hello World'], outputs=['{0}/out.{1}.txt'.format(outdir, i)])

    assert len(d.keys())   == n , "Only {0}/{1} keys in dict".format(len(d.keys()), n)
    time.sleep(1)
    print([d[i][0].done() for i in d] )
    assert len(os.listdir('outputs/')) == n , "Only {0}/{1} files in '{1}' ".format(len(os.listdir('outputs/')),
                                                                                    n, outdir)

    return d


if __name__ == '__main__' :
    parsl.set_stream_logger()
    x = test_parallel_for(10)
