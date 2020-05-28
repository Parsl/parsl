#!/usr/bin/env python3

"""Test process layer.

<bash>
Foreach ….
    10 K app calls in parallel

<bash 30 s> wait on all for loop -> .csv

Foreach .. from .csv
    100K app calls in parallel. Needs softImage

<Bash> wait on all for loop

"""

import parsl
from parsl import bash_app, python_app, DataFlowKernel, ThreadPoolExecutor
import os
import shutil
import random
import argparse


workers = ThreadPoolExecutor(max_workers=8)
dfk = DataFlowKernel(executors=[workers])


def create_dirs(cwd):

    for dir in ['relax.01', 'relax.02', 'relax.03']:
        rel_dir = '{0}/{1}'.format(cwd, dir)
        if os.path.exists(rel_dir):
            shutil.rmtree(rel_dir)
        os.makedirs(rel_dir)
        for i in range(0, random.randint(1, 5)):
            rdir = '{0}/{1}'.format(rel_dir, i)
            os.makedirs(rdir)
            with open('{0}/results'.format(rdir), 'w') as f:
                f.write("{0} {1} - test data\n".format(i, dir))

    for dir in ['neb01', 'neb02', 'neb03', 'neb04']:
        rel_dir = '{0}/{1}'.format(cwd, dir)
        if os.path.exists(rel_dir):
            shutil.rmtree(rel_dir)
        os.makedirs(rel_dir)
        with open('{0}/{1}.txt'.format(rel_dir, dir), 'w') as f:
            f.write("{0} test data\n".format(rel_dir))


@python_app(data_flow_kernel=dfk)
def ls(pwd, outputs=[]):
    import os
    items = os.listdir(pwd)
    with open(outputs[0], 'w') as f:
        f.write(' '.join(items))
        f.write('\n')
    # Returning list of items in current dir as python object
    return items


@bash_app(data_flow_kernel=dfk)
def catter(dir, dur, inputs=[], outputs=[], stdout=None, stderr=None):
    cmd_line = 'cd {0}; echo "sleeping... "; sleep {1}'
    return cmd_line


@bash_app(data_flow_kernel=dfk)
def light_app(dir, dur, inputs=[], outputs=[], stdout=None, stderr=None):
    cmd_line = 'cd {0}; echo "light_app" > {outputs[0]} ; sleep {1}'
    return cmd_line


@bash_app(data_flow_kernel=dfk)
def csv_maker(dir, count, dur, inputs=[], outputs=[], stdout=None, stderr=None):

    cmd_line = """cd {0};
    # create a file with count lines
    shuf -i 1-{1} &> {outputs[0]}
    sleep {2}
    """
    return cmd_line


def main(count):

    # <Bash app >
    c1 = catter('.', 0, stdout='outputs/catter1.out',
                stderr='outputs/catter1.err')

    light_loop = []
    # Foreach parallel loop 10K calls
    for i in range(count):
        if i % 1000:
            print("Launching light : ", i)
        outname = 'outputs/light{0}'.format(i)
        loop1 = light_app('.', 0, inputs=[c1], outputs=[
                          outname + '.txt'], stdout=outname + '.out')
        light_loop.extend([loop1])

    # <Bash app dependent on for loop>
    c2 = csv_maker('.', count * 10, 0, inputs=light_loop, outputs=['csv_maker.csv'],
                   stdout='outputs/catter2.out', stderr='outputs/catter2.err')
    csv_file = c2.outputs[0]

    # This is a blocking call that forces the workflow to wait for the csv_file to be produced.
    lines = open(csv_file.result(), 'r').readlines()
    mid_loop = []

    # Foreach parallel loop 10K calls
    for i in lines:
        i = i.strip()
        outname = 'outputs/mid{0}'.format(i)
        loop1 = light_app('.', 0, inputs=[c1], outputs=[
                          outname + '.txt'], stdout=outname + '.out')
        mid_loop.extend([loop1])

    # <Bash app dependent on mid for loop>
    c3 = catter('.', 3, inputs=mid_loop,
                stdout='outputs/catter3.out', stderr='outputs/catter3.err')
    return c3


def test_HEDM(count=10):
    x = main(count)
    x.result()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    # Remove outputs and recreate
    shutil.rmtree("outputs", True)
    os.mkdir("outputs")
    # x = test_parallel_for(int(args.count))
    x = main(int(args.count))
    print("Waiting for workflow result")
    x.result()
