"""Testing bash apps
"""
import parsl
from parsl import *
import argparse

# parsl.set_stream_logger()
workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('bash', dfk)
def app1(inputs=[], outputs=[], stdout=None, stderr=None, mock=False):
    cmd_line = """echo 'test' > {outputs[0]}"""
    return cmd_line


@App('bash', dfk)
def app2(inputs=[], outputs=[], stdout=None, stderr=None, mock=False):

    with open('somefile.txt', 'w') as f:
        f.write("%s\n" % inputs[0])
    cmd_line = """echo '{inputs[0]}' > {outputs[0]}"""
    return cmd_line


def test_behavior():
    app1_future = app1(inputs=[],
                       outputs=["simple-out.txt"])
    # app1_future.result()

    app2_future = app2(inputs=[app1_future.outputs[0]],
                       outputs=["simple-out2.txt"])
    app2_future.result()

    name = 'a'
    expected_name = 'b'
    with open('somefile.txt', 'r') as f:
        name = f.read()

    with open(app2_future.outputs[0].filepath, 'r') as f:
        expected_name = f.read()

    assert name == expected_name, "Filename mangled due to DataFuture handling"


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_behavior()

    # raise_error(0)
