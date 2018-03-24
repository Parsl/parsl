"""Testing bash apps
"""
import parsl
from parsl import *

print("Parsl version: ", parsl.__version__)


# parsl.set_stream_logger()
workers = ThreadPoolExecutor(max_workers=8)
dfk = DataFlowKernel(executors=[workers])


@App('bash', dfk)
def generate(outputs=[]):
    return "echo $(( RANDOM % (10 - 5 + 1 ) + 5 )) &> {outputs[0]}"


@App('bash', dfk)
def concat(inputs=[], outputs=[], stdout="stdout.txt", stderr='stderr.txt'):
    return "cat {0} >> {1}".format(" ".join(map(lambda x: x.filepath, inputs)), outputs[0])


@App('python', dfk)
def total(inputs=[]):
    total = 0
    with open(inputs[0].filepath, 'r') as f:
        for l in f:
            total += int(l)
    return total


def test_parallel_dataflow():
    """Test parallel dataflow from docs on Composing workflows
    """

    # create 5 files with random numbers
    output_files = []
    for i in range(5):
        output_files.append(generate(outputs=['random-%s.txt' % i]))

    # concatenate the files into a single file
    cc = concat(inputs=[i.outputs[0]
                        for i in output_files], outputs=["all.txt"])

    # calculate the average of the random numbers
    totals = total(inputs=[cc.outputs[0]])
    print(totals.result())


if __name__ == "__main__":

    test_parallel_dataflow()
