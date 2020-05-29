import os
import parsl

from parsl.app.app import bash_app, python_app
from parsl.tests.configs.local_threads import config
from parsl.data_provider.files import File

# parsl.set_stream_logger()


@bash_app
def generate(outputs=[]):
    return "echo $(( RANDOM % (10 - 5 + 1 ) + 5 )) &> {o}".format(o=outputs[0])


@bash_app
def concat(inputs=[], outputs=[], stdout="stdout.txt", stderr='stderr.txt'):
    return "cat {0} >> {1}".format(" ".join(map(lambda x: x.filepath, inputs)), outputs[0])


@python_app
def total(inputs=[]):
    total = 0
    with open(inputs[0].filepath, 'r') as f:
        for line in f:
            total += int(line)
    return total


def test_parallel_dataflow():
    """Test parallel dataflow from docs on Composing workflows
    """

    if os.path.exists('all.txt'):
        os.remove('all.txt')

    # create 5 files with random numbers
    output_files = []
    for i in range(5):
        if os.path.exists('random-%s.txt' % i):
            os.remove('random-%s.txt' % i)
        output_files.append(generate(outputs=[File('random-%s.txt' % i)]))

    # concatenate the files into a single file
    cc = concat(inputs=[i.outputs[0]
                        for i in output_files], outputs=[File("all.txt")])

    # calculate the average of the random numbers
    totals = total(inputs=[cc.outputs[0]])
    print(totals.result())


if __name__ == "__main__":
    parsl.clear()
    parsl.load(config)

    test_parallel_dataflow()
