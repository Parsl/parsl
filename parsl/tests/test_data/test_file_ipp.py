from parsl import *
import os

from parsl.data_provider.files import File

# parsl.set_stream_logger()
from parsl.configs.local import localIPP as config
dfk = DataFlowKernel(config=config)


@App('bash', dfk)
def cat(inputs=[], outputs=[], stdout=None, stderr=None):
    infiles = ' '.join([i.filepath for i in inputs])
    return """echo %s
    cat %s &> {outputs[0]}
    """ % (infiles, infiles)


def test_files():

    fs = [File('data/' + f) for f in os.listdir('data')]
    x = cat(inputs=fs, outputs=['cat_out.txt'],
            stdout='f_app.out', stderr='f_app.err')
    d_x = x.outputs[0]
    print(x.result())
    print(d_x, type(d_x))


@App('bash', dfk)
def increment(inputs=[], outputs=[], stdout=None, stderr=None):
    # Place double braces to avoid python complaining about missing keys for {item = $1}
    return """
    x=$(cat {inputs[0]})
    echo $(($x+1)) > {outputs[0]}
    """


def test_regression_200():
    """Regression test for #200. Pickleablility of Files"""

    with open("test.txt", 'w') as f:
        f.write("Hello World")

    fu = cat(inputs=[File("test.txt")],
             outputs=[File("test_output.txt")])
    fu.result()

    with open(fu.outputs[0].result(), 'r') as f:
        data = f.readlines()
        assert "Hello World" in data, "Missed data"


def test_increment(depth=5):
    """Test simple pipeline A->B...->N
    """
    # Create the first file
    open("test0.txt", 'w').write('0\n')

    # Create the first entry in the dictionary holding the futures
    prev = File("test0.txt")
    futs = {}
    for i in range(1, depth):
        print("Launching {0} with {1}".format(i, prev))
        fu = increment(inputs=[prev],  # Depend on the future from previous call
                       # Name the file to be created here
                       outputs=[File("test{0}.txt".format(i))],
                       stdout="incr{0}.out".format(i),
                       stderr="incr{0}.err".format(i))
        [prev] = fu.outputs
        futs[i] = prev
        print(prev.filepath)

    for key in futs:
        if key > 0:
            fu = futs[key]
            data = open(fu.result().filepath, 'r').read().strip()
            assert data == str(
                key), "[TEST] incr failed for key:{0} got:{1}".format(key, data)


if __name__ == '__main__':

    test_files()
    test_increment()
