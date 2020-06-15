import os

import pytest

import parsl
from parsl.app.app import bash_app
from parsl.data_provider.files import File
from parsl.tests.configs.local_threads import config


@bash_app
def cat(inputs=[], outputs=[], stdout=None, stderr=None):
    infiles = ' '.join([i.filepath for i in inputs])
    return """echo {i}
    cat {i} &> {o}
    """.format(i=infiles, o=outputs[0])


@pytest.mark.usefixtures('setup_data')
@pytest.mark.staging_required
def test_files():

    if os.path.exists('cat_out.txt'):
        os.remove('cat_out.txt')

    fs = [File('data/' + f) for f in os.listdir('data')]
    x = cat(inputs=fs, outputs=[File('cat_out.txt')],
            stdout='f_app.out', stderr='f_app.err')
    d_x = x.outputs[0]
    print(x.result())
    print(d_x, type(d_x))


@bash_app
def increment(inputs=[], outputs=[], stdout=None, stderr=None):
    # Place double braces to avoid python complaining about missing keys for {item = $1}
    return """
    x=$(cat {i})
    echo $(($x+1)) > {o}
    """.format(i=inputs[0], o=outputs[0])


@pytest.mark.usefixtures('setup_data')
@pytest.mark.staging_required
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

        if os.path.exists('test{0}.txt'.format(i)):
            os.remove('test{0}.txt'.format(i))

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
    parsl.clear()
    parsl.load(config)

    test_files()
    test_increment()
