import os
import pytest
import parsl
from parsl.app.app import bash_app
from parsl.data_provider.files import File
from parsl.tests.configs.local_threads import config


@bash_app
def cat(inputs=[], outputs=[], stdout=None, stderr=None):
    infiles = ' '.join([i.filepath for i in inputs])
    return f"""echo {infiles}
    cat {infiles} &> {outputs[0]}
    """


@pytest.mark.usefixtures('setup_data')
@pytest.mark.issue363
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
    return f"""
    x=$(cat {inputs[0]})
    echo $(($x+1)) > {outputs[0]}
    """


@pytest.mark.staging_required
def test_regression_200():
    """Regression test for #200. Pickleablility of Files"""

    if os.path.exists('test_output.txt'):
        os.remove('test_output.txt')

    with open("test.txt", 'w') as f:
        f.write("Hello World")

    fu = cat(inputs=[File("test.txt")],
             outputs=[File("test_output.txt")])
    fu.result()

    fi = fu.outputs[0].result()
    print(f"type of fi: {type(fi)}")
    with open(str(fi), 'r') as f:
        data = f.readlines()
        assert "Hello World" in data, "Missed data"


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
        if os.path.exists(f'test{i}.txt'):
            os.remove(f'test{i}.txt')
        print(f"Launching {i} with {prev}")
        fu = increment(inputs=[prev],  # Depend on the future from previous call
                       # Name the file to be created here
                       outputs=[File(f"test{i}.txt")],
                       stdout=f"incr{i}.out",
                       stderr=f"incr{i}.err")
        [prev] = fu.outputs
        futs[i] = prev
        print(prev.filepath)

    for key in futs:
        if key > 0:
            fu = futs[key]
            data = open(fu.result().filepath, 'r').read().strip()
            assert data == str(key), (f"[TEST] incr failed for key:{key} "
                                      f"got:{data}")


if __name__ == '__main__':
    parsl.clear()
    parsl.load(config)

    test_files()
    test_increment()
