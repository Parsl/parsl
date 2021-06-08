import argparse
import os
import pytest

import parsl
from parsl.app.app import bash_app
from parsl.data_provider.files import File
from parsl.app.futures import DataFuture

from parsl.tests.configs.local_threads import config


@bash_app
def increment(inputs=[], outputs=[], stdout=None, stderr=None):
    cmd_line = f"""
    if ! [ -f {inputs[0]} ] ; then exit 43 ; fi
    x=$(cat {inputs[0]})
    echo $(($x+1)) > {outputs[0]}
    """
    return cmd_line


@bash_app
def slow_increment(dur, inputs=[], outputs=[], stdout=None, stderr=None):
    cmd_line = f"""
    x=$(cat {inputs[0]})
    echo $(($x+1)) > {outputs[0]}
    sleep {dur}
    """
    return cmd_line


def cleanup_work(depth):
    for i in range(depth):
        fn = f"test{i}.txt"
        if os.path.exists(fn):
            os.remove(fn)


@pytest.mark.staging_required
def test_increment(depth=5):
    """Test simple pipeline A->B...->N
    """

    cleanup_work(depth)

    # Create the first file
    open("test0.txt", 'w').write('0\n')

    # Create the first entry in the dictionary holding the futures
    prev = File("test0.txt")
    futs = {}
    for i in range(1, depth):
        print(f"Launching {i} with {prev}")
        assert(isinstance(prev, DataFuture) or isinstance(prev, File))
        output = File(f"test{i}.txt")
        fu = increment(inputs=[prev],  # Depend on the future from previous call
                       # Name the file to be created here
                       outputs=[output],
                       stdout=f"incr{i}.out",
                       stderr=f"incr{i}.err")
        [prev] = fu.outputs
        futs[i] = prev
        print(prev.filepath)
        assert(isinstance(prev, DataFuture))

    for key in futs:
        if key > 0:
            fu = futs[key]
            file = fu.result()
            filename = file.filepath

            # this test is a bit close to a test of the specific implementation
            # of File
            assert file.local_path is None, (f"File on local side has "
                                             f"overridden local_path, "
                                             f"file: {repr(file)}")
            assert file.filepath == f"test{key}.txt", ("Submit side filepath "
                                                       "has not been preserved "
                                                       "over execution")

            data = open(filename, 'r').read().strip()
            assert data == str(key), (f"[TEST] incr failed for key: {key} got "
                                      f"data: {data} from filename {filename}")

    cleanup_work(depth)


@pytest.mark.staging_required
def test_increment_slow(depth=5, dur=0.5):
    """Test simple pipeline slow (sleep.5) A->B...->N
    """

    cleanup_work(depth)

    # Create the first file
    open("test0.txt", 'w').write('0\n')

    prev = File("test0.txt")
    # Create the first entry in the dictionary holding the futures
    futs = {}
    print("************** Type: ", type(dur), dur)
    for i in range(1, depth):
        print(f"Launching {i} with {dur}")
        output = File(f"test{i}.txt")
        fu = slow_increment(dur,
                            # Depend on the future from previous call
                            inputs=[prev],
                            # Name the file to be created here
                            outputs=[output],
                            stdout=f"incr{i}.out",
                            stderr=f"incr{i}.err")
        [prev] = fu.outputs
        futs[i] = prev
        print(prev.filepath)

    for key in futs:
        if key > 0:
            fu = futs[key]
            data = open(fu.result().filepath, 'r').read().strip()
            assert data == str(key), (f"[TEST] incr failed for key: "
                                      f"{key} got: {data}")

    cleanup_work(depth)


if __name__ == '__main__':
    parsl.clear()
    dfk = parsl.load(config)

    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--width", default="5",
                        help="width of the pipeline")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    # test_increment(depth=int(args.width))
    # test_increment(depth=int(args.width))
    test_increment_slow(depth=int(args.width))
