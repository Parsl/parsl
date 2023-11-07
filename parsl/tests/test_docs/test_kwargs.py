"""Functions used to explain kwargs"""
from pathlib import Path

from parsl import python_app, File


def test_inputs():
    @python_app()
    def map_app(x):
        return x * 2

    @python_app()
    def reduce_app(inputs=()):
        return sum(inputs)

    map_futures = [map_app(x) for x in range(3)]
    reduce_future = reduce_app(inputs=map_futures)

    assert reduce_future.result() == 6


def test_outputs(tmpdir):
    @python_app()
    def write_app(message, outputs=()):
        """Write a single message to every file in outputs"""
        for path in outputs:
            with open(path, 'w') as fp:
                print(message, file=fp)

    to_write = [
        File(Path(tmpdir) / 'output-0.txt'),
        File(Path(tmpdir) / 'output-1.txt')
    ]
    write_app('Hello!', outputs=to_write).result()
    for path in to_write:
        with open(path) as fp:
            assert fp.read() == 'Hello!\n'
