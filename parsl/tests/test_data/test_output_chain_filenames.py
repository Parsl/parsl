import argparse
import os

import pytest

import parsl

from concurrent.futures import Future
from parsl import File
from parsl.app.app import bash_app


@bash_app
def app1(inputs=[], outputs=[], stdout=None, stderr=None, mock=False):
    cmd_line = f"""echo 'test' > {outputs[0]}"""
    return cmd_line


@bash_app
def app2(inputs=[], outputs=[], stdout=None, stderr=None, mock=False):

    with open('somefile.txt', 'w') as f:
        f.write("%s\n" % inputs[0])
    cmd_line = f"""echo '{inputs[0]}' > {outputs[0]}"""
    return cmd_line


def test_behavior():
    app1_future = app1(inputs=[],
                       outputs=[File("simple-out.txt")])

    o = app1_future.outputs[0]
    assert isinstance(o, Future)

    app2_future = app2(inputs=[o],
                       outputs=[File("simple-out2.txt")])
    app2_future.result()

    expected_name = 'b'
    with open('somefile.txt', 'r') as f:
        name = f.read()

    with open(app2_future.outputs[0].filepath, 'r') as f:
        expected_name = f.read()

    assert name == expected_name, "Filename mangled due to DataFuture handling"
