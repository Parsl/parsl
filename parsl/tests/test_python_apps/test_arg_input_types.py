"""
These tests check that inputs[] can be passed from any type,
and that they can be used as regular objects unless they are
Files.
"""
from parsl import python_app


@python_app
def take_a_value(inputs=[]):
    return str(inputs[0])


@python_app
def add_two_values(inputs=[]):
    return inputs[0] + inputs[1]


def test_input_str():
    f = take_a_value(inputs=["hi"])
    assert f.result() == "hi"


def test_input_num():
    f = take_a_value(inputs=[3.14])
    assert f.result() == "3.14"

    f = add_two_values(inputs=[5, 7, 3])
    assert f.result() == 12


def test_input_list():
    f = take_a_value(inputs=[["foo", 7]])
    assert f.result() == "['foo', 7]"
