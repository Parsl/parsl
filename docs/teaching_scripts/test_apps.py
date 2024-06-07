"""Tests documentation related to building apps. Must reside outside the Parsl library to be effective"""
from typing import List, Union

import numpy as np

import parsl
from parsl import Config, HighThroughputExecutor, python_app

parsl.load(Config(executors=[HighThroughputExecutor(label='htex_spawn', max_workers_per_node=1, address='127.0.0.1')]))


# Part 1: Explain imports
# BAD: Assumes library has been imported
@python_app(executors=['htex_spawn'])
def bad_imports(x: Union[List[float], np.ndarray], m: float, b: float):
    return np.multiply(x, m) + b


# GOOD: Imports libraries itself
@python_app(executors=['htex_spawn'])
def good_imports(x: Union[List[float], 'np.ndarray'], m: float, b: float):
    import numpy as np
    return np.multiply(x, m) + b


future = bad_imports([1.], 1, 0)

try:
    future.result()
    raise ValueError()
except NameError as e:
    print('Failed, as expected. Error:', e)

future = good_imports([1.], 1, 0)
print(f'Passed, as expected: {future.result()}')

# Part 2: Test other types of globals
# BAD: Uses global variables
global_var = {'a': 0}


@python_app
def bad_global(string: str, character: str = 'a'):
    global_var[character] += string.count(character)  # `global_var` will not be accessible


# GOOD
@python_app
def good_global(string: str, character: str = 'a'):
    return {character: string.count(character)}


try:
    bad_global('parsl').result()
except NameError as e:
    print(f'Failed, as expected: {e}')

for ch, co in good_global('parsl', 'a').result().items():
    global_var[ch] += co


# Part 3: Mutable args

# BAD: Assumes changes to inputs will be communicated
@python_app
def append_to_list(input_list: list, new_val):
    input_list.append(new_val)


mutable_arg = []
append_to_list(mutable_arg, 1).result()
assert mutable_arg == [], 'The list _was_changed'


# GOOD: Changes to inputs are returned
@python_app
def append_to_list(input_list: list, new_val) -> list:
    input_list.append(new_val)
    return input_list


mutable_arg = append_to_list(mutable_arg, 1).result()
assert mutable_arg == [1]
