import os

import pytest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.taskvine_ex import fresh_config


def f_context(y, input_file_names):
    bases = []
    for file_name in input_file_names:
        with open(file_name, 'r') as f:
            bases.append(int(f.read().strip()))

    return {'y': sum(bases) + y * 2}


@python_app
def f_compute(x):
    from parsl.executors.taskvine.utils import load_variable_in_serverless
    y = load_variable_in_serverless('y')
    return y + x + 1


@pytest.mark.taskvine
@pytest.mark.parametrize('num_tasks', (1, 50))
@pytest.mark.parametrize('fresh_config', [fresh_config])
def test_function_context_computation(num_tasks, fresh_config):
    try:
        with parsl.load(fresh_config):
            input_file_names = ['input1.test_taskvine_context.data', 'input2.test_taskvine_context.data']
            input_data = [7, 8]
            for i, file_name in enumerate(input_file_names):
                with open(file_name, 'w') as f:
                    print(input_data[i], file=f)

            futs = []
            x = 5
            y = 1
            function_context_input_files = {}
            for v in input_file_names:
                function_context_input_files[v] = v
            for _ in range(num_tasks):
                futs.append(f_compute(x,
                                      parsl_resource_specification={
                                          'exec_mode': 'serverless',
                                          'function_context': f_context,
                                          'function_context_args': [y, input_file_names],
                                          'function_context_input_files': function_context_input_files}))
            total_sum = 0
            for i in range(num_tasks):
                total_sum += futs[i].result()

            expected = (f_compute(x) + f_context(y, input_file_names)['y']) * num_tasks
            assert total_sum == expected
    finally:
        for file_name in input_file_names:
            os.remove(file_name)
