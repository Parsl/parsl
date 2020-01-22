import parsl

from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


@python_app
def generate(limit):
    from random import randint
    """Generate a random integer and return it"""
    return randint(1, limit)


def test_parallel_for(N=2):
    """Test parallel workflows from docs on Composing workflows
    """
    rand_nums = []
    for i in range(1, 5):
        rand_nums.append(generate(i))

    # wait for all apps to finish and collect the results
    outputs = [i.result() for i in rand_nums]
    return outputs


if __name__ == "__main__":

    parsl.clear()
    parsl.load(config)
    test_parallel_for()
