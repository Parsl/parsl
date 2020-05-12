import time

import pytest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.htex_local import fresh_config


def local_setup():
    global dfk
    config = fresh_config()
    config.executors[0].init_blocks = 0
    config.executors[0].min_blocks = 0
    config.executors[0].max_blocks = 4
    dfk = parsl.load(config)


def local_teardown():
    parsl.clear()


@python_app
def diamond(sleep=0, inputs=[]):
    import time
    time.sleep(sleep)
    return sum(inputs)


@pytest.mark.local
@pytest.mark.skip('slow and does not assert anything')
def test_python(width=10):
    """Diamond pattern to scale from 0 -> 1 -> N -> 1 -> 0 """

    stage_1 = [diamond(sleep=60, inputs=[0])]

    stage_2 = []
    for i in range(0, width):
        stage_2.extend([diamond(sleep=20, inputs=stage_1)])

    stage_3 = [diamond(sleep=30, inputs=stage_2)]

    if not stage_3[0].done():
        time.sleep(30)
        for sitename in dfk.executors:
            print(dfk.executors[sitename].status())


if __name__ == "__main__":
    test_python()
