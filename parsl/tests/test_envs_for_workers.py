import pytest
import os
import subprocess
import parsl
from parsl import App
from parsl.providers import LocalProvider
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.threads import ThreadPoolExecutor

config = Config(
    executors=[
        ThreadPoolExecutor(max_threads=4, label='local_threads'),
        IPyParallelExecutor(
            label='local_ipp',
            engine_dir='engines',
            provider=LocalProvider(
                walltime="00:05:00",
                nodes_per_block=1,
                worker_env={'word': 'bird', 'drink': '"some coffee"', 'x': 3},
                init_blocks=4
            )
        )
    ],
)
parsl.clear()
parsl.load(config)


@App('python')
def get_word_env():
    return os.environ.get('word', 'Not found')


@App('python')
def get_all_env():
    return os.environ.get('word', 'Not found'), os.environ.get('drink', 'Not found'), os.environ.get('x', 'Not found')


@App('python')
def get_word_env_subprocess():
    return subprocess.run('echo $word', shell=True, capture_output=True).stdout.decode().strip()


@pytest.mark.local
def test_setting_envs_for_local_provider():
    assert get_word_env().result() == 'bird', 'Single environment variable not available for first level python script.'
    assert get_all_env().result() == ('bird', 'some coffee', '3'), "Multiple environment variables not available for first level python script."
    assert get_word_env_subprocess().result() == 'bird', "Single environment variable not available for second level python script."


if __name__ == "__main__":
    test_setting_envs_for_local_provider()
