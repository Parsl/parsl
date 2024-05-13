from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor


def fresh_config():
    tpe = ThreadPoolExecutor(label='local_threads_checkpoint_periodic', max_threads=1)
    return Config(
        executors=[tpe],
        checkpoint_mode='periodic',
        checkpoint_period='00:00:02'
    )
