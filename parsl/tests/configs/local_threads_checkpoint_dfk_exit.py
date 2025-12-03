from parsl.config import Config
from parsl.dataflow.memoization import BasicMemoizer
from parsl.executors.threads import ThreadPoolExecutor

config = Config(
    executors=[
        ThreadPoolExecutor(
            label='local_threads_checkpoint_dfk_exit',
        )
    ],
    memoizer=BasicMemoizer(checkpoint_mode='dfk_exit')
)
