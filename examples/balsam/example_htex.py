import parsl
from parsl.app.app import python_app
from parsl.config import Config
from parsl.providers.singularity import SingularityProvider
from parsl.providers.local.local import LocalProvider
from parsl.executors import HighThroughputExecutor

config = Config(
    executors=[
        HighThroughputExecutor(
            label='local_htex',
            provider=LocalProvider(
                min_blocks=1,
                init_blocks=1,
                max_blocks=2,
                nodes_per_block=1,
                parallelism=0.5
            )
        )
    ]
)

parsl.load(config)


def callback(future, **kwargs):

    if not future.cancelled():
        print('Callback result: ', future.result())
    else:
        print('Future was cancelled!')


@python_app(executors=['local_htex'])
def hello():
    h = {'message': 'hello'}
    return h


hi = hello(callback=callback)
print(hi.result())
