import parsl
from parsl.app.app import python_app, singularity_app
from parsl.config import Config
from parsl.providers.local.local import LocalProvider
from parsl.executors import HighThroughputExecutor

config = Config(
    executors=[
        HighThroughputExecutor(
            label='local_htex',
            max_workers=2,
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


@singularity_app(image="/home/darren/alcf/singularity/git/gsas2container/gsas2.img", cmd="/home/darren/alcf/singularity/git/singularity/builddir/singularity")
@python_app(executors=['local_htex'])
def hello(inputs=[]):
    h = {'message': 'hello'}
    return h


hi = hello()
print("RESULT:", hi.result())
