import parsl
from parsl.app.app import python_app, bash_app
from parsl.config import Config
from parsl.executors.balsam.executor import BalsamExecutor


config = Config(
    executors=[
        BalsamExecutor(
            siteid=1,
            maxworkers=3,
            numnodes=1,
            timeout=60,
            node_packing_count=8,
            sitedir='git/site1',
            project='local'
        )
    ]
)

parsl.load(config)


def callback(future,**kwargs):

    if not future.cancelled():
        print('Callback result: ', future.result())
    else:
        print('Future was cancelled!')


@python_app(executors=["BalsamExecutor"])
def greetings(inputs=[]):
    return "Greetings"


@python_app(executors=["BalsamExecutor"])
def hello(inputs=[]):
    return "Hello"


@bash_app(executors=["BalsamExecutor"])
def combine(inputs=[]):
    return "echo {} {}".format(inputs[0], inputs[1])


@bash_app(executors=["BalsamExecutor"])
def world(inputs=[]):
    return "echo {} {} World!".format(inputs[0], inputs[1])


hello = hello(callback=callback)
combine = combine(callback=callback, inputs=[hello, "There!"])
greetings = greetings(callback=callback, timeout=60)

world = world(callback=callback, inputs=[combine, greetings])

print(world.result())
