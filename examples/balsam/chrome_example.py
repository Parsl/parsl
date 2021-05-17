import parsl
from parsl.app.app import python_app, bash_app
from parsl.config import Config
from parsl.executors.chrome.executor import ChromeExecutor


config = Config(
    executors=[
        ChromeExecutor(
            maxworkers=3,
            numnodes=1,
            timeout=60
        )
    ]
)

parsl.load(config)


def callback(future,**kwargs):

    if not future.cancelled():
        print('Callback result: ', future.result())
    else:
        print('Future was cancelled!')


@python_app(executors=["ChromeExecutor"])
def greetings(inputs=[]):
    return "Greetings"


@python_app(executors=["ChromeExecutor"])
def hello(inputs=[]):
    return "Hello! "+inputs[0]


_greetings = greetings(callback=callback)
_hello = hello(inputs=[_greetings])
print(_hello.result())
