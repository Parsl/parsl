import parsl
from parsl.app.app import balsam_app
from parsl.config import Config
from parsl.executors.balsam.executor import BalsamExecutor


config = Config(
    executors=[
        BalsamExecutor(
            siteid=1,
            maxworkers=3,
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


@balsam_app(executors=["BalsamExecutor"])
def greetings(inputs=[]):
    return "echo Greetings"


@balsam_app(executors=["BalsamExecutor"])
def hello(inputs=[]):
    return "Hello"


@balsam_app(executors=["BalsamExecutor"])
def combine(inputs=[]):
    return "echo {} {}".format(inputs[0], inputs[1])


@balsam_app(executors=["BalsamExecutor"])
def world(inputs=[]):
    return "echo {} {} World!".format(inputs[0], inputs[1])


SITE_ID = 1
CLASS_PATH = 'parslapprunner.ParslAppRunner'

settings = {
    'sitedir': 'git/site1',
    'callback': callback,
    'siteid': SITE_ID,
    'numnodes': 1,
    'classpath': CLASS_PATH
}
hello = hello(**settings, script='python', workdir='parsl/hello', appname='hello')
combine = combine(**settings, script='bash', workdir='parsl/combine', appname='combine', inputs=[hello, "There!"])
greetings = greetings(**settings, script='bash', timeout=60, workdir='parsl/greetings', appname='greetings')

world = world(**settings, script='bash', workdir='parsl/world', appname='world', inputs=[combine, greetings])

print(world.result())
