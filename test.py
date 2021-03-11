import parsl
import logging
import sys
from parsl.app.app import balsam_app
from parsl.config import Config
from parsl.executors.balsam.executor import BalsamExecutor

logging.disable(sys.maxsize)

config = Config(
    executors=[
        BalsamExecutor(
            siteid=1,
            project='local'
        )
    ],
)

parsl.load(config)


def callback(future,**kwargs):

    if not future.cancelled():
        print('Callback result: ', future.result())
    else:
        print('Future was cancelled!')


@balsam_app(executors=["BalsamExecutor"])
def hello(inputs=[]):
    return "echo Hello"


@balsam_app(executors=["BalsamExecutor"])
def world(inputs=[]):
    return "echo {} World".format(inputs[0])

SITE_ID = 1
CLASS_PATH = 'parslapprunner.ParslAppRunner'

print("Running hello app...")
hello1 = hello(SITE_ID, CLASS_PATH, numnodes=1, sitedir="git/site1", script='bash', workdir='parsl/hello1', appname='hello1', callback=callback)
hello2 = hello(SITE_ID, CLASS_PATH, numnodes=1, sitedir="git/site1", script='bash', workdir='parsl/hello2', appname='hello2', callback=callback)
# Callback result: Hello
print('Getting hello1')
print('Hello1: ', hello1.result())
print('Getting hello2')
print('Hello2: ', hello2.result())
print("Running world app...")
world = world(SITE_ID, CLASS_PATH, numnodes=1, sitedir="git/site1", script='bash', workdir='parsl/world', appname='world', callback=callback, inputs=[hello1.result()])

# Callback result: World

print(world.result())
# Hello World
