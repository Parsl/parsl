import parsl
import logging
import sys
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

hello = hello(SITE_ID, CLASS_PATH, numnodes=1, sitedir="git/site1", script='python', workdir='parsl/hello', appname='hello', callback=callback)
combine = combine(SITE_ID, CLASS_PATH, numnodes=1, sitedir="git/site1", script='bash', workdir='parsl/combine', appname='combine', callback=callback, inputs=[hello.result(), "There!"])
greetings = greetings(SITE_ID, CLASS_PATH, numnodes=1, sitedir="git/site1", script='bash', workdir='parsl/greetings', appname='greetings', callback=callback)

world = world(SITE_ID, CLASS_PATH, numnodes=1, sitedir="git/site1", script='bash', workdir='parsl/world', appname='world', callback=callback, inputs=[combine.result(), greetings.result()])

print(world.result())
