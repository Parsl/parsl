from parsl import *
import parsl

workers = ThreadPoolExecutor(max_workers=4)
@App('bash', workers)
def echo(inputs=[], stderr='std.err', stdout='std.out'):
    cmd_line = 'echo {inputs[0]} {inputs[1]}'

@App('bash', workers)
def sleep_n(t):
    cmd_line = 'sleep {t}'

@App('bash', workers)
def cats_n_sleep (x, inputs, outputs):
    cmd_line = 'sleep $(($RANDOM % {x})); cat {inputs[0]} > {outputs[0]}'

@App('bash', workers)
def incr (inputs, outputs):
    cmd_line = 'y=$(cat {inputs[0]}); echo $(($y+1)) > {outputs[0]}'

@App('bash', workers)
def raise_error (inputs, outputs):
    cmd_line = 'y=$(cat {inputs[0]}); echo $(($y+1)) > {outputs[0]}'
