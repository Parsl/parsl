from parsl import App

@App('python')
def increment(x):
    return x + 1
