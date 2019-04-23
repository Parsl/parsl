from parsl import python_app

@python_app
def increment(x):
    return x + 1
