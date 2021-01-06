import os


def get_config(filename):
    return os.path.join(os.path.dirname(__file__), 'configs', filename)
