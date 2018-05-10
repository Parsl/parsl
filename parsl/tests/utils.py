import os
import random
import string


def get_tag(n=7):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))


def get_rundir(n=7):
    return 'runinfo_{}'.format(get_tag(n))


def get_config(filename):
    return os.path.join(os.path.dirname(__file__), 'configs', filename)
