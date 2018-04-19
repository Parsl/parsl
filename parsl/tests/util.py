import random
import string

def get_rundir(n=7):
    tag = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))
    return 'runinfo_{}'.format(tag)
