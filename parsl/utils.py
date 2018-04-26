import os
import subprocess
import shlex

import parsl
from parsl.version import VERSION

def get_version():
    if 'site-packages' in __file__:
        version = parsl.__version__
    else:
        start = os.getcwd()
        os.chdir(os.path.dirname(__file__))
        try:
            head = subprocess.check_output(shlex.split('git rev-parse --short HEAD')).strip().decode('utf-8')
            diff = subprocess.check_output(shlex.split('git diff'))
            status = 'dirty' if diff else 'clean'
            version = '{major}-{head}-{status}'.format(major=VERSION, head=head, status=status)
        except subprocess.CalledProcessError:
            version = VERSION
        finally:
            os.chdir(start)
    return version
