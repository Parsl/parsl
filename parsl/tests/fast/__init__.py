import os
import sys

if os.environ.get('PARSL_TESTING', 'False').lower() is not 'true':
    print(os.environ['PARSL_TESTING'])
    raise RuntimeError("must first run 'export PARSL_TESTING=True'")
