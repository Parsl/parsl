import os

if str(os.environ.get('PARSL_TESTING', False)).lower() != 'true':
    raise RuntimeError("must first run 'export PARSL_TESTING=True'")
