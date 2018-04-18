import os

if str(os.environ.get('PARSL_TESTING', False)).lower() != 'true':
    raise RuntimeError("must first run 'export PARSL_TESTING=True'")

"""
def setup_package():
    import subprocess
    import time
    proc = subprocess.Popen(["ipcluster", "start", "-n", "4"])
    time.sleep(2)
    print("Started ipcluster with pid:{0}".format(proc))
    return proc


def teardown_package():
    import subprocess
    import time
    proc = subprocess.Popen(["ipcluster", "stop"])
    print("Stopping ipcluster")
    time.sleep(2)
    return proc
"""
