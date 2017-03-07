import os
import time
import subprocess

def setup_package():
    print("Starting cluster")
    os.system("ipcluster start -n 4 &")
    proc = subprocess.Popen(["ipcluster start -n 4 &"], shell=True, executable="/bin/bash")
    print("Sleeping to allow for slow start")
    time.sleep(10)

def teardown_package():
    print("Terminating cluster")
    os.system("ipcluster stop")
    time.sleep(3)

if __name__ == "__main__" :

    setup_package()
    print("Done with setup")

    teardown_package()
    print("End of setup")
