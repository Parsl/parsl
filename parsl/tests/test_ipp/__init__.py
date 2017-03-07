import os
import time
import subprocess

def setup():
    print("Starting cluster")
    #proc = subprocess.Popen(["ipcluster start -n 4 --daemon"], shell=True, executable="/bin/bash")
    #print("Sleeping to allow for slow start")
    #time.sleep(20)

def teardown():
    print("Terminating cluster")
    #os.system("ipcluster stop")
    #time.sleep(3)

if __name__ == "__main__" :

    setup_package()
    print("Done with setup")

    teardown_package()
    print("End of setup")
