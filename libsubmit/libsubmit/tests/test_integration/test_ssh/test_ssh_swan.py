import libsubmit
from libsubmit import SSHChannel
from libsubmit import Torque
import time


def test_1():

    torque_config = {
        "site": "Swan.CrayPN",
        "execution": {
            "executor": "ipp",
            "provider": "torque",
            "channel": "ssh",
            "block": {
                "initBlocks": 1,
                "maxBlocks": 1,
                "minBlocks": 0,
                "taskBlocks": 1,
                "nodes": 1,
                "walltime": "00:25:00",
                "options": {
                    "partition": "debug",
                    "queue": "ivb12",
                }
            }
        }
    }

    channel = SSHChannel("swan.cray.com", "p01953", script_dir="parsl_scripts")
    ec, out, err = channel.execute_wait("which qsub; echo $HOSTNAME; pwd")
    print("Stdout: ", out)

    provider = Torque(config=torque_config, channel=channel)

    x = provider.submit('''echo "sleeping"
    sleep 120
    echo "Done sleeping" ''', 1)
    time.sleep(3)

    y = provider.submit('''echo "sleeping"
    sleep 120
    echo "Done sleeping" ''', 1)
    time.sleep(3)

    stats = provider.status([x, y])

    print("Trying to cancel : {0}  {1}".format(x, y))
    provider.cancel([x, y])
    print(stats)


if __name__ == "__main__":
    libsubmit.set_stream_logger()
    test_1()
