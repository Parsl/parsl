import libsubmit
from libsubmit import EC2Provider as ec2
import time


def test_1():

    config = {
        "site": "ec2",
        "auth": {
            "foo": "foo"
        },
        "execution": {
            "executor": "ipp",
            "provider": "ec2",
            "channel": None,
            "block": {
                "initBlocks": 1,
                "maxBlocks": 1,
                "minBlocks": 0,
                "taskBlocks": 1,
                "nodes": 1,
                "walltime": "00:25:00",
                "options": {
                    "region": "us-east-2",
                    "imageId": 'ami-82f4dae7',
                    "stateFile": "awsproviderstate.json",
                    "keyName": "parsl.test"
                }
            }
        }
    }

    provider = ec2(config=config, channel=None)

    x = provider.submit('''echo "sleeping"
    sleep 120
    echo "Done sleeping" ''', 1)
    time.sleep(3)

    y = provider.submit('''echo "sleeping"
    sleep 120
    echo "Done sleeping" ''', 1)
    time.sleep(3)

    stats = provider.status([x, y])

    provider.cancel([x, y])
    print(stats)


def test_2():

    config = {
        "site": "ec2",
        "auth": {
            "profile": "default"
        },
        "execution": {
            "executor": "ipp",
            "provider": "ec2",
            "channel": None,
            "block": {
                "initBlocks": 1,
                "maxBlocks": 1,
                "minBlocks": 0,
                "taskBlocks": 1,
                "nodes": 1,
                "walltime": "00:25:00",
                "options": {
                    "region": "us-east-2",
                    "imageId": 'ami-82f4dae7',
                    "stateFile": "awsproviderstate.json",
                    "keyName": "parsl.test"
                }
            }
        }
    }

    provider = ec2(config=config, channel=None)

    x = provider.submit('''echo "sleeping"
    sleep 120
    echo "Done sleeping" ''', 1)
    time.sleep(3)

    y = provider.submit('''echo "sleeping"
    sleep 120
    echo "Done sleeping" ''', 1)
    time.sleep(3)

    print("X : ", x)
    print("Y : ", y)
    stats = provider.status([x, y])
    print("Status : ", stats)

    provider.cancel([x, y])

    # provider.teardown()


if __name__ == "__main__":
    libsubmit.set_stream_logger()
    # test_1 ()
    test_2()
