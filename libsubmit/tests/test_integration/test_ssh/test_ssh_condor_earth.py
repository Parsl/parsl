import os
import libsubmit
from libsubmit import SSHChannel, Condor
import time


def test_1():
    config = {
        "site": "T3_US_NotreDame",
        "execution": {
            "script_dir": ".scripts",
            "environment": {
                'CONDOR_CONFIG': '/opt/condor/RedHat6/etc/condor_config',
                'CONDOR_LOCATION': '/opt/condor/RedHat6',
                'PATH': '/opt/condor/RedHat6/bin:${PATH}'
            },
            "block": {
                "environment": {
                    'foo': 'spacey "quoted" value',
                    'bar': "this 'works' too",
                    'baz': 2
                },
                "nodes": 1,
                "walltime": "01:00:00",
                "options": {
                    "project": "cms.org.nd",
                    "condor_overrides": "",
                    "requirements": ""
                }
            }
        }
    }
    channel = SSHChannel("earth.crc.nd.edu", os.environ['USER'])

    ec, out, err = channel.execute_wait("printenv", envs=config['execution']['environment'])
    print("current env:", out)

    ec, out, err = channel.execute_wait("which condor_submit", envs=config['execution']['environment'])
    print('which condor_submit? ', out)

    provider = Condor(config=config, channel=channel)

    ids = provider.submit('''echo "sleeping"
    sleep 120
    echo "Done sleeping" ''', 1)
    time.sleep(3)

    ids += provider.submit('''echo "sleeping"
    sleep 120
    echo "Done sleeping" ''', 1)
    time.sleep(3)

    stats = provider.status(ids)
    print(stats)

    provider.cancel(ids)


if __name__ == "__main__":
    libsubmit.set_stream_logger()
    test_1()
