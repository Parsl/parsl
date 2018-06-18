import libsubmit
from libsubmit import SSHChannel, Slurm
import time


def test_1():

    slurm_config = {
        "site": "Cori/Nersc",
        "execution": {
            "executor": "ipp",
            "provider": "slurm",
            "channel": "local",
            "options": {
                "init_parallelism": 1,
                "max_parallelism": 1,
                "min_parallelism": 0,
                "tasks_per_node": 1,
                "node_granularity": 1,
                "partition": "debug",
                "walltime": "00:25:00",
                "submit_script_dir": ".scripts",
                "overrides": '''#SBATCH --constraint=haswell'''
            }
        }
    }

    channel = SSHChannel(
        "cori.nersc.gov",
        "yadunand",
        channel_script_dir="/global/homes/y/yadunand/parsl_scripts")
    ec, out, err = channel.execute_wait("which sbatch; echo $HOSTNAME; pwd")
    print("Stdout: ", out)

    provider = Slurm(config=slurm_config, channel=channel)

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


if __name__ == "__main__":
    libsubmit.set_stream_logger()
    test_1()
