from libsubmit.providers import CobaltProvider
from libsubmit.launchers import SimpleLauncher

from parsl.config import Config
from parsl.executors.mpix import MPIExecutor

IP = 'PUBLIC_IP',    # Please replace PUBLIC_IP with your public ip
JOB_URL = "tcp://{}:50005".format(IP)
RESULT_URL = "tcp://{}:50006".format(IP)

config = Config(
    executors=[
        MPIExecutor(
            label="local_ipp",
            jobs_q_url=JOB_URL,
            results_q_url=RESULT_URL,
            launch_cmd='which python3; \
aprun -b -cc depth -j 1 -n $(($COBALT_PARTSIZE * {tasks_per_node})) -N {tasks_per_node} \
python3 /home/yadunand/parsl/parsl/executors/mpix/fabric.py -d --task_url={task_url} --result_url={result_url}',
            provider=CobaltProvider(
                queue="debug-flat-quad",
                # queue="default",
                launcher=SimpleLauncher(),
                walltime="00:30:00",
                nodes_per_block=2,
                tasks_per_node=32,
                init_blocks=1,
                max_blocks=1,
                worker_init="""module load intelpython35/2017.0.035
source activate parsl_intel_py3.5
module swap cray-mpich cray-mpich-abi
export LD_LIBRARY_PATH=${CRAY_LD_LIBRARY_PATH}:$LD_LIBRARY_PATH

ulimit -c unlimited

export OMP_NUM_THREADS=16""",
                account='CSC249ADCD01',    # Please replace CSC249ADCD01 with your ALCF allocation
                cmd_timeout=60
            ),
        )
    ],
    strategy=None,
)
