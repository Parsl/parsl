from parsl.config import Config
from parsl.providers import CondorProvider
from parsl.executors import HighThroughputExecutor

config = Config(
    executors=[
        HighThroughputExecutor(
            label='OSG_HTEX',
            max_workers=1,
            provider=CondorProvider(
                nodes_per_block=1,
                init_blocks=4,
                max_blocks=4,
                # This scheduler option string ensures that the compute nodes provisioned
                # will have modules
                scheduler_options="""
                +ProjectName = "MyProject"
                Requirements = HAS_MODULES=?=TRUE
                """,
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init='''unset HOME; unset PYTHONPATH; module load python/3.7.0;
python3 -m venv parsl_env; source parsl_env/bin/activate; python3 -m pip install parsl''',
                walltime="00:20:00",
            ),
            worker_logdir_root='$OSG_WN_TMP',
            worker_ports=(31000, 31001)
        )
    ]
)
