from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import KubernetesProvider


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="executorname",
                worker_debug=True,
                cores_per_worker=1,
                encrypted=False,  # needs certificate fs to be mounted in same place...
                provider=KubernetesProvider(
                    worker_init=". /venv/bin/activate",
                    # pod_name="override-pod-name", # can't use default name because of dots, without own bugfix
                    image="parslimg:a"
                    ),
            )
        ],
        strategy='none',
    )
