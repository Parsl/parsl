from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import KubernetesProvider


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="executorname",
                storage_access=[],
                worker_debug=True,
                cores_per_worker=1,
                encrypted=False,  # needs certificate fs to be mounted in same place...
                provider=KubernetesProvider(worker_init=". /venv/bin/activate",
                                            image="parsl:ci",
                                            max_mem="2048Gi"
                                            # was getting OOM-killing of workers with default... this helps
                                            ),
            )
        ],
        strategy='none',
    )
