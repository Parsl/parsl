from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.launchers import SimpleLauncher
from parsl.providers import KubernetesProvider

from parsl.executors.taskvine import TaskVineExecutor, TaskVineManagerConfig

def fresh_config():
    return Config(executors=[TaskVineExecutor(manager_config=TaskVineManagerConfig(port=9000),
                                              worker_launch_method='provider',
                provider=KubernetesProvider(
                    worker_init=". /venv/bin/activate",
                    # pod_name="override-pod-name", # can't use default name because of dots, without own bugfix
                    image="parsl:ci",
                    max_mem="2048Gi"  # was getting OOM-killing of workers with default... maybe this will help.
                    ),

                )])
