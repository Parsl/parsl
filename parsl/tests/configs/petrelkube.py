import os

from parsl.addresses import address_by_route
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import KubernetesProvider

from .user_opts import user_opts


def fresh_config():
    worker_init_file = user_opts['petrelkube']['worker_init']
    with open(os.path.expanduser(worker_init_file)) as f:
        lines = f.readlines()
    worker_init = ';'.join([line.strip() for line in lines])

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='kube-htex',
                cores_per_worker=1,
                max_workers_per_node=1,
                worker_logdir_root='.',

                # Address for the pod worker to connect back
                address=address_by_route(),
                encrypted=True,
                provider=KubernetesProvider(
                    namespace="dlhub-privileged",

                    # Docker image url to use for pods
                    image='python:3.7',

                    # Command to be run upon pod start, such as:
                    # 'module load Anaconda; source activate parsl_env'.
                    # or 'pip install parsl'
                    worker_init=worker_init,

                    # The secret key to download the image
                    secret="ryan-kube-secret",

                    # Should follow the Kubernetes naming rules
                    pod_name='parsl-site-test-pod',

                    nodes_per_block=1,
                    init_blocks=1,
                    # Maximum number of pods to scale up
                    max_blocks=2,
                ),
            ),
        ]
    )

    return config
