from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import KubernetesProvider
from parsl.addresses import address_by_route


config = Config(
    executors=[
        HighThroughputExecutor(
            label='kube-htex',
            cores_per_worker=1,
            max_workers=1,
            worker_logdir_root='runinfo',
            address=address_by_route(),  # Address for the pod worker to connect back
            provider=KubernetesProvider(
                namespace="default",
                image='CONTAINER_LOCATION',  # Specify where to download the image
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=10,  # Maximum number of pods to scale up
                worker_init="""pip install parsl""",  # install Parsl when the pod starts
                secret="kube-secret",  # The secret key to download the image
                pod_name='test-pod',  # Should follow the Kubernetes naming rules
            ),
        ),
    ]
)
