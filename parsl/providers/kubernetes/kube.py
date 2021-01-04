import logging
import time
from parsl.providers.kubernetes.template import template_string

logger = logging.getLogger(__name__)

from parsl.providers.error import OptionalModuleMissing
from parsl.providers.provider_base import ExecutionProvider, JobState, JobStatus
from parsl.utils import RepresentationMixin

import typeguard
from typing import Any, Dict, List, Optional, Tuple

try:
    from kubernetes import client, config
    _kubernetes_enabled = True
except (ImportError, NameError, FileNotFoundError):
    _kubernetes_enabled = False


class KubernetesProvider(ExecutionProvider, RepresentationMixin):
    """ Kubernetes execution provider

    Parameters
    ----------

    namespace : str
        Kubernetes namespace to create deployments.
    image : str
        Docker image to use in the deployment.
    nodes_per_block : int
        Nodes to provision per block.
    init_blocks : int
        Number of blocks to provision at the start of the run. Default is 1.
    min_blocks : int
        Minimum number of blocks to maintain.
    max_blocks : int
        Maximum number of blocks to maintain.
    max_cpu : float
        CPU limits of the blocks (pods), in cpu units.
        This is the cpu "limits" option for resource specification.
        Check kubernetes docs for more details. Default is 2.
    max_mem : str
        Memory limits of the blocks (pods), in Mi or Gi.
        This is the memory "limits" option for resource specification on kubernetes.
        Check kubernetes docs for more details. Default is 500Mi.
    init_cpu : float
        CPU limits of the blocks (pods), in cpu units.
        This is the cpu "requests" option for resource specification.
        Check kubernetes docs for more details. Default is 1.
    init_mem : str
        Memory limits of the blocks (pods), in Mi or Gi.
        This is the memory "requests" option for resource specification on kubernetes.
        Check kubernetes docs for more details. Default is 250Mi.
    parallelism : float
        Ratio of provisioned task slots to active tasks. A parallelism value of 1 represents aggressive
        scaling where as many resources as possible are used; parallelism close to 0 represents
        the opposite situation in which as few resources as possible (i.e., min_blocks) are used.
    worker_init : str
        Command to be run first for the workers, such as ``python start.py``.
    secret : str
        Docker secret to use to pull images
    pod_name : str
        The name for the pod, will be appended with a timestamp.
        Default is None, meaning parsl automatically names the pod.
    user_id : str
        Unix user id to run the container as.
    group_id : str
        Unix group id to run the container as.
    run_as_non_root : bool
        Run as non-root (True) or run as root (False).
    persistent_volumes: list[(str, str)]
        List of tuples describing persistent volumes to be mounted in the pod.
        The tuples consist of (PVC Name, Mount Directory).
    """
    @typeguard.typechecked
    def __init__(self,
                 image: str,
                 namespace: str = 'default',
                 nodes_per_block: int = 1,
                 init_blocks: int = 4,
                 min_blocks: int = 0,
                 max_blocks: int = 10,
                 max_cpu: float = 2,
                 max_mem: str = "500Mi",
                 init_cpu: float = 1,
                 init_mem: str = "250Mi",
                 parallelism: float = 1,
                 worker_init: str = "",
                 pod_name: Optional[str] = None,
                 user_id: Optional[str] = None,
                 group_id: Optional[str] = None,
                 run_as_non_root: bool = False,
                 secret: Optional[str] = None,
                 persistent_volumes: List[Tuple[str, str]] = []) -> None:
        if not _kubernetes_enabled:
            raise OptionalModuleMissing(['kubernetes'],
                                        "Kubernetes provider requires kubernetes module and config.")
        config.load_kube_config()

        self.namespace = namespace
        self.image = image
        self.nodes_per_block = nodes_per_block
        self.init_blocks = init_blocks
        self.min_blocks = min_blocks
        self.max_blocks = max_blocks
        self.max_cpu = max_cpu
        self.max_mem = max_mem
        self.init_cpu = init_cpu
        self.init_mem = init_mem
        self.parallelism = parallelism
        self.worker_init = worker_init
        self.secret = secret
        self.pod_name = pod_name
        self.user_id = user_id
        self.group_id = group_id
        self.run_as_non_root = run_as_non_root
        self.persistent_volumes = persistent_volumes

        self.kube_client = client.CoreV1Api()

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}  # type: Dict[str, Dict[str, Any]]

    def submit(self, cmd_string, tasks_per_node, job_name="parsl"):
        """ Submit a job
        Args:
             - cmd_string  :(String) - Name of the container to initiate
             - tasks_per_node (int) : command invocations to be launched per node

        Kwargs:
             - job_name (String): Name for job, must be unique
        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job
        """

        cur_timestamp = str(time.time() * 1000).split(".")[0]
        job_name = "{0}-{1}".format(job_name, cur_timestamp)

        if not self.pod_name:
            pod_name = '{}'.format(job_name)
        else:
            pod_name = '{}-{}'.format(self.pod_name,
                                      cur_timestamp)

        formatted_cmd = template_string.format(command=cmd_string,
                                               worker_init=self.worker_init)

        logger.debug("Pod name :{}".format(pod_name))
        self._create_pod(image=self.image,
                         pod_name=pod_name,
                         job_name=job_name,
                         cmd_string=formatted_cmd,
                         volumes=self.persistent_volumes)
        self.resources[pod_name] = {'status': JobStatus(JobState.RUNNING)}

        return pod_name

    def status(self, job_ids):
        """ Get the status of a list of jobs identified by the job identifiers
        returned from the submit request.
        Args:
             - job_ids (list) : A list of job identifiers
        Returns:
             - A list of JobStatus objects corresponding to each job_id in the job_ids list.
        Raises:
             - ExecutionProviderExceptions or its subclasses
        """
        self._status()
        # This is a hack
        return [JobStatus(JobState.RUNNING) for jid in job_ids]

    def cancel(self, job_ids):
        """ Cancels the jobs specified by a list of job ids
        Args:
        job_ids : [<job_id> ...]
        Returns :
        [True/False...] : If the cancel operation fails the entire list will be False.
        """
        for job in job_ids:
            logger.debug("Terminating job/proc_id: {0}".format(job))
            # Here we are assuming that for local, the job_ids are the process id's
            self._delete_pod(job)

            self.resources[job]['status'] = JobStatus(JobState.CANCELLED)
            del self.resources[job]
        rets = [True for i in job_ids]

        return rets

    def _status(self):
        """ Internal: Do not call. Returns the status list for a list of job_ids
        Args:
              self
        Returns:
              [status...] : Status list of all jobs
        """

        jobs_ids = list(self.resources.keys())
        # TODO: fix this
        return jobs_ids
        # do something to get the deployment's status

    def _create_pod(self,
                    image,
                    pod_name,
                    job_name,
                    port=80,
                    cmd_string=None,
                    volumes=[]):
        """ Create a kubernetes pod for the job.
        Args:
              - image (string) : Docker image to launch
              - pod_name (string) : Name of the pod
              - job_name (string) : App label
        KWargs:
             - port (integer) : Container port
        Returns:
              - None
        """

        security_context = None
        if self.user_id and self.group_id:
            security_context = client.V1SecurityContext(run_as_group=self.group_id,
                                                        run_as_user=self.user_id,
                                                        run_as_non_root=self.run_as_non_root)

        # Create the enviornment variables and command to initiate IPP
        environment_vars = client.V1EnvVar(name="TEST", value="SOME DATA")

        launch_args = ["-c", "{0};".format(cmd_string)]

        volume_mounts = []
        # Create mount paths for the volumes
        for volume in volumes:
            volume_mounts.append(client.V1VolumeMount(mount_path=volume[1],
                                                      name=volume[0]))
        resources = client.V1ResourceRequirements(limits={'cpu': str(self.max_cpu),
                                                          'memory': self.max_mem},
                                                  requests={'cpu': str(self.init_cpu),
                                                            'memory': self.init_mem}
                                                  )
        # Configure Pod template container
        container = client.V1Container(
            name=pod_name,
            image=image,
            resources=resources,
            ports=[client.V1ContainerPort(container_port=port)],
            volume_mounts=volume_mounts,
            command=['/bin/bash'],
            args=launch_args,
            env=[environment_vars],
            security_context=security_context)

        # Create a secret to enable pulling images from secure repositories
        secret = None
        if self.secret:
            secret = client.V1LocalObjectReference(name=self.secret)

        # Create list of volumes from (pvc, mount) tuples
        volume_defs = []
        for volume in volumes:
            volume_defs.append(client.V1Volume(name=volume[0],
                                               persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                                   claim_name=volume[0])))

        metadata = client.V1ObjectMeta(name=pod_name,
                                       labels={"app": job_name})
        spec = client.V1PodSpec(containers=[container],
                                image_pull_secrets=[secret],
                                volumes=volume_defs
                                )

        pod = client.V1Pod(spec=spec, metadata=metadata)
        api_response = self.kube_client.create_namespaced_pod(namespace=self.namespace,
                                                              body=pod)
        logger.debug("Pod created. status='{0}'".format(str(api_response.status)))

    def _delete_pod(self, pod_name):
        """Delete a pod"""

        api_response = self.kube_client.delete_namespaced_pod(name=pod_name,
                                                              namespace=self.namespace,
                                                              body=client.V1DeleteOptions())
        logger.debug("Pod deleted. status='{0}'".format(str(api_response.status)))

    @property
    def label(self):
        return "kubernetes"

    @property
    def status_polling_interval(self):
        return 60
