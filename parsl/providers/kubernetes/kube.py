import logging
import uuid
from typing import Any, Dict, List, Optional, Tuple

import typeguard

from parsl.errors import OptionalModuleMissing
from parsl.jobs.states import JobState, JobStatus
from parsl.providers.base import ExecutionProvider
from parsl.providers.kubernetes.template import template_string
from parsl.utils import RepresentationMixin, sanitize_dns_subdomain_rfc1123

try:
    from kubernetes import client, config
    _kubernetes_enabled = True
except (ImportError, NameError, FileNotFoundError):
    _kubernetes_enabled = False

logger = logging.getLogger(__name__)

translate_table = {
    'Running': JobState.RUNNING,
    'Pending': JobState.PENDING,
    'Succeeded': JobState.COMPLETED,
    'Failed': JobState.FAILED,
    'Unknown': JobState.UNKNOWN,
}


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
    extra_requests: Dict[str, str]
        Extra resource requests of the blocks (pods). Check kubernetes docs for more details.
    extra_limits: Dict[str, str]
        Extra resource limits of the blocks (pods). Check kubernetes docs for more details.
    parallelism : float
        Ratio of provisioned task slots to active tasks. A parallelism value of 1 represents aggressive
        scaling where as many resources as possible are used; parallelism close to 0 represents
        the opposite situation in which as few resources as possible (i.e., min_blocks) are used.
    worker_init : str
        Command to be run first for the workers, such as ``python start.py``.
    secret : str
        The Kubernetes ImagePullSecret secret to use to pull images
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
    service_account_name: str
        Name of the service account to run the pod as.
    annotations: Dict[str, str]
        Annotations to set on the pod.
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
                 extra_requests: Optional[Dict[str, str]] = None,
                 extra_limits: Optional[Dict[str, str]] = None,
                 parallelism: float = 1,
                 worker_init: str = "",
                 pod_name: Optional[str] = None,
                 user_id: Optional[str] = None,
                 group_id: Optional[str] = None,
                 run_as_non_root: bool = False,
                 secret: Optional[str] = None,
                 persistent_volumes: List[Tuple[str, str]] = [],
                 service_account_name: Optional[str] = None,
                 annotations: Optional[Dict[str, str]] = None) -> None:
        if not _kubernetes_enabled:
            raise OptionalModuleMissing(['kubernetes'],
                                        "Kubernetes provider requires kubernetes module and config.")
        try:
            config.load_kube_config()
        except config.config_exception.ConfigException:
            # `load_kube_config` assumes a local kube-config file, and fails if not
            # present, raising:
            #
            #     kubernetes.config.config_exception.ConfigException: Invalid
            #     kube-config file. No configuration found.
            #
            # Since running a parsl driver script on a kubernetes cluster is a common
            # pattern to enable worker-interchange communication, this enables an
            # in-cluster config to be loaded if a kube-config file isn't found.
            #
            # Based on: https://github.com/kubernetes-client/python/issues/1005
            try:
                config.load_incluster_config()
            except config.config_exception.ConfigException:
                raise config.config_exception.ConfigException(
                    "Failed to load both kube-config file and in-cluster configuration."
                )

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
        self.extra_requests = extra_requests if extra_requests else {}
        self.extra_limits = extra_limits if extra_limits else {}
        self.parallelism = parallelism
        self.worker_init = worker_init
        self.secret = secret
        self.pod_name = pod_name
        self.user_id = user_id
        self.group_id = group_id
        self.run_as_non_root = run_as_non_root
        self.persistent_volumes = persistent_volumes
        self.service_account_name = service_account_name
        self.annotations = annotations

        self.kube_client = client.CoreV1Api()

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources: Dict[object, Dict[str, Any]]
        self.resources = {}

    def submit(self, cmd_string: str, tasks_per_node: int, job_name: str = "parsl.kube"):
        """ Submit a job
        Args:
             - cmd_string  :(String) - Name of the container to initiate
             - tasks_per_node (int) : command invocations to be launched per node

        Kwargs:
             - job_name (String): Name for job

        Returns:
             - job_id: (string) Identifier for the job
        """
        job_id = uuid.uuid4().hex[:8]

        pod_name = self.pod_name or job_name
        try:
            pod_name = sanitize_dns_subdomain_rfc1123(pod_name)
        except ValueError:
            logger.warning(
                f"Invalid pod name '{pod_name}' for job '{job_id}', falling back to 'parsl.kube'"
            )
            pod_name = "parsl.kube"
        pod_name = pod_name[:253 - 1 - len(job_id)]  # Leave room for the job ID
        pod_name = pod_name.rstrip(".-")  # Remove trailing dot or hyphen after trim
        pod_name = f"{pod_name}.{job_id}"

        formatted_cmd = template_string.format(command=cmd_string,
                                               worker_init=self.worker_init)

        logger.debug("Pod name: %s", pod_name)
        self._create_pod(image=self.image,
                         pod_name=pod_name,
                         job_id=job_id,
                         cmd_string=formatted_cmd,
                         volumes=self.persistent_volumes,
                         service_account_name=self.service_account_name,
                         annotations=self.annotations)
        self.resources[job_id] = {'status': JobStatus(JobState.RUNNING), 'pod_name': pod_name}

        return job_id

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
        if job_ids:
            self._status()
        return [self.resources[jid]['status'] for jid in job_ids]

    def _get_pod_name(self, job_id: str) -> str:
        return self.resources[job_id]['pod_name']

    def cancel(self, job_ids):
        """ Cancels the jobs specified by a list of job ids
        Args:
        job_ids : [<job_id> ...]
        Returns :
        [True/False...] : If the cancel operation fails the entire list will be False.
        """
        for job in job_ids:
            logger.debug("Terminating job/pod: {0}".format(job))
            pod_name = self._get_pod_name(job)
            self._delete_pod(pod_name)

            self.resources[job]['status'] = JobStatus(JobState.CANCELLED)
        rets = [True for i in job_ids]

        return rets

    def _status(self):
        """Returns the status list for a list of job_ids
        Args:
              self
        Returns:
              [status...] : Status list of all jobs
        """

        job_ids = list(self.resources.keys())
        to_poll_job_ids = [jid for jid in job_ids if not self.resources[jid]['status'].terminal]
        logger.debug("Polling Kubernetes pod status: {}".format(to_poll_job_ids))
        for jid in to_poll_job_ids:
            phase = None
            try:
                pod_name = self._get_pod_name(jid)
                pod = self.kube_client.read_namespaced_pod(name=pod_name, namespace=self.namespace)
            except Exception:
                logger.exception("Failed to poll pod {} status, most likely because pod was terminated".format(jid))
                if self.resources[jid]['status'] is JobStatus(JobState.RUNNING):
                    phase = 'Unknown'
            else:
                phase = pod.status.phase
            if phase:
                status = translate_table.get(phase, JobState.UNKNOWN)
                logger.debug("Updating pod {} with status {} to parsl status {}".format(jid,
                                                                                        phase,
                                                                                        status))
                self.resources[jid]['status'] = JobStatus(status)

    def _create_pod(self,
                    image: str,
                    pod_name: str,
                    job_id: str,
                    port: int = 80,
                    cmd_string=None,
                    volumes=[],
                    service_account_name=None,
                    annotations=None):
        """ Create a kubernetes pod for the job.
        Args:
              - image (string) : Docker image to launch
              - pod_name (string) : Name of the pod
              - job_id (string) : Job ID
        KWargs:
             - port (integer) : Container port
        Returns:
              - None
        """

        security_context = None
        if self.user_id and self.group_id:
            security_context = client.V1SecurityContext(run_as_group=int(self.group_id),
                                                        run_as_user=int(self.user_id),
                                                        run_as_non_root=self.run_as_non_root)

        # Create the environment variables and command to initiate IPP
        environment_vars = client.V1EnvVar(name="TEST", value="SOME DATA")

        launch_args = ["-c", "{0}".format(cmd_string)]

        volume_mounts = []
        # Create mount paths for the volumes
        for volume in volumes:
            volume_mounts.append(client.V1VolumeMount(mount_path=volume[1],
                                                      name=volume[0]))
        resources = client.V1ResourceRequirements(limits={'cpu': str(self.max_cpu),
                                                          'memory': self.max_mem} | self.extra_limits,
                                                  requests={'cpu': str(self.init_cpu),
                                                            'memory': self.init_mem} | self.extra_requests,
                                                  )
        # Configure Pod template container
        container = client.V1Container(
            name=job_id,
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
                                       labels={"parsl-job-id": job_id},
                                       annotations=annotations)
        spec = client.V1PodSpec(containers=[container],
                                image_pull_secrets=[secret],
                                volumes=volume_defs,
                                service_account_name=service_account_name)

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
