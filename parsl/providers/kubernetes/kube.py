import logging
import time
from parsl.providers.kubernetes.template import template_string

logger = logging.getLogger(__name__)

from parsl.providers.error import *
from parsl.providers.provider_base import ExecutionProvider
from parsl.utils import RepresentationMixin

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
    channel : Channel
        Channel for accessing this provider. Possible channels include
        :class:`~parsl.channels.LocalChannel` (the default),
        :class:`~parsl.channels.SSHChannel`, or
        :class:`~parsl.channels.SSHInteractiveLoginChannel`.
    nodes_per_block : int
        Nodes to provision per block.
    init_blocks : int
        Number of blocks to provision at the start of the run. Default is 1.
    min_blocks : int
        Minimum number of blocks to maintain.
    max_blocks : int
        Maximum number of blocks to maintain.
    parallelism : float
        Ratio of provisioned task slots to active tasks. A parallelism value of 1 represents aggressive
        scaling where as many resources as possible are used; parallelism close to 0 represents
        the opposite situation in which as few resources as possible (i.e., min_blocks) are used.
    worker_init : str
        Command to be run first for the workers, such as `python start.py`.
    secret : str
        Docker secret to use to pull images
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

    def __init__(self,
                 image,
                 namespace='default',
                 channel=None,
                 nodes_per_block=1,
                 init_blocks=4,
                 min_blocks=0,
                 max_blocks=10,
                 parallelism=1,
                 worker_init="",
                 deployment_name=None,
                 user_id=None,
                 group_id=None,
                 run_as_non_root=False,
                 secret=None,
                 persistent_volumes=[]):
        if not _kubernetes_enabled:
            raise OptionalModuleMissing(['kubernetes'],
                                        "Kubernetes provider requires kubernetes module and config.")
        config.load_kube_config()

        self.namespace = namespace
        self.image = image
        self.channel = channel
        self.nodes_per_block = nodes_per_block
        self.init_blocks = init_blocks
        self.min_blocks = min_blocks
        self.max_blocks = max_blocks
        self.parallelism = parallelism
        self.worker_init = worker_init
        self.secret = secret
        self.deployment_name = deployment_name
        self.user_id = user_id
        self.group_id = group_id
        self.run_as_non_root = run_as_non_root
        self.persistent_volumes = persistent_volumes

        self.kube_client = client.ExtensionsV1beta1Api()

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

    def submit(self, cmd_string, blocksize, tasks_per_node, job_name="parsl"):
        """ Submit a job
        Args:
             - cmd_string  :(String) - Name of the container to initiate
             - blocksize   :(float) - Number of replicas
             - tasks_per_node (int) : command invocations to be launched per node

        Kwargs:
             - job_name (String): Name for job, must be unique
        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job
        """
        if not self.resources:
            cur_timestamp = str(time.time() * 1000).split(".")[0]
            job_name = "{0}-{1}".format(job_name, cur_timestamp)

            if not self.deployment_name:
                deployment_name = '{}-deployment'.format(job_name)
            else:
                deployment_name = '{}-{}-deployment'.format(self.deployment_name,
                                                            cur_timestamp)

            formatted_cmd = template_string.format(command=cmd_string,
                                                   worker_init=self.worker_init)

            self.deployment_obj = self._create_deployment_object(job_name,
                                                                 self.image,
                                                                 deployment_name,
                                                                 cmd_string=formatted_cmd,
                                                                 replicas=self.init_blocks,
                                                                 volumes=self.persistent_volumes)
            logger.debug("Deployment name :{}".format(deployment_name))
            self._create_deployment(self.deployment_obj)
            self.resources[deployment_name] = {'status': 'RUNNING',
                                               'pods': self.init_blocks}

        return deployment_name

    def status(self, job_ids):
        """ Get the status of a list of jobs identified by the job identifiers
        returned from the submit request.
        Args:
             - job_ids (list) : A list of job identifiers
        Returns:
             - A list of status from ['PENDING', 'RUNNING', 'CANCELLED', 'COMPLETED',
               'FAILED', 'TIMEOUT'] corresponding to each job_id in the job_ids list.
        Raises:
             - ExecutionProviderExceptions or its subclasses
        """
        self._status()
        # This is a hack
        return ['RUNNING' for jid in job_ids]

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
            self._delete_deployment(job)

            self.resources[job]['status'] = 'CANCELLED'
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

    def _create_deployment_object(self, job_name, job_image,
                                  deployment_name, port=80,
                                  replicas=1,
                                  cmd_string=None,
                                  engine_json_file='~/.ipython/profile_default/security/ipcontroller-engine.json',
                                  engine_dir='.',
                                  volumes=[]):
        """ Create a kubernetes deployment for the job.
        Args:
              - job_name (string) : Name of the job and deployment
              - job_image (string) : Docker image to launch
        KWargs:
             - port (integer) : Container port
             - replicas : Number of replica containers to maintain
        Returns:
              - True: The deployment object to launch
        """

        # sorry, quick hack that doesn't pass this stuff through to test it works.
        # TODO it also doesn't only add what is set :(
        security_context = None
        if self.user_id and self.group_id:
            security_context = client.V1SecurityContext(run_as_group=self.group_id,
                                                        run_as_user=self.user_id,
                                                        run_as_non_root=self.run_as_non_root)

        # Create the enviornment variables and command to initiate IPP
        environment_vars = client.V1EnvVar(name="TEST", value="SOME DATA")

        launch_args = ["-c", "{0}; /app/deploy.sh;".format(cmd_string)]

        volume_mounts = []
        # Create mount paths for the volumes
        for volume in volumes:
            volume_mounts.append(client.V1VolumeMount(mount_path=volume[1],
                                                      name=volume[0]))
        # Configureate Pod template container
        container = None
        if security_context:
            container = client.V1Container(
                name=job_name,
                image=job_image,
                ports=[client.V1ContainerPort(container_port=port)],
                volume_mounts=volume_mounts,
                command=['/bin/bash'],
                args=launch_args,
                env=[environment_vars],
                security_context=security_context)
        else:
            container = client.V1Container(
                name=job_name,
                image=job_image,
                ports=[client.V1ContainerPort(container_port=port)],
                volume_mounts=volume_mounts,
                command=['/bin/bash'],
                args=launch_args,
                env=[environment_vars])
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

        # Create and configurate a spec section
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": job_name}),
            spec=client.V1PodSpec(containers=[container],
                                  image_pull_secrets=[secret],
                                  volumes=volume_defs
                                  ))

        # Create the specification of deployment
        spec = client.ExtensionsV1beta1DeploymentSpec(replicas=replicas,
                                                      template=template)

        # Instantiate the deployment object
        deployment = client.ExtensionsV1beta1Deployment(
            api_version="extensions/v1beta1",
            kind="Deployment",
            metadata=client.V1ObjectMeta(name=deployment_name),
            spec=spec)

        return deployment

    def _create_deployment(self, deployment):
        """ Create the kubernetes deployment """

        api_response = self.kube_client.create_namespaced_deployment(
            body=deployment,
            namespace=self.namespace)

        logger.debug("Deployment created. status='{0}'".format(str(api_response.status)))

    def _delete_deployment(self, deployment_name):
        """ Delete deployment """

        api_response = self.kube_client.delete_namespaced_deployment(
            name=deployment_name,
            namespace=self.namespace,
            body=client.V1DeleteOptions(
                propagation_policy='Foreground',
                grace_period_seconds=5))

        logger.debug("Deployment deleted. status='{0}'".format(
            str(api_response.status)))

    @property
    def scaling_enabled(self):
        return False

    @property
    def channels_required(self):
        return False

    @property
    def label(self):
        return "kubernetes"
