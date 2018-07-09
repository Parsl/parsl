import logging
import time
from libsubmit.providers.kubernetes.template import template_string

logger = logging.getLogger(__name__)

from libsubmit.error import *
from libsubmit.providers.provider_base import ExecutionProvider

try:
    from kubernetes import client, config
    config.load_kube_config()
    _kubernetes_enabled = True
except ImportError:
    _kubernetes_enabled = False


class KubernetesProvider(ExecutionProvider):
    """ Kubernetes execution provider:

        TODO: put in a config
    """

    def __repr__(self):
        return "<Kubernetes Execution Provider for site:{0}>".format(self.sitename)

    def __init__(self, config, channel=None):
        """ Initialize the Kubernetes execution provider class

        Args:
             - Config (dict): Dictionary with all the config options.

        KWargs :
             - channel (channel object) : default=None A channel object
        """

        self.channel = channel

        if not _kubernetes_enabled:
            raise OptionalModuleMissing(['kubernetes'],
                                        "Kubernetes provider requires kubernetes module and config.")

        self.kube_client = client.ExtensionsV1beta1Api()

        self.config = config
        self.sitename = self.config['site']
        self.namespace = self.config['execution']['namespace']
        self.image = self.config['execution']['image']

        self.init_blocks = self.config["execution"]["block"]["initBlocks"]
        self.min_blocks = self.config["execution"]["block"]["minBlocks"]
        self.max_blocks = self.config["execution"]["block"]["maxBlocks"]

        self.user_id = None
        self.group_id = None
        self.run_as_non_root = None
        if 'security' in self.config['execution']:
            self.user_id = self.config["execution"]['security']["user_id"]
            self.group_id = self.config["execution"]['security']["group_id"]
            self.run_as_non_root = self.config["execution"]['security']["run_as_non_root"]

        self.secret = None
        if 'secret' in self.config['execution']:
            self.secret = self.config['execution']['secret']

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

    def submit(self, cmd_string, blocksize, job_name="parsl.auto"):
        """ Submit a job

        Args:
             - cmd_string  :(String) - Name of the container to initiate
             - blocksize   :(float) - Number of replicas

        Kwargs:
             - job_name (String): Name for job, must be unique

        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job

        """
        if not self.resources:
            job_name = "{0}-{1}".format(job_name, time.time()).split(".")[0]

            self.deployment_name = '{}-{}-deployment'.format(job_name,
                                                             str(time.time()).split('.')[0])

            formatted_cmd = template_string.format(command=cmd_string,
                                                   overrides=self.config["execution"]["block"]["options"].get("overrides", ''))

            print("Creating replicas :", self.init_blocks)
            self.deployment_obj = self._create_deployment_object(job_name,
                                                                 self.image,
                                                                 self.deployment_name,
                                                                 cmd_string=formatted_cmd,
                                                                 replicas=self.init_blocks)
            logger.debug("Deployment name :{}".format(self.deployment_name))
            self._create_deployment(self.deployment_obj)
            self.resources[self.deployment_name] = {'status': 'RUNNING',
                                                    'pods': self.init_blocks}

        return self.deployment_name

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
            logger.debug("Terminating job/proc_id : {0}".format(job))
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
                                  engine_dir='.'):
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
        if 'security' in self.config['execution']:
            security_context = client.V1SecurityContext(run_as_group=self.group_id,
                                                        run_as_user=self.user_id,
                                                        run_as_non_root=self.run_as_non_root)
            #                    self.user_id = None
            #                    self.group_id = None
            #                    self.run_as_non_root = None
        # Create the enviornment variables and command to initiate IPP
        environment_vars = client.V1EnvVar(name="TEST", value="SOME DATA")

        launch_args = ["-c", "{0}; /app/deploy.sh;".format(cmd_string)]
        print(launch_args)

        # Configureate Pod template container
        container = None
        if security_context:
            container = client.V1Container(
                name=job_name,
                image=job_image,
                ports=[client.V1ContainerPort(container_port=port)],
                command=['/bin/bash'],
                args=launch_args,
                env=[environment_vars],
                security_context=security_context)
        else:
            container = client.V1Container(
                name=job_name,
                image=job_image,
                ports=[client.V1ContainerPort(container_port=port)],
                command=['/bin/bash'],
                args=launch_args,
                env=[environment_vars])
        # Create a secret to enable pulling images from secure repositories
        secret = None
        if self.secret:
            secret = client.V1LocalObjectReference(name=self.secret)

        # Create and configurate a spec section
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": job_name}),
            spec=client.V1PodSpec(containers=[container], image_pull_secrets=[secret]))

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
