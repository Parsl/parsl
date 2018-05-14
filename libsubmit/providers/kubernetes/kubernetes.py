import json
import logging
import os
import time

logger = logging.getLogger(__name__)

from libsubmit.error import *
from libsubmit.providers.provider_base import ExecutionProvider

try:
    from kubernetes import client, config
    config.load_kube_config()
    _kubernetes_enabled = True
except ImportError:
    _kubernetes_enabled = False


 class Kubernetes(ExecutionProvider):
#class Kubernetes():
    ''' Kubernetes execution provider:

        TODO: put in a config
    '''

    def __repr__(self):
        return "<Kubernetes Execution Provider for site:{0}>".format(self.sitename)

    def __init__(self, config, channel=None):
        ''' Initialize the Kubernetes execution provider class

        Args:
             - Config (dict): Dictionary with all the config options.

        KWargs :
             - channel (channel object) : default=None A channel object
        '''

        self.channel = channel

        if not _kubernetes_enabled:
            raise OptionalModuleMissing(['kubernetes'], 
                "Kubernetes provider requires kubernetes module and config.")

        self.kube_client = client.ExtensionsV1beta1Api()

        self.config = config
        self.sitename = self.config['site']
        self.namespace = self.config['execution']['namespace']
        self.image = self.config['execution']['image']
        self.blocksize = self.config['execution']['block']['initParallelism']
        self.secret = None
        if 'secret' in self.config['execution']:
            self.secret = self.config['execution']['secret']

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

        job_name = "{0}-{1}".format("parsl-auto", time.time()).split(".")[0]

        job_image = self.image
        deployment_name = '{0}-deployment'.format(job_name)

        deployment = self._create_deployment_object(job_name, job_image, deployment_name, replicas=self.blocksize)

        self._create_deployment(deployment)

        self.resources[deployment_name] = {'status': 'RUNNING'}


    def submit(self, cmd_string, blocksize, job_name="parsl.auto"):
        ''' Submit 

        Args:
             - cmd_string  :(String) - Name of the container to initiate
             - blocksize   :(float) - Number of replicas

        Kwargs:
             - job_name (String): Name for job, must be unique

        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job

        '''
        pass

    def status(self, job_ids):
        ''' Get the status of a list of jobs identified by the job identifiers
        returned from the submit request.

        Args:
             - job_ids (list) : A list of job identifiers

        Returns:
             - A list of status from ['PENDING', 'RUNNING', 'CANCELLED', 'COMPLETED',
               'FAILED', 'TIMEOUT'] corresponding to each job_id in the job_ids list.

        Raises:
             - ExecutionProviderExceptions or its subclasses

        '''
        self._status()
        return [self.resources[jid]['status'] for jid in job_ids]

    def cancel(self, job_ids):
        ''' Cancels the jobs specified by a list of job ids

        Args:
        job_ids : [<job_id> ...]

        Returns :
        [True/False...] : If the cancel operation fails the entire list will be False.
        '''

        for job in job_ids:
            logger.debug("Terminating job/proc_id : {0}".format(job))
            # Here we are assuming that for local, the job_ids are the process id's
            _delete_deployment(job)

            self.resources[job]['status'] = 'CANCELLED'
        rets = [True for i in job_ids]

        return rets
    
    def _status(self):
        ''' Internal: Do not call. Returns the status list for a list of job_ids

        Args:
              self

        Returns:
              [status...] : Status list of all jobs
        '''

        jobs_ids = list(self.resources.keys())

        # do something to get the deployment's status


    def _create_deployment_object(self, job_name, job_image, 
                                  deployment_name, port=80, 
                                  replicas=1, 
                                  engine_json_file='~/.ipython/profile_default/security/ipcontroller-engine.json',
                                  engine_dir='.'):
        ''' Create a kubernetes deployment for the job.

        Args:
              - job_name (string) : Name of the job and deployment
              - job_image (string) : Docker image to launch

        KWargs:
             - port (integer) : Container port
             - replicas : Number of replica containers to maintain

        Returns:
              - True: The deployment object to launch
        '''

        ipp_launch_cmd = self._compose_launch_cmd(engine_json_file, engine_dir)

        # Create the enviornment variables and command to initiate IPP
        environment_vars = client.V1EnvVar(name="TEST", value="SOME DATA")

        launch_args = ["-c", "{0}; /app/deploy.sh;".format(ipp_launch_cmd)]
        print (launch_args)
        # Configureate Pod template container
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
        spec = client.ExtensionsV1beta1DeploymentSpec(
            replicas=replicas,
            template=template)

        # Instantiate the deployment object
        deployment = client.ExtensionsV1beta1Deployment(
            api_version="extensions/v1beta1",
            kind="Deployment",
            metadata=client.V1ObjectMeta(name=deployment_name),
            spec=spec)

        return deployment

    def _create_deployment(self, deployment):
        ''' Create the kubernetes deployment


        '''

        api_response = self.kube_client.create_namespaced_deployment(
            body=deployment,
            namespace=self.namespace)

        logger.debug("Deployment created. status='{0}'".format(str(api_response.status)))


    def _delete_deployment(self, deployment_name):
        ''' Delete deployment

        '''

        api_response = self.kube_client.delete_namespaced_deployment(
            name=deployment_name,
            namespace=self.namespace,
            body=client.V1DeleteOptions(
                propagation_policy='Foreground',
                grace_period_seconds=5))

        logger.debug("Deployment deleted. status='{0}'".format(str(api_response.status)))

    @property
    def scaling_enabled(self):
        return False

    @property
    def channels_required(self):
        return False



    def _compose_launch_cmd(self, filepath, engine_dir):
        """Reads the json contents from filepath and uses that to compose the engine launch command.

        Args:
            filepath: Path to the engine file
            engine_dir : CWD for the engines .

        """
        self.engine_file = os.path.expanduser(filepath)

        engine_json = None
        try:
            with open(self.engine_file, 'r') as f:
                engine_json = f.read()

        except OSError as e:
            logger.error("Could not open engine_json : ", self.engine_file)
            raise e

        return """cd {0}
cat <<EOF > ipengine.json
{1}
EOF

mkdir -p '.ipengine_logs'
ipengine --file=ipengine.json &>> .ipengine_logs/$JOBNAME.log
""".format(engine_dir, engine_json)

if __name__ == "__main__":
    # print("None")

    config = {
        "site": "OSG",
        "execution": {
            "executor": "ipp",
            "provider": "kubernetes",
            "namespace": "development",
            "image": "039706667969.dkr.ecr.us-east-1.amazonaws.com/hellocontainer2",
            "secret": "ryan-kube-secret",            
            "block": {
                "initParallelism": 1,
                "maxParallelism": 1,
                "minParallelism": 1
            }
        }
    }

    p = Kubernetes(config)
    p._status()
    p.submit("echo 'Hello World'", 1)
    p._status()