import atexit
import logging
import os
from parsl.launchers import SingleNodeLauncher
from parsl.providers.provider_base import JobState, JobStatus

logger = logging.getLogger(__name__)

try:
    import googleapiclient.discovery

except ImportError:
    _google_enabled = False
else:
    _google_enabled = True

translate_table = {
    'PENDING': JobState.PENDING,
    'PROVISIONING': JobState.PENDING,
    "STAGING": JobState.PENDING,
    'RUNNING': JobState.RUNNING,
    'DONE': JobState.COMPLETED,
    'STOPPING': JobState.COMPLETED,
    'STOPPED': JobState.COMPLETED,
    'TERMINATED': JobState.COMPLETED,
    'SUSPENDING': JobState.COMPLETED,
    'SUSPENDED': JobState.COMPLETED,
}


class GoogleCloudProvider():
    """A provider for using resources from the Google Compute Engine.

    Parameters
    ----------
    project_id : str
        Project ID from Google compute engine.
    key_file : str
        Path to authorization private key json file. This is required for auth.
        A new one can be generated here: https://console.cloud.google.com/apis/credentials
    region : str
        Region in which to start instances
    os_project : str
        OS project code for Google compute engine.
    os_family : str
        OS family to request.
    google_version : str
        Google compute engine version to use. Possibilies include 'v1' (default) or 'beta'.
    instance_type: str
        'n1-standard-1',
    init_blocks : int
        Number of blocks to provision immediately. Default is 1.
    min_blocks : int
        Minimum number of blocks to maintain. Default is 0.
    max_blocks : int
        Maximum number of blocks to maintain. Default is 10.
    parallelism : float
        Ratio of provisioned task slots to active tasks. A parallelism value of 1 represents aggressive
        scaling where as many resources as possible are used; parallelism close to 0 represents
        the opposite situation in which as few resources as possible (i.e., min_blocks) are used.

    .. code:: python

                                +------------------
                                |
          script_string ------->|  submit
               id      <--------|---+
                                |
          [ ids ]       ------->|  status
          [statuses]   <--------|----+
                                |
          [ ids ]       ------->|  cancel
          [cancel]     <--------|----+
                                |
                                +-------------------
     """

    def __init__(self,
                 project_id,
                 key_file,
                 region,
                 os_project,
                 os_family,
                 google_version='v1',
                 instance_type='n1-standard-1',
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=10,
                 launcher=SingleNodeLauncher(),
                 parallelism=1):
        self.project_id = project_id
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file
        self.zone = self.get_zone(region)
        self.os_project = os_project
        self.os_family = os_family
        self.label = 'google_cloud'
        self.client = googleapiclient.discovery.build('compute', google_version)
        self.instance_type = instance_type
        self.init_blocks = init_blocks
        self.min_blocks = min_blocks
        self.max_blocks = max_blocks
        self.parallelism = parallelism
        self.num_instances = 0
        self.launcher = launcher

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}
        self.provisioned_blocks = 0
        atexit.register(self.bye)

    def submit(self, command, tasks_per_node, job_name="parsl.gcs"):
        ''' The submit method takes the command string to be executed upon
        instantiation of a resource most often to start a pilot.

        Args :
             - command (str) : The bash command string to be executed.
             - tasks_per_node (int) : command invocations to be launched per node

        KWargs:
             - job_name (str) : Human friendly name to be assigned to the job request

        Returns:
             - A job identifier, this could be an integer, string etc

        Raises:
             - ExecutionProviderException or its subclasses
        '''
        wrapped_cmd = self.launcher(command,
                                    tasks_per_node,
                                    1)

        instance, name = self.create_instance(command=wrapped_cmd)
        self.provisioned_blocks += 1
        self.resources[name] = {"job_id": name, "status": JobStatus(translate_table[instance['status']])}
        return name

    def status(self, job_ids):
        ''' Get the status of a list of jobs identified by the job identifiers
        returned from the submit request.

        Args:
             - job_ids (list) : A list of job identifiers

        Returns:
             - A list of JobStatus objects corresponding to each job_id in the job_ids list.

        Raises:
             - ExecutionProviderException or its subclasses

        '''
        statuses = []
        for job_id in job_ids:
            instance = self.client.instances().get(instance=job_id, project=self.project_id, zone=self.zone).execute()
            self.resources[job_id]['status'] = JobStatus(translate_table[instance['status']])
            statuses.append(translate_table[instance['status']])
        return statuses

    def cancel(self, job_ids):
        ''' Cancels the resources identified by the job_ids provided by the user.

        Args:
             - job_ids (list): A list of job identifiers

        Returns:
             - A list of status from cancelling the job which can be True, False

        Raises:
             - ExecutionProviderException or its subclasses
        '''
        statuses = []
        for job_id in job_ids:
            try:
                self.delete_instance(job_id)
                statuses.append(True)
                self.provisioned_blocks -= 1
            except Exception:
                statuses.append(False)
        return statuses

    @property
    def current_capacity(self):
        """Returns the number of currently provisioned blocks."""
        return self.provisioned_blocks

    def bye(self):
        self.cancel([i for i in list(self.resources)])

    def create_instance(self, command=""):
        name = "parslauto{}".format(self.num_instances)
        self.num_instances += 1
        compute = self.client
        project = self.project_id
        image_response = compute.images().getFromFamily(
            project=self.os_project, family=self.os_family).execute()
        source_disk_image = image_response['selfLink']

        # Configure the machine
        machine_type = "zones/{}/machineTypes/{}".format(self.zone, self.instance_type)
        startup_script = command

        config = {
            'name': name,
            'machineType': machine_type,

            # Specify the boot disk and the image to use as a source.
            'disks': [{
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                }
            }],
            'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [{
                    'type': 'ONE_TO_ONE_NAT',
                    'name': 'External NAT'
                }]
            }],
            'serviceAccounts': [{
                'email':
                'default',
                'scopes': [
                    'https://www.googleapis.com/auth/devstorage.read_write',
                    'https://www.googleapis.com/auth/logging.write'
                ]
            }],
            'metadata': {
                'items': [{
                    # Startup script is automatically executed by the
                    # instance upon startup.
                    'key': 'startup-script',
                    'value': startup_script
                }]
            }
        }

        return compute.instances().insert(project=project, zone=self.zone, body=config).execute(), name

    def get_zone(self, region):
        res = self.client.zones().list(project=self.project_id).execute()
        for zone in res['items']:
            if region in zone['name'] and zone['status'] == "UP":
                return zone["name"]

    def delete_instance(self, name):

        compute = self.client
        project = self.project_id
        zone = self.zone

        return compute.instances().delete(project=project, zone=zone, instance=name).execute()

    @property
    def status_polling_interval(self):
        return 60
