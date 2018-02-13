import os
import pprint
import json
import time
import logging
import atexit
from datetime import datetime, timedelta
from libsubmit.providers.provider_base import ExecutionProvider
from libsubmit.launchers import Launchers
from libsubmit.error import *

logger = logging.getLogger(__name__)

try:
    import googleapiclient.discovery
    from google.auth import compute_engine

except ImportError:
    _google_enabled = False
else:
    _google_enabled = True

translate_table = {'PENDING': 'PENDING',
                   'PROVISIONING': 'PENDING',
                   "STAGING": "PENDING",
                   'RUNNING': 'RUNNING',
                   'DONE': 'COMPLETED',
                   'STOPPING': 'COMPLETED',
                   'STOPPED': 'COMPLETED',
                   'TERMINATED': 'COMPLETED',
                   'SUSPENDING': 'COMPLETED',
                   'SUSPENDED': 'COMPLETED',
                   }


class GoogleCloud():  # ExcecutionProvider):
    """ Define the Google Cloud provider

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
          [True/False] <--------|  scaling_enabled
                                |
                                +-------------------
     """

    def __init__(self, config, channel=None):
        ''' Initialize the GoogleCompute class

        Args:
             - Config (dict): Dictionary with all the config options.

        KWargs:
             - Channel (None): A channel is not required for google cloud.
             
        Google compute instances require a few specific configuration options:
            - auth['keyfile'](string): Path to authorization private key json file. 
                                       This is required for auth. A new one can be 
                                       generated here: 
                                       https://console.cloud.google.com/apis/credentials
            - options['projectID'](string): Project ID from google compute engine
            - options['region'](string): Region in which to start instances
            - options['instanceType'](string): google instance type. Default:"n1-standard-1"
            - options['osProject'](string): OS project code for google compute engine
            - options['osFamily'](string): OS family to request
            - options['googleVersion'](string): Google compute engine version to use ('v1' or 'beta')
        '''
        self.config = config
        self.sitename = config['site']
        self.options = self.config["execution"]["block"]["options"]
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.config["auth"]["keyfile"]
        version = self.options.get('googleVersion', 'v1')
        self.client = googleapiclient.discovery.build('compute', version)
        self.channel = None
        self.project_id = self.config["execution"]["block"]["options"]["projectID"]
        self.zone = self.get_correct_zone(
            self.config["execution"]["block"]["options"]["region"])
        launcher_name = self.config["execution"]["block"].get(
            "launcher", "singleNode")
        self.launcher = Launchers.get(launcher_name, None)
        self.scriptDir = self.config["execution"].get("scriptDir", ".scripts")
        self.name_int = 0
        if not os.path.exists(self.scriptDir):
            os.makedirs(self.scriptDir)

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}
        self.current_blocksize = 0
        atexit.register(self.bye)

    def __repr__(self):
        return "<Google Cloud Platform Execution Provider for site:{0}>".format(
            self.sitename, self.channel)

    def submit(self, cmd_string="", blocksize=1, job_name="parsl.auto"):
        ''' The submit method takes the command string to be executed upon
        instantiation of a resource most often to start a pilot.

        Args :
             - cmd_string (str) : The bash command string to be executed.
             - blocksize (int) : Blocksize to be requested

        KWargs:
             - job_name (str) : Human friendly name to be assigned to the job request

        Returns:
             - A job identifier, this could be an integer, string etc

        Raises:
             - ExecutionProviderExceptions or its subclasses
        '''
        instance, name = self.create_instance(cmd_string=cmd_string)
        self.current_blocksize += 1
        self.resources[name] = {
            "job_id": name, "status": translate_table[instance['status']]}
        return name

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
        statuses = []
        for job_id in job_ids:
            instance = self.client.instances().get(
                instance=job_id, project=self.project_id, zone=self.zone).execute()
            self.resources[job_id][
                'status'] = translate_table[instance['status']]
            statuses.append(translate_table[instance['status']])
        return statuses

    def cancel(self, job_ids):
        ''' Cancels the resources identified by the job_ids provided by the user.

        Args:
             - job_ids (list): A list of job identifiers

        Returns:
             - A list of status from cancelling the job which can be True, False

        Raises:
             - ExecutionProviderExceptions or its subclasses
        '''
        statuses = []
        for job_id in job_ids:
            try:
                self.delete_instance(job_id)
                statuses.append(True)
                self.current_blocksize -= 1
            except Exception as e:
                statuses.append(False)
        return statuses

    @property
    def scaling_enabled(self):
        ''' Scaling is enabled

        Returns:
              - Status (Bool)
        '''
        return True

    @property
    def current_capacity(self):
        ''' Returns the current blocksize.
        This may need to return more information in the futures :
        { minsize, maxsize, current_requested }
        '''
        return self.current_blocksize

    @property
    def channels_required(self):
        '''Google Compute does not require a channel

        Returns:
              - Status (Bool)
        '''
        return False

    def bye(self):
        self.cancel([i for i in list(self.resources)])

    def create_instance(self, cmd_string=""):
        name = "parslauto{}".format(self.name_int)
        self.name_int += 1
        compute = self.client
        project = self.project_id
        zone = self.zone
        image_response = compute.images().getFromFamily(
            project=self.options["osProject"], family=self.options["osFamily"]).execute()
        source_disk_image = image_response['selfLink']

        # Configure the machine
        machine_type = "zones/{}/machineTypes/{}".format(
            zone, self.options.get("instanceType", "n1-standard-1"))
        startup_script = cmd_string

        config = {
            'name': name,
            'machineType': machine_type,

            # Specify the boot disk and the image to use as a source.
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': source_disk_image,
                    }
                }
            ],

   
            'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [
                    {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                ]
            }],

          
            'serviceAccounts': [{
                'email': 'default',
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

        return compute.instances().insert(
            project=project,
            zone=zone,
            body=config).execute(), name

    def get_correct_zone(self, region):
        res = self.client.zones().list(project=self.project_id).execute()
        for zone in res['items']:
            if region in zone['name'] and zone['status'] == "UP":
                return zone["name"]

    def delete_instance(self, name):

        compute = self.client
        project = self.project_id
        zone = self.zone

        return compute.instances().delete(
            project=project,
            zone=zone,
            instance=name).execute()
