import logging
import os
import time

from libsubmit.error import *
from libsubmit.providers.provider_base import ExecutionProvider
from libsubmit.utils import RepresentationMixin

try:
    from azure.common.credentials import UserPassCredentials
    from libsubmit.azure.azure_deployer import Deployer

except ImportError:
    _azure_enabled = False
else:
    _azure_enabled = True

translate_table = {
    'PD': 'PENDING',
    'R': 'RUNNING',
    'CA': 'CANCELLED',
    'CF': 'PENDING',  # (configuring),
    'CG': 'RUNNING',  # (completing),
    'CD': 'COMPLETED',
    'F': 'FAILED',  # (failed),
    'TO': 'TIMEOUT',  # (timeout),
    'NF': 'FAILED',  # (node failure),
    'RV': 'FAILED',  # (revoked) and
    'SE': 'FAILED'
}  # (special exit state

template_string = """
cd ~
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip ipython
sudo pip3 install ipyparallel parsl
"""


class AzureProvider(ExecutionProvider, RepresentationMixin):
    """A provider for using Azure resources.

    Parameters
    ----------
    profile : str
        Profile to be used if different from the standard Azure config file ~/.azure/config.
    nodes_per_block : int
        Nodes to provision per block. Default is 1.
    template_file_location : str
        Location of template file for Azure instance. Default is 'templates/template.json'.
                  "walltime"  :  #{Description : Walltime requested per block in HH:MM:SS
                                 # Type : String,
                                 # Default : "00:20:00" },
                  "initBlocks" : #{Description : # of blocks to provision at the start of
                                 # the DFK
                                 # Type : Integer
                                 # Default : ?
                                 # Required :    },


                      "templateFileLocation" : #{Description : location of template file for azure instance
                                       # Type : String,
                                       # Required : False
                                       # Default : templates/template.json },

                      "region"       : #{"Description : AWS region to launch machines in
                                       # in the submit script to the scheduler
                                       # Type : String,
                                       # Default : 'us-east-2',
                                       # Required : False },

                      "keyName"      : #{"Description : Name of the azure private key (.pem file)
                                       # that is usually generated on the console to allow ssh access
                                       # to the EC2 instances, mostly for debugging.
                                       # in the submit script to the scheduler
                                       # Type : String,
                                       # Required : True },

                  }
              }
            }
         }
    """

    def __init__(self,
                 image_id,
                 key_name,
                 label='azure',
                 region='us-east-2',
                 azure_template_file='template.json',
                 max_blocks=1,
                 nodes_per_block=1,
                 state_file=None):
        self.configure_logger()

        if not _azure_enabled:
            raise OptionalModuleMissing(['azure'], "Azure Provider requires the azure module.")

        credentials = UserPassCredentials(self.config['username'], self.config['pass'])
        subscription_id = self.config['subscriptionId']

        self.resource_client = ResourceManagementClient(credentials, subscription_id)
        self.storage_client = StorageManagementClient(credentials, subscription_id)

        self.resource_group_name = 'my_resource_group'
        self.deployer = Deployer(subscription_id, self.resource_group_name, self.read_configs(config))

        self.channel = channel
        self.config = config
        self.provisioned_blocks = 0
        self.resources = {}
        self.instances = []

        self.instance_type = azure_template_file
        self.image_id = image_id
        self.key_name = key_name
        self.region = region
        self.max_nodes = max_blocks * nodes_per_block

        try:
            self.initialize_boto_client()
        except Exception as e:
            logger.error("Azure '{}' failed to initialize.".format(self.label))
            raise e

        try:
            if state_file is None:
                state_file = '.azure_{}.json'.format(self.label)
            self.read_state_file(state_file)

        except Exception as e:
            self.create_vpc().id
            logger.info("No State File. Cannot load previous options. Creating new infrastructure.")
            self.write_state_file()

    def configure_logger(self):
        """Configure logger."""
        logger = logging.getLogger("AzureProvider")
        logger.setLevel(logging.INFO)
        if not os.path.isfile(self.config['logFile']):
            with open(self.config['logFile'], 'w') as temp_log:
                temp_log.write("Creating new log file.\n")
        fh = logging.FileHandler(self.config['logFile'])
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        self.logger = logger

    def submit(self, command='sleep 1', blocksize=1, job_name="parsl.auto"):
        """Submit command to an Azure instance.

        Submit returns an ID that corresponds to the task that was just submitted.

        Parameters
        ----------
        command : str
            Command to be invoked on the remote side.
        blocksize : int
            Number of blocks requested.
        job_name : str
             Prefix for job name.

        Returns
        -------
        None or str
            If at capacity (no more can be provisioned), None is returned. Otherwise,
            an identifier for the job is returned.
        """

        job_name = "parsl.auto.{0}".format(time.time())
        [instance, *rest] = self.deployer.deploy(command=command, job_name=job_name, blocksize=1)

        if not instance:
            logger.error("Failed to submit request to Azure")
            return None

        logger.debug("Started instance_id : {0}".format(instance.instance_id))

        state = translate_table.get(instance.state['Name'], "PENDING")

        self.resources[instance.instance_id] = {"job_id": instance.instance_id, "instance": instance, "status": state}

        return instance.instance_id

    def status(self, job_ids):
        """Get the status of a list of jobs identified by their ids.

        Parameters
        ----------
        job_ids : list of str
            Identifiers for the jobs.

        Returns
        -------
        list of int
            Status codes for each requested job.
        """
        states = []
        statuses = self.deployer.get_vm_status([self.resources.get(job_id) for job_id in job_ids])
        for status in statuses:
            states.append(translate_table.get(status.state['Name'], "PENDING"))
        return states

    def cancel(self, job_ids):
        """ Cancels the jobs specified by a list of job ids

        Parameters
        ----------
        list of str
            List of identifiers of jobs which should be canceled.

        Returns
        -------
        list of bool
            For each entry, True if the cancel operation is successful, otherwise False.
        """
        for job_id in job_ids:
            try:
                self.deployer.destroy(self.resources.get(job_id))
                return True
            except e:
                logger.error("Failed to cancel {}".format(repr(job_id)))
                logger.error(e)
                return False

    @property
    def scaling_enabled():
        return True

    @property
    def current_capacity(self):
        """Returns the current blocksize."""
        return len(self.instances)


if __name__ == '__main__':
    config = open("azureconf.json")
