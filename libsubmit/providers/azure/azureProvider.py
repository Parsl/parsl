import logging
import os
import pprint
import time

from libsubmit.error import *
from libsubmit.providers.provider_base import ExecutionProvider

try:
    from azure.common.credentials import UserPassCredentials
    from libsubmit.azure.azureDeployer import Deployer

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


class AzureProvider(ExecutionProvider):
    '''
    Here's a sample config for the Azure provider:

    .. code-block:: python

         { "auth" : {
              "profile"    : #{Description: Specify the profile to be used from the standard Azure config file
                             # ~/.azure/config.
                             # Type : String,
                             # Expected : "default", # Use the 'default' aws profile
                             # Required : False},

            },

           "execution" : { # Definition of all execution aspects of a site

              "executor"   : #{Description: Define the executor used as task executor,
                             # Type : String,
                             # Expected : "ipp",
                             # Required : True},

              "provider"   : #{Description : The provider name, in this case ec2
                             # Type : String,
                             # Expected : "aws",
                             # Required :  True },

              "block" : { # Definition of a block

                  "nodes"      : #{Description : # of nodes to provision per block
                                 # Type : Integer,
                                 # Default: 1},

                  "taskBlocks" : #{Description : # of workers to launch per block
                                 # as either an number or as a bash expression.
                                 # for eg, "1" , "$(($CORES / 2))"
                                 # Type : String,
                                 #  Default: "1" },

                  "walltime"  :  #{Description : Walltime requested per block in HH:MM:SS
                                 # Type : String,
                                 # Default : "00:20:00" },

                  "initBlocks" : #{Description : # of blocks to provision at the start of
                                 # the DFK
                                 # Type : Integer
                                 # Default : ?
                                 # Required :    },

                  "minBlocks" :  #{Description : Minimum # of blocks outstanding at any time
                                 # WARNING :: Not Implemented
                                 # Type : Integer
                                 # Default : 0 },

                  "maxBlocks" :  #{Description : Maximum # Of blocks outstanding at any time
                                 # WARNING :: Not Implemented
                                 # Type : Integer
                                 # Default : ? },

                  "options"   : {  # Scheduler specific options


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
    '''

    def __init__(self, config: dict, channel=None):
        """INITIALIZE AZURE PROVIDER. USES AZURE PYTHON SDK TO PROVIDE EXECUTION RESOURCES
            ARGS:
             - :parm config (dict): Dictionary with all the config options.

            KWargs:
             - :param channel (None): A channel is not required for Azure.


        """
        self.config = self.read_configs(config)
        self.config_logger()

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
        self.sitename = config['site']
        self.current_blocksize = 0
        self.resources = {}
        self.instances = []

        self.config = config
        options = self.config["execution"]["block"]["options"]
        logger.warn("Options %s", options)
        self.instance_type = options.get("azure_template_file", "template.json")
        self.image_id = options["imageId"]
        self.key_name = options["keyName"]
        self.region = options.get("region", 'us-east-2')
        self.max_nodes = (
            self.config["execution"]["block"].get("maxBlocks", 1) * self.config["execution"]["block"].get("nodes", 1))

        try:
            self.initialize_boto_client()
        except Exception as e:
            logger.error("Site:[{0}] Failed to initialize".format(self))
            raise e

        try:
            self.statefile = self.config["execution"]["block"]["options"].get("stateFile", '.ec2site_{0}.json'.format(
                self.sitename))
            self.read_state_file(self.statefile)

        except Exception as e:
            self.create_vpc().id
            logger.info("No State File. Cannot load previous options. Creating new infrastructure")
            self.write_state_file()

    def __repr__(self) -> str:
        return "<Azure Execution Provider for site:{0}>".format(self.sitename)

    @property
    def channels_required(self):
        ''' No channel required for Azure
        '''
        return False

    def config_logger(self):
        """Configure Logger
        """
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

    def pretty_configs(self, configs):
        """prettyprint config"""
        printer = pprint.PrettyPrinter(indent=4)
        printer.pprint(configs)

    def ipyparallel_configuration(self):
        config = ''
        try:
            with open(os.path.expanduser(self.config['iPyParallelConfigFile'])) as f:
                config = f.read().strip()
        except Exception as e:
            self.logger.error(e)
            self.logger.info("Couldn't find user iPyParallel config file. Trying default location.")
            with open(os.path.expanduser("~/.ipython/profile_parallel/security/ipcontroller-engine.json")) as f:
                config = f.read().strip()
        else:
            self.logger.error("Cannot find iPyParallel config file. Cannot proceed.")
            return -1
        ipptemplate = """
cat <<EOF> ipengine.json
{}
EOF

mkdir -p '.ipengine_logs'
sleep 5
ipengine --file=ipengine.json &> .ipengine_logs/ipengine.log""".format(config)
        return ipptemplate

    def submit(self, cmd_string='sleep 1', blocksize=1, job_name="parsl.auto"):
        '''Submits the cmd_string onto a freshly instantiated AWS EC2 instance.
        Submit returns an ID that corresponds to the task that was just submitted.

        Args:
             - cmd_string (str): Commandline invocation to be made on the remote side.
             - blocksize (int) : Number of blocks requested

        Kwargs:
             - job_name (String): Prefix for job name

        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job

        '''

        job_name = "parsl.auto.{0}".format(time.time())
        [instance, *rest] = self.deployer.deploy(cmd_string=cmd_string, job_name=job_name, blocksize=1)

        if not instance:
            logger.error("Failed to submit request to Azure")
            return None

        logger.debug("Started instance_id : {0}".format(instance.instance_id))

        state = translate_table.get(instance.state['Name'], "PENDING")

        self.resources[instance.instance_id] = {"job_id": instance.instance_id, "instance": instance, "status": state}

        return instance.instance_id

    def status(self):
        '''  Get the status of a list of jobs identified by their ids.

        Args:
            - job_ids (List of ids) : List of identifiers for the jobs

        Returns:
            - List of status codes.
        '''
        states = []
        statuses = self.deployer.get_vm_status([self.resources.get(job_id) for job_id in job_ids])
        for status in statuses:
            states.append(translate_table.get(status.state['Name'], "PENDING"))
        return states

    def cancel(self, job_ids):
        ''' Cancels the jobs specified by a list of job ids

        Args:
             job_ids (list) : List of of job identifiers

        Returns :
             [True/False...] : If the cancel operation fails the entire list will be False.
        TODO: Make this change statuses
        '''
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
        ''' Returns the current blocksize.
        This may need to return more information in the futures :
        { minsize, maxsize, current_requested }
        '''
        return len(self.instances)


if __name__ == '__main__':
    config = open("azureconf.json")
