import os
import pprint
import json
import time
import logging
import atexit
from libsubmit.execution_provider_base import ExecutionProvider
from libsubmit.error import *

try :
    from azure.common.credentials import UserPassCredentials
    from libsubmit.azure.azureDeployer import Deployer

except ImportError :
    _azure_enabled = False
else:
    _azure_enabled = True

translate_table = {'PD': 'PENDING',
                   'R': 'RUNNING',
                   'CA': 'CANCELLED',
                   'CF': 'PENDING',  # (configuring),
                   'CG': 'RUNNING',  # (completing),
                   'CD': 'COMPLETED',
                   'F': 'FAILED',  # (failed),
                   'TO': 'TIMEOUT',  # (timeout),
                   'NF': 'FAILED',  # (node failure),
                   'RV': 'FAILED',  # (revoked) and
                   'SE': 'FAILED'}  # (special exit state

template_string = """
cd ~
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip ipython
sudo pip3 install ipyparallel parsl
"""


class AzureProvider(ExecutionProvider):
    def __init__(self, config):
        """Initialize Azure provider. Uses Azure python sdk to provide execution resources"""
        self.config = self.read_configs(config)
        self.config_logger()

        if not _azure_enabled :
            raise OptionalModuleMissing(['azure', 'haikunator'], "Azure Provider requires the azure and haikunator modules.")

        credentials = UserPassCredentials(
            self.config['username'], self.config['pass'])
        subscription_id = self.config['subscriptionId']

        # self.resource_client = ResourceManagementClient(credentials, subscription_id)
        # self.storage_client = StorageManagementClient(credentials, subscription_id)

        self.resource_group_name = 'my_resource_group'
        self.deployer = Deployer(
            subscription_id,
            self.resource_group_name,
            self.read_configs(config))

    def config_logger(self):
        """Configure Logger"""
        logger = logging.getLogger("AzureProvider")
        logger.setLevel(logging.INFO)
        if not os.path.isfile(self.config['logFile']):
            with open(self.config['logFile'], 'w') as temp_log:
                temp_log.write("Creating new log file.\n")
        fh = logging.FileHandler(self.config['logFile'])
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        self.logger = logger

    def _read_conf(self, config_file):
        """read config file"""
        config = json.load(open(config_file, 'r'))
        return config

    def pretty_configs(self, configs):
        """prettyprint config"""
        printer = pprint.PrettyPrinter(indent=4)
        printer.pprint(configs)

    def read_configs(self, config_file):
        """Read config file"""
        config = self._read_conf(config_file)
        return config

    def ipyparallel_configuration(self):
        config = ''
        try:
            with open(os.path.expanduser(self.config['iPyParallelConfigFile'])) as f:
                config = f.read().strip()
        except Exception as e:
            self.logger.error(e)
            self.logger.info(
                "Couldn't find user ipyparallel config file. Trying default location.")
            with open(os.path.expanduser("~/.ipython/profile_parallel/security/ipcontroller-engine.json")) as f:
                config = f.read().strip()
        else:
            self.logger.error(
                "Cannot find iPyParallel config file. Cannot proceed.")
            return -1
        ipptemplate = """
cat <<EOF> ipengine.json
{}
EOF

mkdir -p '.ipengine_logs'
sleep 5
ipengine --file=ipengine.json &> .ipengine_logs/ipengine.log""".format(config)
        return ipptemplate

    def submit(self):
        """Uses AzureDeployer to spin up an instance and connect it to the ipyparallel controller"""
        self.deployer.deploy()

    def status(self):
        """Get status of azure VM. Not implemented yet."""
        raise NotImplemented

    def cancel(self):
        """Destroy an azure VM"""
        self.deployer.destroy()

    def scale_in(self):
        raise NotImplemented

    def scale_out(self):
        raise NotImplemented


if __name__ == '__main__':
    config = "azureconf.json"
    provider = AzureProvider(config)
    provider.submit()
