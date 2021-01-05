import json
import logging
import os
import time
from string import Template

from parsl.dataflow.error import ConfigurationError
from parsl.providers.azure.template import template_string
from parsl.providers.provider_base import ExecutionProvider, JobState, JobStatus
from parsl.providers.error import OptionalModuleMissing
from parsl.utils import RepresentationMixin
from parsl.launchers import SingleNodeLauncher

logger = logging.getLogger(__name__)

try:
    from azure.common.credentials import ServicePrincipalCredentials
    from azure.mgmt.resource import ResourceManagementClient
    from azure.mgmt.network import NetworkManagementClient
    from azure.mgmt.compute import ComputeManagementClient
    from azure.mgmt.compute.models import DiskCreateOption
    from msrestazure.azure_exceptions import CloudError

    _api_enabled = True

except ImportError:
    _api_enabled = False
else:
    _api_enabled = True

translate_table = {
    'VM pending': JobState.PENDING,
    'VM running': JobState.RUNNING,
    'VM deallocated': JobState.COMPLETED,
    'VM stopping': JobState.COMPLETED,  # We shouldn't really see this state
    'VM stopped': JobState.COMPLETED,  # We shouldn't really see this state
}


class AzureProvider(ExecutionProvider, RepresentationMixin):
    """
    A Provider for using Microsoft Azure Resources

    One of 2 methods are required to authenticate: `key_file`, or environment
    variables. If `key_file` is not set, the following environment
    variables must be set: `AZURE_CLIENT_ID` (the access key for
    your azure account),
    `AZURE_CLIENT_SECRET` (the secret key for your azure account),
    `AZURE_TENANT_ID` (the session key for your azure account), and
    `AZURE_SUBSCRIPTION_ID`.

    The tenant ID is also known as the directory ID here:
    in https://portal.azure.com/#blade/Microsoft_AAD_IAM/ActiveDirectoryMenuBlade/Properties
    A tenant ID is a GUID.

    A client ID and secret can be created by following these instructions:
    https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal
    A client ID is a GUID.

    The subscription ID can be found here:
    https://portal.azure.com/#blade/Microsoft_Azure_Billing/SubscriptionsBlade

    Parameters
    ----------
    vm_reference : dict
        Dictionary describing the parameters of the Azure VM.

        Required structure:
        {
          'publisher': VM OS publisher
          'offer': VM OS offer
          'sku': VM OS SKU
          'version': VM OS version
          'vm_size': VM Size, analogous to instance type for AWS
          'disk_size_gb': (int) size of VM disk in gb
          'admin_username': (str) admin username of VM instances,
          'password': (str) admin password for VM instances
        }

        VM Publisher, author, SKU, and Version can be found in the Azure marketplace.
        One way to do so is to use the CLI, as described
        [here](https://docs.microsoft.com/en-us/azure/virtual-machines/linux/cli-ps-findimage)

        Another way is to look in the marketplace's GUI,
        [here](https://azuremarketplace.microsoft.com/en-us/marketplace/apps?filters=virtual-machine-images%3Blinux&page=1).

    worker_init : str
        String to append to the Userdata script executed in the cloudinit phase of
        instance initialization.
    key_file : str
        Path to JSON file that contains 'Azure keys'
        The structure of the key file is as follows:
        {
            "AZURE_CLIENT_ID": (str) azure client id [from account principal],
            "AZURE_CLIENT_SECRET":(str) azure client secret [from account principal],
            "AZURE_TENANT_ID": (str) azure tenant [account] id,
            "AZURE_SUBSCRIPTION_ID": (str) azure subscription id
        }
    init_blocks : int
        Number of blocks to provision at the start of the run. Default is 1.
    min_blocks : int
        Minimum number of blocks to maintain. Default is 0.
    max_blocks : int
        Maximum number of blocks to maintain. Default is 10.
    region : str
        Azure region to launch machines. Default is 'westus'.
    key_name : str
        Name of the Azure private key (.pem file) that is usually generated on the console
        to allow SSH access to the Azure instances. This is mostly used for debugging.
    launcher : Launcher
        Launcher for this provider. Possible launchers include
        :class:`~parsl.launchers.SingleNodeLauncher` (the default),
        :class:`~parsl.launchers.SrunLauncher`, or
        :class:`~parsl.launchers.AprunLauncher`
    linger : Bool
        When set to True, the workers will not `halt`. The user is responsible for shutting
        down the nodes.
    """

    def __init__(self,
                 vm_reference,
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=10,
                 parallelism=1,
                 worker_init='',
                 location='westus',
                 group_name='parsl.group',
                 key_name=None,
                 key_file=None,
                 vnet_name="parsl.vnet",
                 linger=False,
                 launcher=SingleNodeLauncher()):
        if not _api_enabled:
            raise OptionalModuleMissing(
                ['azure', 'msrestazure'], "Azure Provider requires the azure module.")

        self._label = 'azure'
        self.init_blocks = init_blocks
        self.min_blocks = min_blocks
        self.max_blocks = max_blocks
        self.max_nodes = max_blocks
        self.parallelism = parallelism
        self.nodes_per_block = 1

        self.worker_init = worker_init
        self.vm_reference = vm_reference
        self.region = location
        self.vnet_name = vnet_name

        self.key_name = key_name
        self.key_file = key_file
        self.location = location
        self.group_name = group_name

        self.launcher = launcher
        self.linger = linger
        self.resources = {}
        self.instances = []

        env_specified = os.getenv("AZURE_CLIENT_ID") is not None and os.getenv(
            "AZURE_CLIENT_SECRET") is not None and os.getenv(
            "AZURE_TENANT_ID") is not None and os.getenv("AZURE_SUBSCRIPTION_ID") is not None

        if key_file is None and not env_specified:
            raise ConfigurationError(("Must specify either: 'key_file', or "
                                      "`AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, "
                                      "and `AZURE_TENANT_ID` environment variables."))

        if key_file is None:
            self.clientid = os.getenv("AZURE_CLIENT_ID")
            self.clientsecret = os.getenv("AZURE_CLIENT_SECRET")
            self.tenantid = os.getenv("AZURE_TENANT_ID")
            self.subid = os.getenv("AZURE_SUBSCRIPTION_ID")
        else:
            with open(key_file) as fh:
                keys = json.load(fh)
                self.clientid = keys.get("AZURE_CLIENT_ID")
                self.clientsecret = keys.get("AZURE_CLIENT_SECRET")
                self.tenantid = keys.get("AZURE_TENANT_ID")
                self.subid = keys.get("AZURE_SUBSCRIPTION_ID")

        self.get_clients()

    def get_clients(self):
        """
        Set up access to Azure API clients
        """
        credentials, subscription_id = self.get_credentials()
        self.resource_client = ResourceManagementClient(
            credentials, subscription_id)
        self.compute_client = ComputeManagementClient(credentials,
                                                      subscription_id)
        self.network_client = NetworkManagementClient(credentials,
                                                      subscription_id)

    def get_credentials(self):
        """
        Authenticate to the Azure API
        """
        subscription_id = self.subid
        credentials = ServicePrincipalCredentials(
            client_id=self.clientid,
            secret=self.clientsecret,
            tenant=self.tenantid)
        return credentials, subscription_id

    def submit(self,
               command='sleep 1',
               tasks_per_node=1,
               job_name="parsl.azure"):
        """
        Submit the command onto a freshly instantiated Azure VM
        Submit returns an ID that corresponds to the task that was just submitted.
        Parameters
        ----------
        command : str
            Command to be invoked on the remote side.
        tasks_per_node : int (default=1)
            Number of command invocations to be launched per node
        job_name : str
            Prefix for the job name.
        Returns
        -------
        None or str
            If at capacity, None will be returned. Otherwise, the job identifier will be returned.
        """
        self.resource_client.resource_groups.create_or_update(
            self.group_name, {'location': self.location})
        self.resources["group"] = self.group_name

        logger.info('Creating NIC')
        nic = self.create_nic(self.network_client)

        wrapped_cmd = self.launcher(command, tasks_per_node, 1)

        cmd_str = Template(template_string).substitute(jobname=job_name,
                                                       user_script=wrapped_cmd,
                                                       linger=str(self.linger).lower(),
                                                       worker_init=self.worker_init)

        logger.info('Creating Linux Virtual Machine')
        vm_parameters = self.create_vm_parameters(nic.id,
                                                  self.vm_reference)

        # Uniqueness strategy from AWS provider
        job_name = "{0}-parsl-azure".format(str(time.time()).replace(".", ""))

        async_vm_creation = self.compute_client.\
            virtual_machines.create_or_update(
                self.vnet_name, job_name, vm_parameters)

        vm_info = async_vm_creation.result()
        self.instances.append(vm_info.name)

        try:

            logger.debug("Started instance_id: {0}".format(vm_info.id))
            disk, d_name = self.create_disk()

            self.resources[vm_info.id] = {
                "job_id": vm_info.id,
                "instance": vm_info,
                "status": JobStatus(JobState.PENDING)
            }

            vm_info.storage_profile.data_disks.append({
                'lun':
                12,
                'name':
                d_name,
                'create_option':
                DiskCreateOption.attach,
                'managed_disk': {
                    'id': disk.id
                }
            })
            async_disk_attach = self.\
                compute_client.virtual_machines.create_or_update(
                    self.group_name, vm_info.name, vm_info)
            async_disk_attach.wait()

            async_vm_start = self.compute_client.virtual_machines.start(
                self.group_name, job_name)
            async_vm_start.wait()

            logger.debug("attempting to connect instance to Parsl master")
            run_command_parameters = {
                                        'command_id': 'RunShellScript',
                                        'script': cmd_str.split("\n")
                                    }
            self.compute_client.virtual_machines.run_command(
                                            self.group_name,
                                            vm_info.name,
                                            run_command_parameters)
        except KeyboardInterrupt:
            self.cancel([vm_info.name])
            raise KeyboardInterrupt

        return vm_info.name

    def status(self, job_ids):
        """Get the status of a list of jobs identified by their ids.
        Parameters
        ----------
        job_ids : list of str
            Identifiers for the jobs.
        Returns
        -------
        list of int
            The status codes of the requsted jobs.
        """
        statuses = []
        logger.info('List VMs in resource group')
        for job_id in job_ids:
            try:
                vm = self.compute_client.virtual_machines.get(
                    self.group_name, job_id, expand='instanceView')
                status = vm.instance_view.statuses[1].display_status
                statuses.append(JobStatus(translate_table.get(status, JobState.UNKNOWN)))
            # This only happens when it is in ProvisionState/Pending
            except IndexError:
                statuses.append(JobStatus(JobState.PENDING))
        return statuses

    def cancel(self, job_ids):
        """Cancel the jobs specified by a list of job ids.
        Parameters
        ----------
        job_ids : list of str
            List of of job identifiers
        Returns
        -------
        list of bool
            Each entry in the list will contain False if the operation fails. Otherwise, the entry will be True.
        """

        return_vals = []

        if self.linger:
            logger.debug("Ignoring cancel requests due to linger mode")
            return [False for x in job_ids]

        for job_id in job_ids:
            try:
                logger.debug('Delete VM {}'.format(job_id))
                async_vm_delete = self.compute_client.virtual_machines.delete(
                    self.group_name, job_id)
                async_vm_delete.wait()
                self.instances.remove(job_id)
                return_vals.append(True)
            except Exception:
                return_vals.append(False)

        return return_vals

    @property
    def label(self):
        return self._label

    def create_nic(self, network_client):
        """Create (or update, if it exists already) a Network Interface for a VM.

            Also ensures that there's a virtual network available.

            We create a VPC with CIDR 10.0.0.0/16, which provides up to 64,000 instances.

            We attach a subnet for each availability zone within the region specified in the
            config. We give each subnet an ip range like 10.0.X.0/20, which is large enough
            for approx. 4000 instances.


        """
        try:
            logger.info('Creating (or updating) Vnet')
            async_vnet_creation = self.network_client.virtual_networks.\
                create_or_update(
                    self.group_name, self.vnet_name, {
                        'location': self.location,
                        'address_space': {
                            'address_prefixes': ['10.0.0.0/16']
                        }
                    })
            vnet_info = async_vnet_creation.result()
            self.resources["vnet"] = vnet_info

        except CloudError as e:

            if "InUse" in str(e):
                logger.info('Found Existing Vnet. Proceeding.')
            else:
                raise e

        if not self.resources.get("subnets", None):
            self.resources["subnets"] = {}

        try:
            logger.info('Creating (or updating) Subnet')
            async_subnet_creation = self.network_client.subnets.create_or_update(
                self.group_name, self.vnet_name, "{}.subnet".format(
                    self.group_name), {'address_prefix': '10.0.0.0/20'})
            subnet_info = async_subnet_creation.result()

            self.resources["subnets"][subnet_info.id] = subnet_info

        except CloudError as e:
            if "InUse" in str(e):
                subnet_info = self.network_client.subnets.get(self.group_name, self.vnet_name, "{}.subnet".format(
                        self.group_name))
                self.resources["subnets"][subnet_info.id] = subnet_info
                logger.info('Found Existing Subnet. Proceeding.')
            else:
                raise e

        logger.info('Creating (or updating) NIC')
        async_nic_creation = self.network_client.network_interfaces.\
            create_or_update(
                self.group_name,
                "{}.{}.nic".format(self.group_name, time.time()), {
                    'location':
                    self.location,
                    'ip_configurations': [{
                        'name':
                        "{}.ip.config".format(self.group_name),
                        'subnet': {
                            'id': subnet_info.id
                        }
                    }]
                })

        nic_info = async_nic_creation.result()

        if not self.resources.get("nics", None):
            self.resources["nics"] = {}

        self.resources["nics"][nic_info.id] = nic_info

        return nic_info

    def create_vm_parameters(self, nic_id, vm_reference):
        """Create the VM parameters structure.
        """
        return {
            'location': self.region,
            'os_profile': {
                'computer_name': "{}.{}".format(self.vnet_name, time.time()),
                'admin_username': self.vm_reference["admin_username"],
                'admin_password': self.vm_reference["password"]
            },
            'hardware_profile': {
                'vm_size': vm_reference["vm_size"]
            },
            'storage_profile': {
                'image_reference': {
                    'publisher': vm_reference['publisher'],
                    'offer': vm_reference['offer'],
                    'sku': vm_reference['sku'],
                    'version': vm_reference['version']
                },
            },
            'network_profile': {
                'network_interfaces': [{
                    'id': nic_id,
                }]
            }
        }

    def create_disk(self):
        """Create a managed data disk of size specified in config.

        Each instance gets one disk"""
        logger.info('Create (empty) managed Data Disk')
        name = '{}.{}'.format(self.group_name, time.time())
        async_disk_creation = self.compute_client.disks.create_or_update(
            self.group_name, name, {
                'location': self.location,
                'disk_size_gb': self.vm_reference["disk_size_gb"],
                'creation_data': {
                    'create_option': DiskCreateOption.empty
                }
            })
        data_disk = async_disk_creation.result()
        return data_disk, name

    @property
    def status_polling_interval(self):
        return 60
