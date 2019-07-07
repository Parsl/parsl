import json
import logging
import os
import time
from string import Template
import base64

from parsl.dataflow.error import ConfigurationError
from template import template_string
from parsl.providers.provider_base import ExecutionProvider
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

    _api_enabled = True

except ImportError:
    _api_enabled = False
else:
    _api_enabled = True

translate_table = {
    'VM pending': 'PENDING',
    'VM running': 'RUNNING',
    'VM deallocated': 'COMPLETED',
    'VM stopping': 'COMPLETED',  # We shouldn't really see this state
    'VM stopped': 'COMPLETED',  # We shouldn't really see this state
}


class AzureProvider(ExecutionProvider, RepresentationMixin):
    """
    A Provider for using Microsoft Azure Resources

    One of 2 methods are required to authenticate: keyfile, or environment
    variables. If  keyfile is not set, the following environment
    variables must be set: `AZURE_CLIENT_ID` (the access key for
    your azure account),
    `AZURE_CLIENT_SECRET` (the secret key for your azure account), the
    `AZURE_TENANT_ID` (the session key for your azure account), and
    AZURE_SUBSCRIPTION_ID.

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
          "admin_username": (str) admin username of VM instances,
          "password": (str) admin password for VM instances
        }
    worker_init : str
        String to append to the Userdata script executed in the cloudinit phase of
        instance initialization.
    walltime : str
        Walltime requested per block in HH:MM:SS.
    key_file : str
        Path to json file that contains 'Azure keys'
    nodes_per_block : int
        This is always 1 for Azure. Nodes to provision per block.
    nodes_per_block : int
        Nodes to provision per block. Default is 1.
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
    spot_max_bid : float
        Maximum bid price (if requesting spot market machines).
    iam_instance_profile_arn : str
        Launch instance with a specific role.
    walltime : str
        Walltime requested per block in HH:MM:SS. This option is not currently honored by this provider.
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
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=10,
                 nodes_per_block=1,
                 parallelism=1,
                 worker_init='',
                 instance_type_ref=None,
                 location='westus',
                 group_name='parsl.auto',
                 key_name=None,
                 key_file=None,
                 profile=None,
                 vnet_name="parsl.auto",
                 state_file=None,
                 walltime="01:00:00",
                 linger=False,
                 launcher=SingleNodeLauncher()):
        if not _api_enabled:
            raise OptionalModuleMissing(
                ['azure'], "Azure Provider requires the azure module.")

        self._label = 'azure'
        self.init_blocks = init_blocks
        self.min_blocks = min_blocks
        self.max_blocks = max_blocks
        self.nodes_per_block = nodes_per_block
        self.max_nodes = max_blocks * nodes_per_block
        self.parallelism = parallelism

        self.worker_init = worker_init
        self.vm_reference = instance_type_ref
        self.vm_disk_size = self.vm_reference["disk_size_gb"]
        self.region = location
        self.vnet_name = vnet_name

        self.key_name = key_name
        self.key_file = key_file
        self.location = location
        self.group_name = group_name

        self.walltime = walltime
        self.launcher = launcher
        self.linger = linger
        self.resources = {}
        self.instances = []

        env_specified = os.getenv("AZURE_CLIENT_ID") is not None and os.getenv(
            "AZURE_CLIENT_SECRET") is not None

        if key_file is None and not env_specified:
            raise ConfigurationError("Must specify either, 'key_file', or\
                 `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`,\
                  and `AZURE_TENANT_ID` environment variables.")

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

        One of 2 methods are required to authenticate: keyfile, or environment
        variables. If  keyfile is not set, the following environment
        variables must be set: `AZURE_CLIENT_ID` (the access key for
        your azure account),
        `AZURE_CLIENT_SECRET` (the secret key for your azure account), the
        `AZURE_TENANT_ID` (the session key for your azure account), and
        AZURE_SUBSCRIPTION_ID.

        """
        subscription_id = self.subid
        credentials = ServicePrincipalCredentials(
            client_id=self.clientid,
            secret=self.clientsecret,
            tenant=self.tenantid)
        return credentials, subscription_id

    def submit(self,
               command='sleep 1',
               blocksize=1,
               tasks_per_node=1,
               job_name="parsl.auto"):
        """
        Submit the command onto a freshly instantiated Azure VM
        Submit returns an ID that corresponds to the task that was just submitted.
        Parameters
        ----------
        command : str
            Command to be invoked on the remote side.
        blocksize : int
            Number of blocks requested.
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

        logger.info('\nCreating NIC')
        nic = self.create_nic(self.network_client)

        wrapped_cmd = self.launcher(command, tasks_per_node,
                                    self.nodes_per_block)

        cmd_str = Template(template_string).substitute(jobname=job_name,
                                                       user_script=wrapped_cmd,
                                                       linger=str(self.linger).lower(),
                                                       worker_init=self.worker_init)

        logger.info('\nCreating Linux Virtual Machine')
        vm_parameters = self.create_vm_parameters(nic.id,
                                                  self.vm_reference,
                                                  cmd_str)

        # Uniqueness strategy from AWS provider
        job_name = "parsl.auto.{0}".format(time.time())

        async_vm_creation = self.compute_client.\
            virtual_machines.create_or_update(
                self.vnet_name, job_name, vm_parameters)

        vm_info = async_vm_creation.result()
        self.instances.append(vm_info.name)

        disk, d_name = self.create_disk()

        logger.debug("Started instance_id: {0}".format(vm_info.id))

        # state = translate_table.get(instance.state['Name'], "PENDING")

        self.resources[vm_info.id] = {
            "job_id": vm_info.id,
            "instance": vm_info,
            "status": "Test State"
        }

        virtual_machine = async_vm_creation.result()

        virtual_machine.storage_profile.data_disks.append({
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
                self.group_name, virtual_machine.name, virtual_machine)
        async_disk_attach.wait()

        async_vm_start = self.compute_client.virtual_machines.start(
            self.group_name, job_name)
        async_vm_start.wait()

        return virtual_machine.name

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
        logger.info('\nList VMs in resource group')
        for job_id in job_ids:
            try:
                vm = self.compute_client.virtual_machines.get(
                    self.group_name, job_id, expand='instanceView')
                status = vm.instance_view.statuses[1].display_status
                statuses.append(translate_table.get(status, "UNKNOWN"))
            # This only happens when it is in ProvisionState/Pending
            except IndexError:
                statuses.append("PENDING")
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
                logger.debug('\nDelete VM {}'.format(job_id))
                async_vm_delete = self.compute_client.virtual_machines.delete(
                    self.group_name, job_id)
                async_vm_delete.wait()
                self.instances.remove(job_id)
                return_vals.append(True)
            except Exception:
                return_vals.append(False)

        return return_vals

    @property
    def scaling_enabled(self):
        return True

    @property
    def label(self):
        return self._label

    @property
    def current_capacity(self):
        """Returns the current blocksize."""
        return len(self.instances)

    def create_nic(self, network_client):
        """Create (or update, if it exists already) a Network Interface for a VM.

            Also ensures that there's a virtual network available.

            We create a VPC with CIDR 10.0.0.0/16, which provides up to 64,000 instances.

            We attach a subnet for each availability zone within the region specified in the
            config. We give each subnet an ip range like 10.0.X.0/20, which is large enough
            for approx. 4000 instances.


        """
        try:
            logger.info('\nCreating (or updating) Vnet')
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

        except Exception:
            logger.info('Found Existing Vnet. Proceeding.')

        # Create Subnet
        logger.info('\nCreating (or updating) Subnet')
        async_subnet_creation = self.network_client.subnets.create_or_update(
            self.group_name, self.vnet_name, "{}.subnet".format(
                self.group_name), {'address_prefix': '10.0.0.0/20'})
        subnet_info = async_subnet_creation.result()

        if not self.resources.get("subnets", None):
            self.resources["subnets"] = {}

        self.resources["subnets"][subnet_info.id] = subnet_info

        # Create NIC
        logger.info('\nCreating (or updating) NIC')
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

    def create_vm_parameters(self, nic_id, vm_reference, cmd_str):
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
            },
            'resources': [{
                "location": self.location,
                "properties": {
                    "publisher": "Microsoft.Azure.Extensions",
                    "type": "CustomScript",
                    "typeHandlerVersion": "2.0",
                    "autoUpgradeMinorVersion": False,
                    "protectedSettings": {
                        "script": str(base64.b64encode(bytes(cmd_str, encoding='utf-8')))
                    }
                }
            }]
        }

    def create_disk(self):
        """Create a managed data disk of size specified in config.

        Each instance gets one disk"""
        logger.info('\nCreate (empty) managed Data Disk')
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
