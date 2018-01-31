"""A deployer class to deploy a template on Azure"""
import os.path
import json
from haikunator import Haikunator
from azure.common.credentials import ServicePrincipalCredentials, UserPassCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode


class Deployer(object):
    """ Initialize the deployer class with subscription, resource group and public key.

    :raises IOError: If the public key path cannot be read (access or not exists)
    :raises KeyError: If AZURE_CLIENT_ID, AZURE_CLIENT_SECRET or AZURE_TENANT_ID env
        variables or not defined
    """
    config = ""

    def __init__(self, subscription_id, resource_group, config,
                 pub_ssh_key_path='~/.ssh/id_rsa.pub'):
        self.config = config
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.dns_label_prefix = self.name_generator.haikunate()
        self.location = self.config['location']

        pub_ssh_key_path = os.path.expanduser(pub_ssh_key_path)
        # Will raise if file not exists or not enough permission
        with open(pub_ssh_key_path, 'r') as pub_ssh_file_fd:
            self.pub_ssh_key = pub_ssh_file_fd.read()
        self.credentials = ServicePrincipalCredentials(
            client_id=self.config['AZURE_CLIENT_ID'],
            secret=self.config['AZURE_CLIENT_SECRET'],
            tenant=self.config['AZURE_TENANT_ID']
        )
        self.client = ResourceManagementClient(
            self.credentials, self.subscription_id)

    def deploy(self, job_name, cmd_string='', blocksize=1):
        instances = []
        """Deploy the template to a resource group."""
        self.client.resource_groups.create_or_update(
            self.resource_group,
            {
                'location': self.location,

            }
        )

        template_path = os.path.join(os.path.dirname(
            __file__), 'templates', 'template.json')
        with open(template_path, 'r') as template_file_fd:
            template = json.load(template_file_fd)

        parameters = {
            'sshKeyData': self.pub_ssh_key,
            'vmName': 'azure-deployment-sample-vm',
            'dnsLabelPrefix': self.dns_label_prefix
        }
        parameters = {k: {'value': v} for k, v in parameters.items()}

        deployment_properties = {
            'mode': DeploymentMode.incremental,
            'template': template,
            'parameters': parameters
        }
        for i in range(blocksize):
            deployment_async_operation = self.client.deployments.create_or_update(
                self.resource_group,
                'azure-sample',
                deployment_properties
            )
            instances.append(deployment_async_operation.wait())
        return instances

    def destroy(self, job_ids):
        """Destroy the given resource group"""
        for job_id in job_ids:
            self.client.resource_groups.delete(self.resource_group)

    def get_vm(self, resource_group_name, vm_name):
        '''
        you need to retry this just in case the credentials token expires,
        that's where the decorator comes in
        this will return all the data about the virtual machine
        '''
        return compute_client.virtual_machines.get(
            resource_group_name, vm_name, expand='instanceView')

    def get_vm_status(self, vm_name, rgn):
        '''
        this will just return the status of the virtual machine
        sometime the status may be unknown as shown by the azure portal;
        in that case statuses[1] doesn't exist, hence retrying on IndexError
        also, it may take on the order of minutes for the status to become
        available so the decorator will bang on it forever
        '''
        rgn = rgn if rgn else self.resource_group
        return self.client.virtual_machines.get(
            resource_group_name,
            vm_name).instance_view.statuses[1].display_status
