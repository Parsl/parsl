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
    name_generator = Haikunator()
    config = ""

    def __init__(self, subscription_id, resource_group, config, pub_ssh_key_path='~/.ssh/id_rsa.pub'):
        self.config = config
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.dns_label_prefix = self.name_generator.haikunate()
        self.location = self.config['location']

        pub_ssh_key_path = os.path.expanduser(pub_ssh_key_path)
        # Will raise if file not exists or not enough permission
        with open(pub_ssh_key_path, 'r') as pub_ssh_file_fd:
            self.pub_ssh_key = pub_ssh_file_fd.read()
        # self.credentials = UserPassCredentials(self.config['username'], self.config['pass'])
        self.credentials = ServicePrincipalCredentials(
            client_id=self.config['AZURE_CLIENT_ID'],
            secret=self.config['AZURE_CLIENT_SECRET'],
            tenant=self.config['AZURE_TENANT_ID']
        )
        self.client = ResourceManagementClient(self.credentials, self.subscription_id)

    def deploy(self):
        """Deploy the template to a resource group."""
        self.client.resource_groups.create_or_update(
            self.resource_group,
            {
                'location':self.location
            }
        )

        template_path = os.path.join(os.path.dirname(__file__), 'templates', 'template.json')
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

        deployment_async_operation = self.client.deployments.create_or_update(
            self.resource_group,
            'azure-sample',
            deployment_properties
        )
        deployment_async_operation.wait()

    def destroy(self):
        """Destroy the given resource group"""
        self.client.resource_groups.delete(self.resource_group)