"""Config for Azure"""
import getpass

from parsl.addresses import address_by_query
from parsl.config import Config
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.http import HTTPInTaskStaging
from parsl.data_provider.rsync import RSyncStaging
from parsl.executors import HighThroughputExecutor
from parsl.providers import AzureProvider
from parsl.usage_tracking.levels import LEVEL_1

vm_reference = {
    # All fields below are required
    "admin_username": 'YOUR_ADMIN_USERNAME_FOR_INSTANCE',
    "password": 'YOUR_PASSWORD_FOR_INSTANCE',
    "vm_size": 'YOUR_VM_SIZE',
    "disk_size_gb": 'YOUR_VM_DISK_SIZE',
    "publisher": 'YOUR_IMAGE_PUBLISHER',
    "offer": 'YOUR_VM_OS_OFFER',
    "sku": 'YOUR_VM_OS_SKU',
    "version": 'YOUR_VM_OS_VERSION',
}

config = Config(
    executors=[
        HighThroughputExecutor(
            label='azure_single_node',
            provider=AzureProvider(
                vm_reference=vm_reference,
                key_file='azure_key_file.json',
            ),
            storage_access=[HTTPInTaskStaging(),
                            FTPInTaskStaging(),
                            RSyncStaging(getpass.getuser() + "@" + address_by_query())],
        )
    ],
    usage_tracking=LEVEL_1,
)
