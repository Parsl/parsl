"""Config for Azure"""
import getpass

from parsl.config import Config
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.http import HTTPInTaskStaging
from parsl.data_provider.rsync import RSyncStaging
from parsl.executors import HighThroughputExecutor
from parsl.providers import AzureProvider

# If you are a developer running tests, make sure to update parsl/tests/configs/user_opts.py
# If you are a user copying-and-pasting this as an example, make sure to either
#       1) create a local `user_opts.py`, or
#       2) delete the user_opts import below and replace all appearances of `user_opts` with the literal value
#          (i.e., user_opts['swan']['username'] -> 'your_username')
from parsl.tests.configs.user_opts import user_opts

vm_reference = {

    # these fields gave me key errors unless I specified them:
    "admin_username": user_opts['azure']['admin_username'],
    "password": user_opts['azure']['password'],
    "vm_size": user_opts['azure']['vm_size'],
    "disk_size_gb": user_opts['azure']['disk_size_gb'],
    "publisher": user_opts['azure']['publisher'],
    "offer": user_opts['azure']['offer'],
    "sku": user_opts['azure']['sku'],
    "version": user_opts['azure']['version']
}

provider = AzureProvider(
              vm_reference=vm_reference,
              key_file='azure_key_file.json',
            )

config = Config(
    executors=[
        HighThroughputExecutor(
            storage_access=[HTTPInTaskStaging(), FTPInTaskStaging(), RSyncStaging(getpass.getuser() + "@" + user_opts['public_ip'])],
            label='azure_single_node',
            address=user_opts['public_ip'],
            encrypted=True,
            provider=provider
        )
    ]
)
