''' Execution Provider Factory

Centralize creation of execution providers and executors.

'''

import logging

logger = logging.getLogger(__name__)

# Executors
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.swift_t import TurbineExecutor
from parsl.executors.threads import ThreadPoolExecutor

# Execution Providers
from parsl.execution_provider.slurm.slurm import Slurm
from parsl.execution_provider.aws.aws import EC2Provider
from parsl.execution_provider.local.local import Local

# Channels
from parsl.channels.ssh_cl import SshClClient

class ExecProviderFactory (object):

    def __init__ (self):
        ''' Constructor

        Args:
             name (string) : Name of the execution resource

        Returns:
             object (ExecutionProvider)

        '''

        self.executors = { 'ipp' : IPyParallelExecutor,
                           'swift_t' : TurbineExecutor,
                           'threads' : ThreadPoolExecutor }

        self.execution_providers = { 'slurm' : Slurm,
                                     'local' : Local,
                                     'aws' : EC2Provider }

        self.channels = { 'ssh-cl' : SshClClient,
                          'local' : None }


    def make (self, config):
        ''' Construct the appropriate provider, executors and channels and link them together.
        '''
        sitename = config['site']
        provider = config['execution']['provider']
        executor = config['execution']['executor']
        channel = config['execution']['channel']

        if provider not in self.execution_providers:
            logger.debug("[ERROR] %s is not a known execution_provider", provider)
            exit(-1)
        if executor not in self.executors:
            logger.debug("[ERROR] %s is not a known executor", executor)
            exit(-1)
        if channel not in self.channels:
            logger.debug("[ERROR] %s is not a known channel", channel)

        print("Foo")
        ep = self.execution_providers[provider](config)

        executor = self.executors[executor](execution_provider=ep, config=config)
        return executor




