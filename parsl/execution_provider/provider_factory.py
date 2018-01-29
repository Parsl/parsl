''' Execution Provider Factory

Centralize creation of execution providers and executors.

'''

import copy
import logging

# Executors
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.swift_t import TurbineExecutor
from parsl.executors.threads import ThreadPoolExecutor
from parsl.execution_provider.errors import *

# Controller
from parsl.executors.ipp_controller import Controller

# Execution Providers and channels
from libsubmit import *

logger = logging.getLogger(__name__)


class ExecProviderFactory (object):

    def __init__(self):
        ''' Constructor for the execution provider factory.

        Args:
             None
        '''

        self.executors = {'ipp': IPyParallelExecutor,
                          'swift_t': TurbineExecutor,
                          'threads': ThreadPoolExecutor,
                          None: lambda *args, **kwargs: None}

        self.execution_providers = {'slurm': Slurm,
                                    'local': Local,
                                    'aws': EC2Provider,
                                    'cobalt': Cobalt,
                                    'condor': Condor,
                                    'torque': Torque,
                                    None: lambda *args, **kwargs: None}

        self.channels = {'ssh': SshChannel,
                         'ssh-il': SshILChannel,
                         'local': LocalChannel,
                         None: lambda *args, **kwargs: None}

    def validate_config(self, config):
        ''' Validate_config validates config
        There is no logic implemented here yet.
        This might be a good first task for a new dev.

        Args:
             - config (dict) : Config data structure
        Returns:
             - Bool: validity of config
        '''
        return True

    def make(self, rundir, config):
        ''' Construct the appropriate provider, executors and channels and link them together.
        '''

        self.rundir = rundir
        sites = {}

        for site in config.get("sites"):

            logger.debug("Constructing site : %s ", site.get('site', 'Unnamed_site'))
            channel_name = site["auth"]["channel"]

            if channel_name in self.channels:
                channel_opts = site["auth"].copy()
                if "channel" in channel_opts:
                    del channel_opts["channel"]
                channel = self.channels[channel_name](**channel_opts)

            else:
                logger.error("Site:{0} requests an invalid channel:{0}".format(site["site"],
                                                                               channel_name))
                raise BadConfig(site["site"],
                                "invalid channel:{0} requested".format(channel_name))

            logger.debug("Created channel : {0}".format(channel))

            provider_name = site["execution"]["provider"]
            if provider_name in self.execution_providers:
                provider = self.execution_providers[provider_name](site,
                                                                   channel=channel)

            else:
                logger.error("Site:{0} requests an invalid provider:{0}".format(site["site"],
                                                                                provider_name))
                raise BadConfig(site["site"],
                                "invalid provider:{0} requested".format(provider_name))

            logger.debug("Created execution_provider : {0}".format(provider))

            executor_name = site["execution"]["executor"]

            if executor_name in self.executors:

                controller = None

                if executor_name == 'ipp' and config.get("controller", None):

                    logger.debug("Starting controller")
                    # A controller needs to be started per run
                    site["controller"] = copy.copy(config["controller"])

                    site["controller"]['ipythonDir'] = self.rundir
                    site["controller"]['profile'] = config["controller"].get('profile', site["site"])

                    controller = Controller(**site["controller"])
                    logger.debug("Controller engine file : %s", controller.engine_file)
                    logger.debug("Controller client file : %s", controller.client_file)

                executor = self.executors[executor_name](execution_provider=provider,
                                                         controller=controller,
                                                         config=site)

            else:
                logger.error("Site:{0} requests an invalid executor:{0}".format(site["site"],
                                                                                executor_name))
                raise BadConfig(site["site"],
                                "invalid executor:{0} requested".format(executor_name))

            logger.debug("Created executor : {0}".format(executor))

            sites[site["site"]] = executor

        return sites
