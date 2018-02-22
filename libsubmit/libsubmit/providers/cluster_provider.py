from libsubmit.providers.provider_base import ExecutionProvider
from libsubmit.exec_utils import wtime_to_minutes
from string import Template
import os
import logging
import libsubmit.error as ep_error
from libsubmit.launchers import Launchers

logger = logging.getLogger(__name__)

class ClusterProvider(ExecutionProvider):
    """ This class defines behavior common to all cluster/supercompute sytle scheduler systems.

    .. code:: python

                                +------------------
                                |
          script_string ------->|  submit
               id      <--------|---+
                                |
          [ ids ]       ------->|  status
          [statuses]   <--------|----+
                                |
          [ ids ]       ------->|  cancel
          [cancel]     <--------|----+
                                |
          [True/False] <--------|  scaling_enabled
                                |
                                +-------------------
    """

    def __repr__ (self):
        return "<{0} Execution Provider for site:{0} with channel:{1}>".format(self.__class__,
                                                                               self.sitename,
                                                                               self.channel)

    def __init__(self, config, channel=None):
        ''' Here we do initialization that is common across all cluster-style providers

        Args:
             - Config (dict): Dictionary with all the config options.

        KWargs:
             - Channel (None): A channel is required for all cluster-style providers
        '''
        self._scaling_enabled = True
        self._channels_required = True
        self.channel = channel
        if self.channel == None:
            logger.error("Provider: Cannot be initialized without a channel")
            raise(ep_error.ChannelRequired(self.__class__.__name__,
                                           "Missing a channel to execute commands"))
        self.config = config
        self.sitename = config['site']
        self.current_blocksize = 0
        launcher_name = self.config["execution"]["block"].get("launcher",
                                                              "singleNode")
        self.launcher = Launchers.get(launcher_name, None)
        self.max_walltime = wtime_to_minutes(self.config["execution"]["block"].get("walltime", '01:00:00'))

        self.scriptDir = self.config["execution"]["scriptDir"]
        if not os.path.exists(self.scriptDir):
            os.makedirs(self.scriptDir)

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

    @property
    def channels_required(self):
        ''' Returns Bool on whether a channel is required
        '''
        return self._channels_required

    def execute_wait(self, cmd, timeout=10):
        return self.channel.execute_wait(cmd, timeout)

    def get_configs (self, cmd_string, blocksize):
        ''' Compose a flat dict job_config with all necessary configs
        for writing the submit script
        '''
        nodes = self.config["execution"]["block"].get("nodes", 1)
        logger.debug("Requesting blocksize:%s nodes:%s taskBlocks:%s", blocksize,
                     nodes,
                     self.config["execution"]["block"].get("taskBlocks", 1))

        job_config = self.config["execution"]["block"]["options"]
        job_config["submit_script_dir"] = self.channel.script_dir
        job_config["nodes"] = nodes
        job_config["taskBlocks"] = self.config["execution"]["block"]["taskBlocks"]
        job_config["walltime"] = self.config["execution"]["block"]["walltime"]
        job_config["overrides"] = job_config.get("overrides", '')
        job_config["user_script"] = cmd_string

        job_config["user_script"] = self.launcher(cmd_string,
                                                  taskBlocks=job_config["taskBlocks"])
        return job_config


    def _write_submit_script(self, template_string, script_filename, job_name, configs):
        '''
        Load the template string with config values and write the generated submit script to
        a submit script file.

        Args:
              - template_string (string) : The template string to be used for the writing submit script
              - script_filename (string) : Name of the submit script
              - job_name (string) : job name
              - configs (dict) : configs that get pushed into the template

        Returns:
              - True: on success

        Raises:
              SchedulerMissingArgs : If template is missing args
              ScriptPathError : Unable to write submit script out
        '''

        try:
            submit_script = Template(template_string).substitute(jobname=job_name, **configs)
            #submit_script = Template(template_string).safe_substitute(jobname=job_name, **configs)
            with open(script_filename, 'w') as f:
                f.write(submit_script)

        except KeyError as e:
            logger.error("Missing keys for submit script : %s", e)
            raise(ep_error.SchedulerMissingArgs(e.args, self.sitename))

        except IOError as e:
            logger.error("Failed writing to submit script: %s", script_filename)
            raise(ep_error.ScriptPathError(script_filename, e))
        except Exception as e:
            print("Template : ", template_string)
            print("Args : ", job_name)
            print("Kwargs : ", configs)
            logger.error("Uncategorized error: %s",e)
            raise(e)

        return True

    def submit(self, cmd_string, blocksize, job_name="parsl.auto"):
        ''' The submit method takes the command string to be executed upon
        instantiation of a resource most often to start a pilot (such as IPP engine
        or even Swift-T engines).

        Args :
             - cmd_string (str) : The bash command string to be executed.
             - blocksize (int) : Blocksize to be requested

        KWargs:
             - job_name (str) : Human friendly name to be assigned to the job request

        Returns:
             - A job identifier, this could be an integer, string etc

        Raises:
             - ExecutionProviderExceptions or its subclasses
        '''
        raise NotImplementedError


    def _status(self):
        raise NotImplementedError


    def status(self, job_ids):
        ''' Get the status of a list of jobs identified by the job identifiers
        returned from the submit request.

        Args:
             - job_ids (list) : A list of job identifiers

        Returns:
             - A list of status from ['PENDING', 'RUNNING', 'CANCELLED', 'COMPLETED',
               'FAILED', 'TIMEOUT'] corresponding to each job_id in the job_ids list.

        Raises:
             - ExecutionProviderExceptions or its subclasses

        '''
        self._status()
        return [self.resources[jid]['status'] for jid in job_ids]

    def cancel(self, job_ids):
        ''' Cancels the resources identified by the job_ids provided by the user.

        Args:
             - job_ids (list): A list of job identifiers

        Returns:
             - A list of status from cancelling the job which can be True, False

        Raises:
             - ExecutionProviderExceptions or its subclasses
        '''

        raise NotImplementedError

    @property
    def scaling_enabled(self):
        ''' The callers of ParslExecutors need to differentiate between Executors
        and Executors wrapped in a resource provider

        Returns:
              - Status (Bool)
        '''
        return self._scaling_enabled

    @property
    def current_capacity(self):
        ''' Returns the current blocksize.
        This may need to return more information in the futures :
        { minsize, maxsize, current_requested }
        '''
        return self.current_blocksize
