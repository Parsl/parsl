import logging
import os
from string import Template

import libsubmit.error as ep_error
from libsubmit.utils import wtime_to_minutes
from libsubmit.launchers import launchers
from libsubmit.providers.provider_base import ExecutionProvider

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

    def __init__(self, config, channel):
        ''' Here we do initialization that is common across all cluster-style providers

        Args:
             - Config (dict): Dictionary with all the config options.

        KWargs:
             - Channel (None): A channel is required for all cluster-style providers
        '''
        self._scaling_enabled = True
        self._channels_required = True
        self.channel = channel
        self.config = config
        self.sitename = config['site']
        launcher_name = self.config["execution"]["block"].get("launcher", "singleNode")
        self.max_walltime = wtime_to_minutes(self.config["execution"]["block"].get("walltime", '01:00:00'))
        self.provisioned_blocks = 0
        self.launcher = launchers.get(launcher, None)

        self.script_dir = self.config["execution"]["script_dir"]
        if not os.path.exists(self.script_dir):
            os.makedirs(self.script_dir)

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

    @property
    def channels_required(self):
        """Returns True if a channel is required, False otherwise."""
        return self._channels_required

    def execute_wait(self, cmd, timeout=10):
        return self.channel.execute_wait(cmd, timeout)

    def get_configs(self, cmd_string, blocksize):
        ''' Compose a flat dict job_config with all necessary configs
        for writing the submit script
        '''
        nodes = self.config["execution"]["block"].get("nodes", 1)
        logger.debug("Requesting blocksize:%s nodes:%s taskBlocks:%s", blocksize, nodes,
                     self.config["execution"]["block"].get("taskBlocks", 1))

        job_config = self.config["execution"]["block"]["options"]
        job_config["submit_script_dir"] = self.channel.script_dir
        job_config["nodes"] = nodes
        job_config["taskBlocks"] = self.config["execution"]["block"]["taskBlocks"]
        job_config["walltime"] = self.config["execution"]["block"]["walltime"]
        job_config["overrides"] = job_config.get("overrides", '')
        job_config["user_script"] = cmd_string

        job_config["user_script"] = self.launcher(cmd_string, taskBlocks=job_config["taskBlocks"])
        return job_config

    def _write_submit_script(self, template, script_filename, job_name, configs):
        """Load the template string with config values and write the generated submit script to
        a submit script file.

        Args:
              - template (string) : The template string to be used for the writing submit script
              - script_filename (string) : Name of the submit script
              - job_name (string) : job name
              - configs (dict) : configs that get pushed into the template

        Returns:
              - True: on success

        Raises:
              SchedulerMissingArgs : If template is missing args
              ScriptPathError : Unable to write submit script out
        """

        try:
            submit_script = Template(template).substitute(jobname=job_name, **configs)
            # submit_script = Template(template).safe_substitute(jobname=job_name, **configs)
            with open(script_filename, 'w') as f:
                f.write(submit_script)

        except KeyError as e:
            logger.error("Missing keys for submit script : %s", e)
            raise (ep_error.SchedulerMissingArgs(e.args, self.sitename))

        except IOError as e:
            logger.error("Failed writing to submit script: %s", script_filename)
            raise (ep_error.ScriptPathError(script_filename, e))
        except Exception as e:
            print("Template : ", template)
            print("Args : ", job_name)
            print("Kwargs : ", configs)
            logger.error("Uncategorized error: %s", e)
            raise (e)

        return True

    def _status(self):
        raise NotImplementedError

    def status(self, job_ids):
        """ Get the status of a list of jobs identified by the job identifiers
        returned from the submit request.

        Args:
             - job_ids (list) : A list of job identifiers

        Returns:
             - A list of status from ['PENDING', 'RUNNING', 'CANCELLED', 'COMPLETED',
               'FAILED', 'TIMEOUT'] corresponding to each job_id in the job_ids list.

        Raises:
             - ExecutionProviderException or its subclasses

        """
        if job_ids:
            self._status()
        return [self.resources[jid]['status'] for jid in job_ids]

    def cancel(self, job_ids):
        """ Cancels the resources identified by the job_ids provided by the user.

        Args:
             - job_ids (list): A list of job identifiers

        Returns:
             - A list of status from cancelling the job which can be True, False

        Raises:
             - ExecutionProviderException or its subclasses
        """

        raise NotImplementedError

    @property
    def scaling_enabled(self):
        """ The callers of ParslExecutors need to differentiate between Executors
        and Executors wrapped in a resource provider

        Returns:
              - Status (Bool)
        """
        return self._scaling_enabled

    @property
    def current_capacity(self):
        """ Returns the currently provisioned blocks.
        This may need to return more information in the futures :
        { minsize, maxsize, current_requested }
        """
        return self.provisioned_blocks
