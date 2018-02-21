from libsubmit.providers.provider_base import ExecutionProvider
from libsubmit.exec_utils import wtime_to_minutes
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

        pass

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

        pass

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
