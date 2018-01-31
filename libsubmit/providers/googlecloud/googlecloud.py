import os
import pprint
import math
import json
import time
import logging
import atexit
from datetime import datetime, timedelta
from string import Template
from libsubmit.providers.provider_base import ExecutionProvider
from libsubmit.launchers import Launchers
from libsubmit.error import *

logger = logging.getLogger(__name__)

try:
    import googleapiclient

except ImportError:
    _google_enabled = False
else:
    _google_enabled = True

translate_table = {'qw': 'PENDING',
                   'r': 'RUNNING',
                   'd': 'COMPLETED',
                   'shutting-down': 'COMPLETED',  # (configuring),
                   'rd': 'COMPLETED',  # We shouldn't really see this state
                   'stopped': 'COMPLETED',  # We shouldn't really see this state
                   }


class GoogleCloud():  # ExcecutionProvider):
    """ Define the Google Cloud provider

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

    def __init__(self, config, channel=None):
        ''' Initialize the GridEngine class

        Args:
             - Config (dict): Dictionary with all the config options.

        KWargs:
             - Channel (None): A channel is required for slurm.
        '''
        self.channel = channel
        self.config = config
        self.sitename = config['site']
        self.client = None
        launcher_name = self.config["execution"]["block"].get("launcher",
                                                              "singleNode")
        self.launcher = Launchers.get(launcher_name, None)
        self.scriptDir = self.config["execution"]["scriptDir"]
        if not os.path.exists(self.scriptDir):
            os.makedirs(self.scriptDir)

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}
        self.current_blocksize = 0
        atexit.register(self.bye)

    def get_google_client():
        pass

    def __repr__(self):
        return "<Google Cloud Platform Execution Provider for site:{0}>".format(
            self.sitename, self.channel)

    def submit(self, cmd_string=None, blocksize=1, job_name="parsl.auto"):
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

        pass

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
        ''' Scaling is enabled

        Returns:
              - Status (Bool)
        '''
        return True

    @property
    def current_capacity(self):
        ''' Returns the current blocksize.
        This may need to return more information in the futures :
        { minsize, maxsize, current_requested }
        '''
        return self.current_blocksize

    @property
    def channels_required(self):
        '''Google Compute does not require a channel

        Returns:
              - Status (Bool)
        '''
        return False

    def bye(self):
        self.cancel([i for i in list(self.resources)])
