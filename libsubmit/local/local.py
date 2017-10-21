import os
import logging
import subprocess
import math
import time
import signal
from string import Template
from libsubmit.execution_provider_base import ExecutionProvider
from libsubmit.exec_utils import execute_no_wait
import libsubmit.error as ep_error
logger = logging.getLogger(__name__)



translate_table = { 'PD' :  'PENDING',
                    'R'  :  'RUNNING',
                    'CA' : 'CANCELLED',
                    'CF' : 'PENDING', #(configuring),
                    'CG' : 'RUNNING', # (completing),
                    'CD' : 'COMPLETED',
                    'F'  : 'FAILED', # (failed),
                    'TO' : 'TIMEOUT', # (timeout),
                    'NF' : 'FAILED', # (node failure),
                    'RV' : 'FAILED', #(revoked) and
                    'SE' : 'FAILED' } # (special exit state


class Local(ExecutionProvider):
    ''' Slurm Execution Provider

    This provider uses sbatch to submit, squeue for status and scancel to cancel jobs.
    '''

    def __repr__ (self):
        return "<Local Execution Provider for site:{0}>".format(self.sitename)

    def __init__ (self, config, channel_script_dir=None, channel=None):
        ''' Initialize the Slurm class
        Args:
             - Config (dict): Dictionary with all the config options.
        '''
        self.channel = channel
        self.config = config
        self.sitename = config['site']
        self.current_blocksize = 0

        if channel_script_dir:
            self.channel_script_dir = channel_script_dir
        else:
            self.channel_script_dir = os.path.abspath("./.scripts")

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

    @property
    def script_dir(self):
        return self.channel_script_dir

    @property
    def channels_required(self):
        ''' Returns Bool on whether a channel is required
        '''
        return False

    ###########################################################################################################
    # Status
    ###########################################################################################################
    def status (self, job_ids):
        '''  Get the status of a list of jobs identified by their ids.
        Args:
            - job_ids (List of ids) : List of identifiers for the jobs

        Returns:
            - List of status codes.

        '''
        for job_id in self.resources:
            poll_code = self.resources[job_id]['proc'].poll()
            if poll_code == None :
                self.resources[job_id]['status'] = 'RUNNING'
            elif poll_code == 0 and self.resources[job_id]['status'] != 'RUNNING':
                self.resources[job_id]['status'] = 'COMPLETED'
            elif poll_code < 0 and self.resources[job_id]['status'] != 'RUNNING' :
                self.resources[job_id]['status'] = 'FAILED'

        return [self.resources[jid]['proc'].poll() for jid in job_ids]


    ###########################################################################################################
    # Submit
    ###########################################################################################################
    def submit (self, cmd_string, blocksize, job_name="parsl.auto"):
        ''' Submits the cmd_string onto an Local Resource Manager job of blocksize parallel elements.
        Submit returns an ID that corresponds to the task that was just submitted.

        If tasks_per_node <  1:
             1/tasks_per_node is provisioned

        If tasks_per_node == 1:
             A single node is provisioned

        If tasks_per_node >  1 :
             tasks_per_node * blocksize number of nodes are provisioned.

        Args:
             - cmd_string  :(String) Commandline invocation to be made on the remote side.
             - blocksize   :(float) - Not really used for local

        Kwargs:
             - job_name (String): Name for job, must be unique

        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job

        '''

        job_id, proc = execute_no_wait(cmd_string, 3)
        logger.debug("Started pid:%s proc:%s ", job_id, proc)
        self.resources[job_id] = {'job_id' : job_id,
                                  'status' : 'RUNNING',
                                  'blocksize' : blocksize,
                                  'proc' : proc }

        return job_id

    ###########################################################################################################
    # Cancel
    ###########################################################################################################
    def cancel(self, job_ids):
        ''' Cancels the jobs specified by a list of job ids

        Args:
        job_ids : [<job_id> ...]

        Returns :
        [True/False...] : If the cancel operation fails the entire list will be False.
        '''

        for job in job_ids:
            logger.debug("Terminating job/proc_id : {0}".format(job))
            # Here we are assuming that for local, the job_ids are the process id's
            os.killpg(os.getpgid(job), signal.SIGTERM)
            self.resources[job]['status'] = 'CANCELLED'
        rets = [True for i in job_ids]

        return rets

    @property
    def scaling_enabled(self):
        return True

    @property
    def current_capacity(self):
        return len(self.resources)

    @property
    def channels_required(self):
        return False


if __name__ == "__main__" :

    print("None")
