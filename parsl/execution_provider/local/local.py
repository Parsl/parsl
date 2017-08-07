import os
import logging
import subprocess
import math
import time
from string import Template
from parsl.execution_provider.execution_provider_base import ExecutionProvider
import parsl.execution_provider.error as ep_error

logger = logging.getLogger(__name__)

def execute_no_wait (cmd, walltime):
    ''' Synchronously execute a commandline string on the shell.
    Args:
         - cmd (string) : Commandline string to execute
         - walltime (int) : walltime in seconds, this is not really used now.

    Returns:
         A tuple of the following:
         retcode : Return code from the execution, -1 on fail
         stdout  : stdout string
         stderr  : stderr string

    Raises:
         None.
    '''
    retcode = -1
    stdout = None
    stderr = None
    try :
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        pid = proc.pid

    except Exception as e:
        print("Caught exception : {0}".format(e))
        logger.warn("Execution of command [%s] failed due to \n %s ",  (cmd, e))

    return pid, proc


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

    def __init__ (self, config):
        ''' Initialize the Slurm class
        Args:
             - Config (dict): Dictionary with all the config options.
        '''

        self.config = config
        self.sitename = config['site']
        self.current_blocksize = 0

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

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
        logging.debug("Started pid:%s proc:%s ", job_id, proc)
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
            print("Killing : ", job)
            self.resources[job]['proc'].kill()
            self.resources[job]['status'] = 'CANCELLED'
        rets = [True for i in job_ids]

        return rets

    @property
    def scaling_enabled(self):
        return True

    @property
    def current_capacity(self):
        return len(self.resources)


if __name__ == "__main__" :

    print("None")
