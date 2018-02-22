import os
import logging
import subprocess
import math
import time

from libsubmit.providers.provider_base import ExecutionProvider
from libsubmit.providers.slurm.template import template_string
from libsubmit.launchers import Launchers
import libsubmit.error as ep_error
from libsubmit.providers.cluster_provider import ClusterProvider

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

class Slurm(ClusterProvider):
    ''' Slurm Execution Provider

    This provider uses sbatch to submit, squeue for status and scancel to cancel
    jobs. The sbatch script to be used is created from a template file in this
    same module.

    .. warning::
        Please note that in the config documented below, description and values
        are placed inside a schema that is delimited by <{ schema.. }>

    Here's a sample config for the Slurm provider:

    .. code-block:: python

         { "execution" : { # Definition of all execution aspects of a site

              "executor"   : #{Description: Define the executor used as task executor,
                             # Type : String,
                             # Expected : "ipp",
                             # Required : True},

              "provider"   : #{Description : The provider name, in this case slurm
                             # Type : String,
                             # Expected : "slurm",
                             # Required :  True },

              "scriptDir"  : #{Description : Relative or absolute path to a
                             # directory in which intermediate scripts are placed
                             # Type : String,
                             # Default : "./.scripts"},

              "block" : { # Definition of a block

                  "nodes"      : #{Description : # of nodes to provision per block
                                 # Type : Integer,
                                 # Default: 1},

                  "taskBlocks" : #{Description : # of workers to launch per block
                                 # as either an number or as a bash expression.
                                 # for eg, "1" , "$(($CORES / 2))"
                                 # Type : String,
                                 #  Default: "1" },

                  "walltime"  :  #{Description : Walltime requested per block in HH:MM:SS
                                 # Type : String,
                                 # Default : "00:20:00" },

                  "initBlocks" : #{Description : # of blocks to provision at the start of
                                 # the DFK
                                 # Type : Integer
                                 # Default : ?
                                 # Required :    },

                  "minBlocks" :  #{Description : Minimum # of blocks outstanding at any time
                                 # WARNING :: Not Implemented
                                 # Type : Integer
                                 # Default : 0 },

                  "maxBlocks" :  #{Description : Maximum # Of blocks outstanding at any time
                                 # WARNING :: Not Implemented
                                 # Type : Integer
                                 # Default : ? },

                  "options"   : {  # Scheduler specific options

                      "partition" : #{Description : Slurm partition to request blocks from
                                    # Type : String,
                                    # Required : True },

                      "overrides" : #{"Description : String to append to the #SBATCH blocks
                                    # in the submit script to the scheduler
                                    # Type : String,
                                    # Required : False },
                  }
              }
            }
         }
    '''
    def __init__ (self, config, channel=None):
        ''' Initialize the Slurm class

        Args:
             - Config (dict): Dictionary with all the config options.

        KWargs:
             - Channel (None): A channel is required for slurm.
        '''

        super().__init__(config, channel=channel)


    ###########################################################################################################
    # Status
    ###########################################################################################################
    def _status(self):
        ''' Internal: Do not call. Returns the status list for a list of job_ids

        Args:
              self

        Returns:
              [status...] : Status list of all jobs
        '''
        job_id_list  = ','.join(self.resources.keys())
        cmd = "squeue --job {0}".format(job_id_list)

        retcode, stdout, stderr = super().execute_wait(cmd)

        # Execute_wait failed. Do no update
        if retcode != 0 :
            return

        jobs_missing = list(self.resources.keys())
        for line in stdout.split('\n'):
            parts = line.split()
            if parts and parts[0] != 'JOBID' :
                job_id = parts[0]
                status = translate_table.get(parts[4], 'UNKNOWN')
                self.resources[job_id]['status'] = status
                jobs_missing.remove(job_id)

        # squeue does not report on jobs that are not running. So we are filling in the
        # blanks for missing jobs, we might lose some information about why the jobs failed.
        for missing_job in jobs_missing:
            if self.resources[missing_job]['status'] in ['PENDING', 'RUNNING']:
                self.resources[missing_job]['status'] = 'COMPLETED'

    ###########################################################################################################
    # Submit
    ###########################################################################################################
    def submit (self, cmd_string, blocksize, job_name="parsl.auto"):
        ''' Submits the cmd_string onto an Local Resource Manager job of blocksize parallel elements.
        Submit returns an ID that corresponds to the task that was just submitted.

        If tasks_per_node <  1 : ! This is illegal. tasks_per_node should be integer

        If tasks_per_node == 1:
             A single node is provisioned

        If tasks_per_node >  1 :
             tasks_per_node * blocksize number of nodes are provisioned.

        Args:
             - cmd_string  :(String) Commandline invocation to be made on the remote side.
             - blocksize   :(float)

        Kwargs:
             - job_name (String): Name for job, must be unique

        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job

        '''

        if self.current_blocksize >= self.config["execution"]["block"].get("maxBlocks", 2):
            logger.warn("[%s] at capacity, cannot add more blocks now", self.sitename)
            return None

        # Note: Fix this later to avoid confusing behavior.
        # We should always allocate blocks in integer counts of node_granularity
        if blocksize < self.config["execution"]["block"].get("nodes", 1):
            blocksize = self.config["execution"]["block"].get("nodes",1)

        # Set job name
        job_name = "{0}.{1}".format(job_name,time.time())

        # Set script path
        script_path = "{0}/{1}.submit".format(self.scriptDir,
                                              job_name)
        script_path = os.path.abspath(script_path)

        # Calculate nodes
        nodes = self.config["execution"]["block"].get("nodes", 1)
        logger.debug("Requesting blocksize:%s nodes:%s taskBlocks:%s", blocksize,
                     nodes,
                     self.config["execution"]["block"].get("taskBlocks", 1))

        job_config = self.config["execution"]["block"]["options"]
        # TODO : script_path might need to change to accommodate script dir set via channels
        job_config["submit_script_dir"] = self.channel.script_dir
        job_config["nodes"] = nodes
        job_config["taskBlocks"] = self.config["execution"]["block"].get("taskBlocks", 1)
        job_config["walltime"] = self.config["execution"]["block"].get("walltime", "00:20:00")
        job_config["overrides"] = job_config.get("overrides", '')
        job_config["user_script"] = cmd_string


        # Wrap the cmd_string
        job_config["user_script"] = self.launcher(cmd_string,
                                                  taskBlocks=job_config["taskBlocks"])

        logger.debug("Writing submit script")
        ret = self._write_submit_script(template_string, script_path, job_name, job_config)

        channel_script_path = self.channel.push_file(script_path, self.channel.script_dir)


        retcode, stdout, stderr = self.channel.execute_wait("sbatch {0}".format(channel_script_path), 10)

        job_id = None
        if retcode == 0 :
            for line in stdout.split('\n'):
                if line.startswith("Submitted batch job"):
                    job_id = line.split("Submitted batch job")[1].strip()
                    self.resources[job_id] = {'job_id' : job_id,
                                              'status' : 'PENDING',
                                              'blocksize'   : blocksize }
        else:
            print("Submission of command to scale_out failed")
            logger.error("Retcode:%s STDOUT:%s STDERR:%s", retcode, stdout.strip(), stderr.strip())
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

        job_id_list = ' '.join(job_ids)
        retcode, stdout, stderr = self.channel.execute_wait("scancel {0}".format(job_id_list), 3)
        rets = None
        if retcode == 0 :
            for jid in job_ids:
                self.resources[jid]['status'] = translate_table['CA'] # Setting state to cancelled
            rets = [True for i in job_ids]
        else:
            rets = [False for i in job_ids]

        return rets


    def _test_add_resource (self, job_id):
        self.resources.extend([{'job_id' : job_id,
                                'status' : 'PENDING',
                                'size'   : 1 }])
        return True

if __name__ == "__main__" :

    print("None")
