import logging
import os
import time

from libsubmit.providers.cluster_provider import ClusterProvider
from libsubmit.providers.gridEngine.template import template_string

logger = logging.getLogger(__name__)

translate_table = {
    'qw': 'PENDING',
    'hqw': 'PENDING',
    'hrwq': 'PENDING',
    'r': 'RUNNING',
    's': 'FAILED',  # obsuspended
    'ts': 'FAILED',
    't': 'FAILED',  # Suspended by alarm
    'eqw': 'FAILED',  # Error states
    'ehqw': 'FAILED',  # ..
    'ehrqw': 'FAILED',  # ..
    'd': 'COMPLETED',
    'dr': 'COMPLETED',
    'dt': 'COMPLETED',
    'drt': 'COMPLETED',
    'ds': 'COMPLETED',
    'drs': 'COMPLETES',
}


class GridEngine(ClusterProvider):
    """ Define the Grid Engine provider

    .. warning::
        Please note that in the config documented below, description and values
        are placed inside a schema that is delimited by <{ schema.. }>

    Here's the config schema for the GridEngine provider:

    .. code-block:: python

         { "execution": { # Definition of all execution aspects of a site

              "executor": # {Description: Define the executor used as task executor,
                          # Type : String,
                          # Expected : "ipp",
                          # Required : True},

              "provider": # {Description : The provider name, in this case slurm
                          # Type : String,
                          # Expected : "gridEngine",
                          # Required :  True },

              "scriptDir": #{Description : Relative or absolute path to a
                           # directory in which intermediate scripts are placed
                           # Type : String,
                           # Default : "./.scripts"},

              "block": { # Definition of a block

                  "nodes": #{Description : # of nodes to provision per block
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
                                 # Type : Integer
                                 # Default : 0 },

                  "maxBlocks" :  #{Description : Maximum # Of blocks outstanding at any time
                                 # Type : Integer
                                 # Default : ? },

                  "options"   : {  # Scheduler specific options

                      "overrides" : #{"Description : String to append to the submit_scipt block
                                    # in the submit script to the scheduler
                                    # Type : String,
                                    # Required : False },
                  }
              }
            }
         }

     """

    def __init__(self, config, channel=None):
        ''' Initialize the GridEngine class

        Args:
             - Config (dict): Dictionary with all the config options.

        KWargs:
             - Channel (None): A channel is required for GridEngine.
        '''
        super().__init__(config, channel=channel)

    def submit(self, cmd_string="", blocksize=1, job_name="parsl.auto"):
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

        # Note: Fix this later to avoid confusing behavior.
        # We should always allocate blocks in integer counts of node_granularity
        if blocksize < self.config["execution"]["block"].get("nodes", 1):
            blocksize = self.config["execution"]["block"].get("nodes", 1)

        # Set job name
        job_name = "{0}.{1}".format(job_name, time.time())

        # Set script path
        script_path = "{0}/{1}.submit".format(self.scriptDir, job_name)
        script_path = os.path.abspath(script_path)

        job_config = self.get_configs(cmd_string, blocksize)

        logger.debug("Writing submit script")
        self._write_submit_script(template_string, script_path, job_name, job_config)

        channel_script_path = self.channel.push_file(script_path, self.channel.script_dir)
        cmd = "qsub -terse {0}".format(channel_script_path)
        retcode, stdout, stderr = super().execute_wait(cmd, 10)

        if retcode == 0:
            for line in stdout.split('\n'):
                job_id = line.strip()
                if not job_id:
                    continue
                self.resources[job_id] = {'job_id': job_id, 'status': 'PENDING', 'blocksize': blocksize}
                return job_id
        else:
            print("[WARNING!!] Submission of command to scale_out failed")
            logger.error("Retcode:%s STDOUT:%s STDERR:%s", retcode, stdout.strip(), stderr.strip())

    def _status(self):
        ''' Get the status of a list of jobs identified by the job identifiers
        returned from the submit request.

        Returns:
             - A list of status from ['PENDING', 'RUNNING', 'CANCELLED', 'COMPLETED',
               'FAILED', 'TIMEOUT'] corresponding to each job_id in the job_ids list.

        Raises:
             - ExecutionProviderExceptions or its subclasses

        '''

        cmd = "qstat"

        retcode, stdout, stderr = super().execute_wait(cmd)

        # Execute_wait failed. Do no update
        if retcode != 0:
            return

        jobs_missing = list(self.resources.keys())
        for line in stdout.split('\n'):
            parts = line.split()
            if parts and parts[0].lower().lower() != 'job-id' \
                    and not parts[0].startswith('----'):
                job_id = parts[0]
                status = translate_table.get(parts[4].lower(), 'UNKNOWN')
                if job_id in self.resources:
                    self.resources[job_id]['status'] = status
                    jobs_missing.remove(job_id)

        # Filling in missing blanks for jobs that might have gone missing
        # we might lose some information about why the jobs failed.
        for missing_job in jobs_missing:
            if self.resources[missing_job]['status'] in ['PENDING', 'RUNNING']:
                self.resources[missing_job]['status'] = 'COMPLETED'

    def cancel(self, job_ids):
        ''' Cancels the resources identified by the job_ids provided by the user.

        Args:
             - job_ids (list): A list of job identifiers

        Returns:
             - A list of status from cancelling the job which can be True, False

        Raises:
             - ExecutionProviderExceptions or its subclasses
        '''

        job_id_list = ' '.join(job_ids)
        cmd = "qdel {}".format(job_id_list)
        retcode, stdout, stderr = super().execute_wait(cmd, 3)

        rets = None
        if retcode == 0:
            for jid in job_ids:
                self.resources[jid]['status'] = "COMPLETED"
            rets = [True for i in job_ids]
        else:
            rets = [False for i in job_ids]

        return rets
