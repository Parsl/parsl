import os
import logging
import subprocess
import math
import time
import signal
from string import Template
from libsubmit.providers.provider_base import ExecutionProvider
from libsubmit.launchers import Launchers
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
    ''' Local Execution Provider

    This provider is used to launch IPP engines on the localhost.

    .. warning::
        Please note that in the config documented below, description and values
        are placed inside a schema that is delimited by #{ schema.. }

    Here's the scheme for the Local provider:

    .. code-block:: python

         { "execution" : { # Definition of all execution aspects of a site

              "executor"   : #{Description: Define the executor used as task executor,
                             # Type : String,
                             # Expected : "ipp",
                             # Required : True},

              "provider"   : #{Description : The provider name, in this case local
                             # Type : String,
                             # Expected : "local",
                             # Required :  True },

              "scriptDir"  : #{Description : Relative or absolute path to a
                             # directory in which intermediate scripts are placed
                             # Type : String,
                             # Default : "./.scripts"},

              "block" : { # Definition of a block

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

                  "taskBlocks": #{Description : workers to launch per request
                                # Type : Integer
                                # Default : 1 },
              }
            }
         }

    '''

    def __repr__ (self):
        return "<Local Execution Provider for site:{0}>".format(self.sitename)

    def __init__ (self, config, channel_script_dir=None, channel=None):
        ''' Initialize the local provider class

        Args:
             - Config (dict): Dictionary with all the config options.
        '''

        self.channel = channel
        self.config = config
        self.sitename = config['site']
        self.current_blocksize = 0
        self.scriptDir  = self.config["execution"]["scriptDir"]
        self.taskBlocks = self.config["execution"]["block"].get("taskBlocks", 1)
        launcher_name   = self.config["execution"]["block"].get("launcher",
                                                                "singleNode")
        self.launcher   = Launchers.get(launcher_name, None)

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}


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

        logging.debug("Checking status of : {0}".format(job_ids))
        for job_id in self.resources:
            poll_code = self.resources[job_id]['proc'].poll()
            if self.resources[job_id]['status'] in ['COMPLETED', 'FAILED']:
                continue

            if poll_code == None :
                self.resources[job_id]['status'] = 'RUNNING'
            elif poll_code == 0 and self.resources[job_id]['status'] != 'RUNNING':
                self.resources[job_id]['status'] = 'COMPLETED'
            elif poll_code < 0 and self.resources[job_id]['status'] != 'RUNNING' :
                self.resources[job_id]['status'] = 'FAILED'

        return [self.resources[jid]['status'] for jid in job_ids]

    ###########################################################################################################
    # Write the submit script
    ###########################################################################################################
    def _write_submit_script(self, script_string, script_filename):
        '''
        Load the template string with config values and write the generated submit script to
        a submit script file.

        Args:
              - template_string (string) : The template string to be used for the writing submit script
              - script_filename (string) : Name of the submit script

        Returns:
              - True: on success

        Raises:
              SchedulerMissingArgs : If template is missing args
              ScriptPathError : Unable to write submit script out
        '''

        try:
            with open(script_filename, 'w') as f:
                f.write(script_string)

        except KeyError as e:
            logger.error("Missing keys for submit script : %s", e)
            raise(ep_error.SchedulerMissingArgs(e.args, self.sitename))

        except IOError as e:
            logger.error("Failed writing to submit script: %s", script_filename)
            raise(ep_error.ScriptPathError(script_filename, e))

        return True

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

        job_name = "{0}.{1}".format(job_name, time.time())

        # Set script path
        script_path = "{0}/{1}.sh".format(self.scriptDir,
                                              job_name)
        script_path = os.path.abspath(script_path)

        lname = self.config["execution"]["block"].get("launcher", "singleNode")
        launcher = Launchers.get(lname, None)
        wrap_cmd_string = self.launcher(cmd_string,
                                        taskBlocks=self.taskBlocks)

        ret = self._write_submit_script(wrap_cmd_string, script_path)

        job_id, proc = execute_no_wait('bash {0}'.format(script_path),
                                       3)
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
            proc = self.resources[job]['proc']
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
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

    print("Nothing here")
