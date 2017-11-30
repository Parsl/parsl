import os
import logging
import subprocess
import math
import time
from string import Template
from libsubmit.providers.provider_base import ExecutionProvider
from libsubmit.providers.cobalt.template import template_string
from libsubmit.exec_utils import execute_wait, wtime_to_minutes
from libsubmit.launchers import Launchers
import libsubmit.error as ep_error

logger = logging.getLogger(__name__)

translate_table = { 'queued'  :  'PENDING',
                    'starting' : 'PENDING',
                    'running' :  'RUNNING',
                    'exiting' : 'COMPLETED',
                    'killing' : 'COMPLETED'
                  } # (special exit state




class Cobalt(ExecutionProvider):
    ''' Cobalt Execution Provider

    This provider uses sbatch to submit, squeue for status and scancel to cancel
    jobs. The sbatch script to be used is created from a template file in this
    same module.

    .. code-block:: python

         { "execution" : {
              "executor" : "ipp",
              "provider" : "cobalt",  # LIKELY SHOULD BE BOUND TO SITE
              "script_dir" : ".scripts",
              "block" : { # Definition of a block
                  "nodes" : 1,            # of nodes in that block
                  "taskBlocks" : 1,       # total tasks in a block
                  "walltime" : "00:05:00",
                  "initBlocks" : 2,
                  "minBlocks" : 0,
                  "maxBlocks" : 2,
                  "scriptDir" : ".",
                  "options" : {
                      "partition" : "debug",
                      "overrides" : "source /home/yadunand/setup_cooley_env.sh"
                  }
              }
          }
         }
    '''

    def __repr__ (self):
        return "<Cobalt Execution Provider for site:{0}>".format(self.sitename)

    def __init__ (self, config, channel=None):
        ''' Initialize the Cobalt execution provider class

        Args:
             - Config (dict): Dictionary with all the config options.

        KWargs :
             - channel (channel object) : default=None A channel object
        '''

        self.channel = channel
        if self.channel == None:
            logger.error("Provider:Cobalt cannot be initialized without a channel")
            raise(ep_error.ChannelRequired(self.__class__.__name__,
                                           "Missing a channel to execute commands"))

        self.config = config
        self.sitename = config['site']
        self.current_blocksize = 0

        self.max_walltime = wtime_to_minutes(self.config["execution"]["block"].get("walltime", '01:00:00'))

        if not os.path.exists(self.config["execution"].get("scriptDir", '.scripts')):
            os.makedirs(self.config["execution"]["scriptDir"])

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

    @property
    def channels_required(self):
        ''' Returns Bool on whether a channel is required
        '''

        return True

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

        #job_id_list  = ','.join(self.resources.keys())

        jobs_missing = list(self.resources.keys())

        retcode, stdout, stderr = self.channel.execute_wait("qstat -u $USER", 3)
        for line in stdout.split('\n'):
            if line.startswith('=') : continue

            parts = line.upper().split()
            if parts and parts[0] != 'JOBID':
                job_id = parts[0]
                print(parts)

                if job_id not in self.resources : continue

                status = translate_table.get(parts[4], 'UNKNOWN')

                self.resources[job_id]['status'] = status
                jobs_missing.remove(job_id)

        print("Jobs list : " , self.resources)
        # squeue does not report on jobs that are not running. So we are filling in the
        # blanks for missing jobs, we might lose some information about why the jobs failed.
        for missing_job in jobs_missing:
            if self.resources[missing_job]['status'] in ['PENDING', 'RUNNING']:
                self.resources[missing_job]['status'] = translate_table['CD']

    def status (self, job_ids):
        '''  Get the status of a list of jobs identified by their ids.

        Args:
            - job_ids (List of ids) : List of identifiers for the jobs

        Returns:
            - List of status codes.

        '''

        self._status()
        return [self.resources[jid]['status'] for jid in job_ids]


    ###########################################################################################################
    # Write submit script
    ###########################################################################################################
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
            script_dir = os.path.dirname(script_filename)
            if script_dir :
                os.makedirs(script_dir)

        except Exception as e:
            if e.errno == 17:
                pass
            else:
                logger.error("Unable to create script_dir:{0} due to:{1}".format(
                        script_dir,
                        e))
                raise(ep_error.ScriptPathError(script_filename, e ))

        try:
            submit_script = Template(template_string).substitute(**configs)

            with open(script_filename, 'w') as f:
                f.write(submit_script)
            os.chmod(script_filename, 0o777)

        except KeyError as e:
            logger.error("Missing keys for submit script : %s", e)
            raise(ep_error.SchedulerMissingArgs(e.args, self.sitename))

        except IOError as e:
            logger.error("Failed writing to submit script: %s", script_filename)
            raise(ep_error.ScriptPathError(script_filename, e))

        return True

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


        # Set account options
        account_opt = ''
        if self.config["execution"]["block"]["options"].get("account", None):
            account_opt = "-A {0}".format(self.config["execution"]["block"]["options"]["account"])

        # Set job name
        job_name = "parsl.{0}.{1}".format(job_name,time.time())

        # Set script path
        script_path = "{0}/{1}.submit".format(self.config["execution"]["block"].get("script_dir",'./.scripts'),
                                              job_name)
        script_path = os.path.abspath(script_path)

        # Calculate nodes
        nodes = self.config["execution"]["block"].get("nodes", 1)
        logger.debug("Requesting blocksize:%s nodes:%s taskBlocks:%s", blocksize,
                     nodes,
                     self.config["execution"]["block"].get("taskBlocks", 1))

        job_config = self.config["execution"]["block"]["options"]
        job_config["nodes"] = nodes
        job_config["overrides"] = job_config.get("overrides", '')
        job_config["jobname"] = job_name
        # Wrap the cmd_string
        lname = self.config["execution"]["block"].get("launcher", "singleNode")
        launcher = Launchers.get(lname, None)
        job_config["user_script"] = launcher(cmd_string,
                                             self.config["execution"]["block"]["taskBlocks"])

        # Get queue request if requested
        self.queue = ''
        if job_config.get("queue", None):
            self.queue = "-q {0}".format(job_config["queue"])


        logger.debug("Writing submit script")
        ret = self._write_submit_script(template_string, script_path, job_name, job_config)

        channel_script_path = self.channel.push_file(script_path, self.channel.script_dir)

        logger.debug("Executing : qsub -n {0} {1} -t {2} {3} {4}".format(nodes,
                                                                         self.queue,
                                                                         self.max_walltime,
                                                                         account_opt,
                                                                         channel_script_path))
        
        retcode, stdout, stderr = self.channel.execute_wait(
            "qsub -n {0} {1} -t {2} {3} {4}".format(nodes,
                                                    self.queue,
                                                    self.max_walltime,
                                                    account_opt,
                                                    channel_script_path), 5)

        # TODO : FIX this block
        if retcode != 0 :
            logger.error("Launch failed stdout:\n{0}  \nstderr:{1}\n".format(stdout, stderr))
        logger.debug ("Retcode:%s STDOUT:%s STDERR:%s", retcode,
                      stdout.strip(), stderr.strip())

        job_id = None

        if retcode == 0 :
            # We should be getting only one line back
            job_id = stdout.strip()
            self.resources[job_id] = {'job_id' : job_id,
                                      'status' : 'PENDING',
                                      'blocksize'   : blocksize }
        else:
            logger.error("Submission of command to scale_out failed: {0}".format(stderr))
            raise(ep_error.ScaleOutFailed(self.__class__,
                                          "Request to submit job to local scheduler failed"))

        logger.debug("Returning job id : {0}".format(job_id))
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
        retcode, stdout, stderr = self.channel.execute_wait("qdel {0}".format(job_id_list), 3)
        rets = None
        if retcode == 0 :
            for jid in job_ids:
                self.resources[jid]['status'] = translate_table['killing'] # Setting state to cancelled
            rets = [True for i in job_ids]
        else:
            rets = [False for i in job_ids]

        return rets

    @property
    def scaling_enabled(self):
        return True

    @property
    def current_capacity(self):
        return self

    def _test_add_resource (self, job_id):
        self.resources.extend([{'job_id' : job_id,
                                'status' : 'PENDING',
                                'size'   : 1 }])
        return True

if __name__ == "__main__" :


    config = {  "site" : "cooley",
                "execution" :
                    {"executor" : "ipp",
                     "provider" : "cobalt",
                     "block"  : {
                         "initParallelism" : 2,
                         "maxParallelism" : 2,
                         "minParallelism" : 0,
                         "walltime" : "00:25:00",
                         "options" : {
                             "account" : "ExM",
                             "submit_script_dir" : ".scripts",
                             "overrides" : "",
                             }
                         }
                     }
                }

    p = Cobalt(config)
    p._status()
    p.submit("echo 'Hello World'", 1)
    p._status()
