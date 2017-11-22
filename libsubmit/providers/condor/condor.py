import os
import logging
import subprocess
import math
import time
import re
from string import Template
from libsubmit.providers.provider_base import ExecutionProvider
from libsubmit.providers.condor.template import template_string
from libsubmit.exec_utils import execute_wait, wtime_to_minutes
from libsubmit.launchers import Launchers
import libsubmit.error as ep_error

logger = logging.getLogger(__name__)

# See http://pages.cs.wisc.edu/~adesmet/status.html
translate_table = { '1'  : 'PENDING',
                    '2'  : 'RUNNING',
                    '3'  : 'CANCELLED',
                    '4'  : 'COMPLETED',
                    '5'  : 'FAILED',
                    '6'  : 'FAILED', }


class Condor(ExecutionProvider):
    ''' Condor Execution Provider

    .. code-block:: python

         { "execution" : {
              "executor" : "ipp",
              "provider" : "condor",  # LIKELY SHOULD BE BOUND TO SITE
              "scriptDir" : ".scripts",
              "environment": {},  # Env vars to be set on channel execution
              "block" : { # Definition of a block
                  "environment": {},  # Env vars to be set for submitted tasks
                  "nodes" : 1,            # of nodes in that block
                  "taskBlocks" : 1,       # total tasks in a block
                  "walltime" : "00:05:00",
                  "initBlocks" : 1,
                  "minBlocks" : 0,
                  "maxBlocks" : 1,
                  "scriptDir" : ".",
                  "options" : {
                      "partition" : "debug",
                      "overrides" : "",
                      "workerSetup" : """module load python/3.5.2;
                       python3 -m venv parsl_env;
                       source parsl_env/bin/activate;
                       pip3 install ipyparallel """
                  }
              }
           }
        }
    '''

    def __repr__ (self):
        return "<Condor Execution Provider for site:{0} with channel:{1}>".format(self.sitename, self.channel)

    def __init__ (self, config, channel=None):
        ''' Initialize the Condor class

        Args:
             - Config (dict): Dictionary with all the config options.

        KWargs:
             - Channel (none): A channel is required for htcondor.
        '''

        self.channel = channel
        if self.channel == None:
            logger.error("Provider:Condor cannot be initialized without a channel")
            raise(ep_error.ChannelRequired(self.__class__.__name__,
                                            "Missing a channel to execute commands"))

        self.config = config
        self.sitename = config['site']
        self.current_blocksize = 0

        self.max_walltime = wtime_to_minutes(self.config["execution"]["block"].get("walltime", '01:00:00'))

        if not os.path.exists(self.config["execution"].get("scriptDir", '.scripts')):
            os.makedirs([self.config["execution"].get("scriptDir", ".scripts")])

        self.config['execution']['environment'] = self.config['execution'].get('environment', {})
        self.config['execution']['block']['environment'] = self.config['execution']['block'].get('environment', {})

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

        job_id_list  = ' '.join(self.resources.keys())
        cmd = "condor_q {0} -af:jr JobStatus".format(job_id_list)
        retcode, stdout, stderr = self.channel.execute_wait(cmd, 3, envs=self.config['execution']['environment'])

        '''
        Example output: 

        $ condor_q 34524642.0 34524643.0 -af:jr JobStatus
        34524642.0 2
        34524643.0 1
        '''


        for line in stdout.strip().split('\n'):
            parts = line.split()
            job_id = parts[0]
            status = translate_table.get(parts[1], 'UNKNOWN')
            self.resources[job_id]['status'] = status

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
    # Submit
    ###########################################################################################################
    def _write_submit_script(self, template_string, script_filename, job_name, configs):
        '''
        Load the template string with config values and write the generated submit script to
        a submit script file.

        Args:
              - template_string (string) : The template string to be used for the writing submit script script_filename (string) : Name of the submit script
              - job_name (string) : job name
              - configs (dict) : configs that get pushed into the template

        Returns:
              - True: on success

        Raises:
              - SchedulerMissingArgs : If template is missing args
              - ScriptPathError : Unable to write submit script out
        '''

        # This section needs to be brought upto par with the cobalt provider.
        try:
            submit_script = Template(template_string).substitute(**configs,
                                                                 jobname=job_name)
            with open(script_filename, 'w') as f:
                f.write(submit_script)

        except KeyError as e:
            logger.error("Missing keys for submit script : %s", e)
            #raise(ep_error.SchedulerMissingArgs(e.args, self.sitename))

        except IOError as e:
            logger.error("Failed writing to submit script: %s", script_filename)
            raise(ep_error.ScriptPathError(script_filename, e))

        return True

    def submit (self, cmd_string, blocksize, job_name="parsl.auto"):
        ''' Submits the cmd_string onto an Local Resource Manager job of blocksize parallel elements.

        example file with the complex case of multiple submits per job:
            Universe =vanilla
            output = out.$(Cluster).$(Process)
            error = err.$(Cluster).$(Process)
            log = log.$(Cluster)
            leave_in_queue = true
            executable = test.sh
            queue 5
            executable = foo
            queue 1

        $ condor_submit test.sub
        Submitting job(s)......
        5 job(s) submitted to cluster 118907.
        1 job(s) submitted to cluster 118908.
        '''

        logger.debug("Attempting to launch at blocksize : %s" % blocksize)
        if self.current_blocksize >= self.config["execution"]["block"].get("maxBlocks", 2):
            logger.warn("[%s] at capacity, cannot add more blocks now", self.sitename)
            return None

        # Note: Fix this later to avoid confusing behavior.
        # We should always allocate blocks in integer counts of node_granularity
        if blocksize < self.config["execution"]["block"].get("nodes", 1):
            blocksize = self.config["execution"]["block"].get("nodes",1)

        # Set job name
        job_name = "parsl.{0}.{1}".format(job_name,time.time())

        # Set script path
        script_path = "{0}/{1}.submit".format(self.config["execution"]["block"].get("script_dir",'./.scripts'),
                                              job_name)
        script_path = os.path.abspath(script_path)
        # Set executable script
        userscript_path = "{0}/{1}.script".format(self.config["execution"]["block"].get("script_dir",'./.scripts'),
                                                  job_name)
        userscript_path = os.path.abspath(userscript_path)


        # Calculate nodes
        nodes = self.config["execution"]["block"].get("nodes", 1)

        env = self.config["execution"]["block"].get('environment', {})
        env["JOBNAME"] = job_name
        for key, value in env.items():
            # To escape literal quote marks, double them
            # See: http://research.cs.wisc.edu/htcondor/manual/v8.6/condor_submit.html
            try:
                env[key] = "'{}'".format(value.replace("'", '"').replace('"', '""'))
            except AttributeError:
                pass

        job_config = {}
        job_config["job_name"] = job_name
        job_config["submit_script_dir"] = self.channel.script_dir
        job_config["project"] = self.config["execution"]["block"]["options"].get("project", "")
        job_config["nodes"] = nodes
        job_config["condor_overrides"] = self.config["execution"]["block"]["options"].get("overrides", '')
        job_config["worker_setup"] = self.config["execution"]["block"]["options"].get("workerSetup", '')
        job_config["user_script"] = cmd_string
        job_config["tasks_per_node"] =  1
        job_config["requirements"] = self.config["execution"]["block"]["options"].get("requirements", "")
        job_config["environment"] = ' '.join(['{}={}'.format(key, value) for key, value in env.items()])

        # Move the user script
        # This is where the cmd_string should be wrapped by the launchers.
        with open(userscript_path, 'w') as f:
            f.write(job_config["worker_setup"] + '\n' + cmd_string)

        user_script_path = self.channel.push_file(userscript_path, self.channel.script_dir)
        job_config["input_files"] = user_script_path
        job_config["job_script"] = os.path.basename(user_script_path)

        # Construct and move the submit script
        ret = self._write_submit_script(template_string, script_path, job_name, job_config)
        channel_script_path = self.channel.push_file(script_path, self.channel.script_dir)

        cmd = "condor_submit {0}".format(channel_script_path)
        retcode, stdout, stderr = self.channel.execute_wait(cmd, 3, envs=self.config['execution']['environment'])
        logger.debug ("Retcode:%s STDOUT:%s STDERR:%s", retcode,
                      stdout.strip(), stderr.strip())

        job_id = []

        if retcode == 0 :
            for line in stdout.split('\n'):
                if re.match('^[0-9]', line) is not None:
                    cluster = line.split(" ")[5]
                    # We know the first job id ("process" in condor terms) within a
                    # cluster is 0 and we know the total number of jobs from
                    # condor_submit, so we use some list comprehensions to expand
                    # the condor_submit output into job IDs
                    # e.g., ['118907.0', '118907.1', '118907.2', '118907.3', '118907.4', '118908.0']
                    processes = [str(x) for x in range(0,int(line[0]))]
                    job_id += [cluster + process for process in processes]

            self._add_resource(job_id)
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
        cmd = "condor_rm {0}".format(job_id_list)
        retcode, stdout, stderr = self.channel.execute_wait(cmd, 3, envs=self.config['execution']['environment'])
        rets = None
        if retcode == 0 :
            for jid in job_ids:
                self.resources[jid]['status'] = 'CANCELLED'
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

    def _add_resource(self, job_id):
        for jid in job_id:
            self.resources[jid] = {
                'status': 'PENDING',
                'size': 1
            }
        return True

if __name__ == "__main__" :

    print("None")
