import os
import logging
import subprocess
import math
import time
import re
from string import Template
from libsubmit.execution_provider_base import ExecutionProvider
from libsubmit.slurm.template import template_string
from libsubmit.exec_utils import execute_wait

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
    '''

    def __repr__ (self):
        return "<Condor Execution Provider for site:{0} with channel:{1}>".format(self.sitename, self.channel)

    def __init__ (self, config):
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

        if not os.path.exists(self.config["execution"]["options"]["submit_script_dir"]):
            os.makedirs(self.config["execution"]["options"]["submit_script_dir"])

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

        # Condor job tracking is a bit inverted from SLURM. Assume all jobs are
        # completed unless we can condor_q them
        jobs_completed = list(self.resources.keys()) 

        retcode, stdout, stderr = self.channel.execute_wait("condor_q {0} -af:jr JobStatus".format(job_id_list), 3)

        '''
        Example output: 

        $ condor_q 34520408.5 -af:jr JobStatus
        34520408.5 1
        '''

        for line in stdout.split('\n'):
            parts = line.split()
            job_id = parts[0]
            try:
                jobs_completed.remove(job_id)
            except Exception as e:
                logger.error("Could not remove job id %s from list of job ids %s" % (job_id, jobs_completed))
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
              - template_string (string) : The template string to be used for the writing submit script
               script_filename (string) : Name of the submit script
              - job_name (string) : job name
              - configs (dict) : configs that get pushed into the template

        Returns:
              - True: on success

        Raises:
              SchedulerMissingArgs : If template is missing args
              ScriptPathError : Unable to write submit script out
        '''

        try:
            submit_script = Template(template_string).substitute(**configs,
                                                                 jobname=job_name)
            print("Script_filename : ", script_filename)
            with open(script_filename, 'w') as f:
                f.write(submit_script)

        except KeyError as e:
            logger.error("Missing keys for submit script : %s", e)
            raise(ep_error.SchedulerMissingArgs(e.args, self.sitename))

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

        blocksize = 1
        job_config["nodes"] = 1
        job_config["condor_overrides"] = job_config.get("condor_overrides", '')
        job_config["user_script"] = cmd_string

        ret = self._write_submit_script(template_string, script_path, job_name, job_config)

        retcode, stdout, stderr = execute_wait("condor_submit {0}".format(script_path), 3)
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
        retcode, stdout, stderr = execute_wait("condor_rm {0}".format(job_id_list), 3)
        rets = None
        if retcode == 0 :
            for jid in job_ids:
                self.resources[jid]['status'] = translate_table['CA'] # Setting state to cancelled
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

    print("None")
