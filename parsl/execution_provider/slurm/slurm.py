import os
import logging
import subprocess
import math
from datetime import datetime
from string import Template
from parsl.execution_provider.execution_provider_base import ExecutionProvider
from parsl.execution_provider.slurm.template import template_string
import parsl.execution_provider.error as ep_error

logger = logging.getLogger(__name__)

def execute_wait (cmd, walltime):
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
        proc.wait(timeout=walltime)
        stdout = proc.stdout.read()
        stderr = proc.stderr.read()
        retcode = proc.returncode

    except Exception as e:
        print("Caught exception : {0}".format(e))
        logger.warn("Execution of command [%s] failed due to \n %s ",  (cmd, e))

    print("RunCommand Completed {0}".format(cmd))
    return (retcode, stdout.decode("utf-8"), stderr.decode("utf-8"))


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


class Slurm(ExecutionProvider):
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

        if not os.path.exists(self.config["execution"]["options"]["submit_script_dir"]):
            os.makedirs(self.config["execution"]["options"]["submit_script_dir"])

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

    ###########################################################################################################
    # Status
    ###########################################################################################################
    def _status(self):
        ''' Internal: Do not call. Returns the status list for a list of job_ids
        Args:
        self.
        job_ids : [<job_id> ...]

        Returns :
        [status...]
        '''

        #job_id_list  = ','.join([j['job_id'] for j in self.resources])

        job_id_list  = ','.join(self.resources.keys())

        jobs_missing = list(self.resources.keys())
        print("Jobs_missing : ", jobs_missing)

        retcode, stdout, stderr = execute_wait("squeue --job {0}".format(job_id_list), 3)
        for line in stdout.split('\n'):
            parts = line.split()
            if parts and parts[0] != 'JOBID' :
                print("Parts : ", parts)
                job_id = parts[0]
                status = translate_table.get(parts[4], 'UNKNOWN')
                self.resources[job_id]['status'] = status
                jobs_missing.remove(job_id)

        # We are filling in the blanks for missing jobs, we might lose some information
        # about why the jobs failed.
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
    # Submit
    ###########################################################################################################
    def _write_submit_script(self, template_string, script_filename, job_name, configs):
        '''
        Load the template string with config values and write the generated submit script to
        a submit script file.
        '''

        try:
            submit_script = Template(template_string).substitute(**configs,
                                                                 jobname=job_name)
            with open(script_filename, 'w') as f:
                f.write(submit_script)

        except KeyError as e:
            logger.error("Missing keys for submit script : %s", e)
            raise(ep_error.SchedulerMissingArgs(e.args, self.sitename))

        except IOError as e:
            logger.error("Failed writing to submit script: %s", script_filename)
            raise(ep_error.ScriptPathError(script_filename, e))

        return True

    def submit (self, cmd_string, blocksize):
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
             - blocksize   :(float)

        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job

        '''

        if self.current_blocksize >= self.config["execution"]["options"]["max_parallelism"]:
            logger.warn("[%s] at capacity, cannot add more blocks now", self.sitename)
            return None

        job_name = "midway.parsl_auto.{0}".format(datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))
        script_path = "{0}/job_name.submit".format(self.config["execution"]["options"]["submit_script_dir"])

        nodes = math.ceil(float(blocksize) / self.config["execution"]["options"]["tasks_per_node"])
        logger.debug("Requesting blocksize:%s tasks_per_node:%s nodes:%s", blocksize,
                     self.config["execution"]["options"]["tasks_per_node"],nodes)

        job_config = self.config["execution"]["options"]
        job_config["nodes"] = nodes
        job_config["slurm_overrides"] = job_config.get("slurm_overrides", '')
        job_config["user_script"] = cmd_string

        ret = self._write_submit_script(template_string, script_path, job_name, job_config)

        retcode, stdout, stderr = execute_wait("sbatch {0}".format(script_path), 3)
        logger.debug ("Retcode:%s STDOUT:%s STDERR:%s", retcode,
                      stdout.strip(), stderr.strip())

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
        retcode, stdout, stderr = execute_wait("scancel {0}".format(job_id_list), 3)
        rets = None
        if retcode == 0 :
            for jid in job_ids:
                self.resources[jid]['status'] = translate_table['CA'] # Setting state to cancelled
            rets = [True for i in job_ids]
        else:
            rets = [False for i in job_ids]

        return rets

    def scale_out (self, size, name=None):
        pass

    def scale_in (self, size):
        count = 0
        if not self.resources :
            print("No resources online, cannot scale down")

        else :
            for resource in self.resources[0:size]:
                print("Cancelling : ", resource['job_id'])
                retcode, stdout, stderr = execute_wait("scancel {0}".format(resource['job_id']), 3)
                print(retcode, stdout, stderr)

        return count

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
