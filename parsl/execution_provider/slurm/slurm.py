import os
import logging
import subprocess
from datetime import datetime
from string import Template
from parsl.execution_provider.execution_provider_base import ExecutionProvider
from parsl.execution_provider.slurm.template import template_string

logger = logging.getLogger(__name__)

def execute_wait (cmd, walltime):
    retcode = -1
    stdout = None
    stderr = None
    try :
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        proc.wait()
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

        self.sitename = config['site']
        # These are the defaults that will be overriden by the user
        self.config   = {"execution" :
                         {"options"  :
                          {"submit_script_dir" : ".scripts"}
                          }
                         }
        self.config.update(config)

        logger.debug("Writing submit scripts to : ", self.config["execution"]["options"]["submit_script_dir"])
        if not os.path.exists(self.config["execution"]["options"]["submit_script_dir"]):
            os.makedirs(self.config["execution"]["options"]["submit_script_dir"])

        self.resources = []
        print("Template : ", template_string)
        logger.debug("Slurm Config : %s" % self.config)

    ###########################################################################################################
    # Status
    ###########################################################################################################
    def _get_job_status(self, job_id):
        retcode, stdout, stderr = execute_wait("squeue {0}".format(script_name), 1)
        print("Stdout : ", stdout)

    def status (self, job_ids):
        ''' Docs pending
        '''

        job_id_list  = ','.join([j['job_id'] for j in self.resources])
        retcode, stdout, stderr = execute_wait("squeue --job {0}".format(job_id_list), 1)
        for line in stdout.split('\n'):
            parts = line.split()
            if parts and parts[0] != 'JOBID' :
                print("Parts : ", parts)
                job_id = parts[0]
                status = translate_table.get(parts[4], 'UNKNOWN')
                for job in self.resources:
                    if job['job_id'] == job_id :
                        job['status'] = status
        print(self.resources)

    ###########################################################################################################
    # Submit
    ###########################################################################################################
    def _write_submit_script(self, template_string, script_filename, job_name, configs):
        '''
        Load the template string with config values and write the generated submit script to
        a submit script file.
        '''
        try:
            submit_script = Template(template_string).safe_substitute(**self.config,
                                                                      jobname=job_name)
            with open(script_filename, 'w') as f:
                f.write(submit_script)

        except KeyError as e:
            logger.error("Missing keys for submit script : %s", e)
            return False
        except IOError as e:
            logger.error("Failed writing to submit script: %s", script_filename)
            logger.error("Error : %s", e)

        return True

    def submit (self, cmd_string, size):
        job_name = "midway.parsl_auto.{0}".format(datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))
        script_name = job_name + ".submit"
        submit_script = None

        job_config = self.config
        ret = self._write_submit_script(template_string, script_name, job_name, self.config)
        retcode, stdout, stderr = execute_wait("sbatch {0}".format(script_name), 1)
        print ("retcode : ", retcode)
        print ("stdout  : ", stdout)
        print ("stderr  : ", stderr)
        job_id = None

        if retcode == 0 :
            for line in stdout.split('\n'):
                if line.startswith("Submitted batch job"):
                    job_id = line.split("Submitted batch job")[1].strip()
                    self.resources.extend([{'job_id' : job_id,
                                            'status' : 'submitted',
                                            'size'   : size }])
        else:
            print("Submission of command to scale_out failed")

        return job_id

    ###########################################################################################################
    # Cancel
    ###########################################################################################################
    def cancel(self, job_ids):
        raise NotImplemented

    def scale_out (self, size, name=None):
        pass

    def scale_in (self, size):
        count = 0
        if not self.resources :
            print("No resources online, cannot scale down")

        else :
            for resource in self.resources[0:size]:
                print("Cancelling : ", resource['job_id'])
                retcode, stdout, stderr = execute_wait("scancel {0}".format(resource['job_id']), 1)
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

    conf = { "site" : "pool1",
             "queue" : "bigmem",
             "maxnodes" : 4,
             "walltime" : '00:04:00',
             "controller" : "10.50.181.1:50001" }

    pool1 = Midway(conf)
    pool1.scale_out(1)
    pool1.scale_out(1)
    print("Pool resources : ", pool1.resources)
    pool1.status()
    pool1.scale_in(1)
    pool1.scale_in(1)
