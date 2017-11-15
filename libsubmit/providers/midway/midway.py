import os
import logging
import subprocess
from string import Template
from libsubmit.providers.provider_base import ExecutionProvider
from libsubmit.exec_utils import execute_wait
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

class Midway(ExecutionProvider):

    def __init__ (self, config):

        self.sitename = config['site']
        self.config   = config

        default = '~/.ipython/profile_default/security/ipcontroller-engine.json'
        self.engine_file = self.config.get('engine.json',
                                           os.path.expanduser(default))
        try:
            with open (self.engine_file, 'r') as f:
                self.config['engine_json'] = f.read()
        except OSError as e:
            logger.error("Could not open engine_json : ", self.engine_file)
            raise e

        self.resources = []

        logger.debug("Config : %s" % self.config)

        for engine in range(0, config["min_engines"]) :
            self.scale_out(config["min_engines"], 1)

        #print(self.config['engine_json'])


    def submit (self, *args, **kwargs):
        submit_template = None
        print("Not implemented")
        logger.debug("Submit job")
        raise NotImplementedError

    def scale_out (self, size, name=None):
        from datetime import datetime

        ipengine_json = None
        with open( os.path.expanduser("~/.ipython/profile_default/security/ipcontroller-engine.json"), 'r') as f:
            ipengine_json = f.read()

        job_name = "midway.parsl_auto.{0}".format(datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))
        script_name = job_name + ".submit"
        submit_script = None

        with open(os.path.join(os.path.dirname(__file__), './midway.template.submit'), 'r') as f:
            submit_script = Template(f.read()).safe_substitute(**self.config, nodes=1,
                                                               jobname=job_name,
                                                               ipengine_json=ipengine_json)

        with open(script_name, 'w') as f:
            f.write(submit_script)

        retcode, stdout, stderr = execute_wait("sbatch {0}".format(script_name), 1)
        print ("retcode : ", retcode)
        print ("stdout  : ", stdout)
        print ("stderr  : ", stderr)
        if retcode == 0 :
            for line in stdout.split('\n'):
                if line.startswith("Submitted batch job"):
                    job_id = line.split("Submitted batch job")[1]
                    self.resources.extend([{'job_id' : job_id.strip(),
                                            'status' : 'submitted',
                                            'size'   : size }])
        else:
            print("Submission of command to scale_out failed")


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


    def _get_job_status(self, job_id):
        retcode, stdout, stderr = execute_wait("squeue {0}".format(script_name), 1)
        print("Stdout : ", stdout)

    def status (self):

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
