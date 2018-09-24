import logging
import os
import signal
import time

import libsubmit.error as ep_error
from libsubmit.channels import LocalChannel
from libsubmit.launchers import SingleNodeLauncher
from libsubmit.providers.provider_base import ExecutionProvider
from libsubmit.utils import RepresentationMixin

logger = logging.getLogger(__name__)

translate_table = {
    'PD': 'PENDING',
    'R': 'RUNNING',
    'CA': 'CANCELLED',
    'CF': 'PENDING',  # (configuring),
    'CG': 'RUNNING',  # (completing),
    'CD': 'COMPLETED',
    'F': 'FAILED',
    'TO': 'TIMEOUT',
    'NF': 'FAILED',  # (node failure),
    'RV': 'FAILED',  # (revoked) and
    'SE': 'FAILED'
}  # (special exit state


class LocalProvider(ExecutionProvider, RepresentationMixin):
    """ Local Execution Provider

    This provider is used to provide execution resources from the localhost.

    Parameters
    ----------

    min_blocks : int
        Minimum number of blocks to maintain.
    max_blocks : int
        Maximum number of blocks to maintain.
    parallelism : float
        Ratio of provisioned task slots to active tasks. A parallelism value of 1 represents aggressive
        scaling where as many resources as possible are used; parallelism close to 0 represents
        the opposite situation in which as few resources as possible (i.e., min_blocks) are used.
    """

    def __init__(self,
                 channel=LocalChannel(),
                 label='local',
                 script_dir='parsl_scripts',
                 tasks_per_node=1,
                 nodes_per_block=1,
                 launcher=SingleNodeLauncher(),
                 init_blocks=4,
                 min_blocks=0,
                 max_blocks=10,
                 walltime="00:15:00",
                 parallelism=1):
        self.channel = channel
        self.label = label
        if not os.path.exists(script_dir):
            os.makedirs(script_dir)
        self.script_dir = script_dir
        self.provisioned_blocks = 0
        self.nodes_per_block = nodes_per_block
        self.tasks_per_node = tasks_per_node
        self.launcher = launcher
        self.init_blocks = init_blocks
        self.min_blocks = min_blocks
        self.max_blocks = max_blocks
        self.parallelism = parallelism
        self.walltime = walltime

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

    def status(self, job_ids):
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

            if poll_code is None:
                self.resources[job_id]['status'] = 'RUNNING'
            elif poll_code == 0 and self.resources[job_id]['status'] != 'RUNNING':
                self.resources[job_id]['status'] = 'COMPLETED'
            elif poll_code < 0 and self.resources[job_id]['status'] != 'RUNNING':
                self.resources[job_id]['status'] = 'FAILED'

        return [self.resources[jid]['status'] for jid in job_ids]

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
            raise (ep_error.SchedulerMissingArgs(e.args, self.label))

        except IOError as e:
            logger.error("Failed writing to submit script: %s", script_filename)
            raise (ep_error.ScriptPathError(script_filename, e))

        return True

    def submit(self, command, blocksize, job_name="parsl.auto"):
        ''' Submits the command onto an Local Resource Manager job of blocksize parallel elements.
        Submit returns an ID that corresponds to the task that was just submitted.

        If tasks_per_node <  1:
             1/tasks_per_node is provisioned

        If tasks_per_node == 1:
             A single node is provisioned

        If tasks_per_node >  1 :
             tasks_per_node * blocksize number of nodes are provisioned.

        Args:
             - command  :(String) Commandline invocation to be made on the remote side.
             - blocksize   :(float) - Not really used for local

        Kwargs:
             - job_name (String): Name for job, must be unique

        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job

        '''

        job_name = "{0}.{1}".format(job_name, time.time())

        # Set script path
        script_path = "{0}/{1}.sh".format(self.script_dir, job_name)
        script_path = os.path.abspath(script_path)

        wrap_command = self.launcher(command, self.tasks_per_node, self.nodes_per_block)

        self._write_submit_script(wrap_command, script_path)

        job_id, proc = self.channel.execute_no_wait('bash {0}'.format(script_path), 3)
        self.resources[job_id] = {'job_id': job_id, 'status': 'RUNNING', 'blocksize': blocksize, 'proc': proc}

        return job_id

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


if __name__ == "__main__":

    print("Nothing here")
