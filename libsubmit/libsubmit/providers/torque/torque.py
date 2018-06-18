import logging
import os
import time
from string import Template

import libsubmit.error as ep_error
from libsubmit.providers.provider_base import ExecutionProvider
from libsubmit.providers.torque.template import template_string
# from libsubmit.utils import RepresentationMixin

logger = logging.getLogger(__name__)

# From the man pages for qstat for PBS/Torque systems
translate_table = {
    'R': 'RUNNING',
    'C': 'COMPLETED',  # Completed after having run
    'E': 'COMPLETED',  # Exiting after having run
    'H': 'HELD',  # Held
    'Q': 'PENDING',  # Queued, and eligible to run
    'W': 'PENDING',  # Job is waiting for it's execution time (-a option) to be reached
    'S': 'HELD'
}  # Suspended


class Torque(ExecutionProvider):
    """Torque Execution Provider

    This provider uses sbatch to submit, squeue for status, and scancel to cancel
    jobs. The sbatch script to be used is created from a template file in this
    same module.

    Parameters
    ----------
    channel : Channel
        Channel for accessing this provider. Possible channels include
        :class:`~libsubmit.channels.local.local.LocalChannel` (the default),
        :class:`~libsubmit.channels.ssh.ssh.SSHChannel`, or
        :class:`~libsubmit.channels.ssh_il.ssh_il.SSHInteractiveLoginChannel`.
    account : str
        Account the job will be charged against.
    queue : str
        Torque queue to request blocks from.
    label : str
        Label for this provider.
    script_dir : str
        Relative or absolute path to a directory where intermediate scripts are placed.
    nodes_per_block : int
        Nodes to provision per block.
    tasks_per_node : int
        Tasks to run per node.
    init_blocks : int
        Number of blocks to provision at the start of the run. Default is 1.
    min_blocks : int
        Minimum number of blocks to maintain. Default is 0.
    max_blocks : int
        Maximum number of blocks to maintain.
    parallelism : float
        Ratio of provisioned task slots to active tasks. A parallelism value of 1 represents aggressive
        scaling where as many resources as possible are used; parallelism close to 0 represents
        the opposite situation in which as few resources as possible (i.e., min_blocks) are used.
    walltime : str
        Walltime requested per block in HH:MM:SS.
    overrides : str
        String to prepend to the Torque submit script.
    """
    def __init__(self,
                 channel,
                 account,
                 queue=None,
                 overrides='',
                 label='torque',
                 script_dir='parsl_scripts',
                 nodes_per_block=1,
                 tasks_per_node=1,
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=100,
                 parallelism=1,
                 walltime="00:20:00"):
        self.channel = channel
        if self.channel is None:
            logger.error("Provider:Torque cannot be initialized without a channel")
            raise (ep_error.ChannelRequired(self.__class__.__name__, "Missing a channel to execute commands"))
        self.account = account
        self.queue = queue
        self.overrides = overrides
        self.label = label
        self.nodes_per_block = nodes_per_block
        self.tasks_per_node = tasks_per_node
        self.init_blocks = init_blocks
        self.min_blocks = min_blocks
        self.max_blocks = max_blocks
        self.parallelism = parallelism
        self.walltime = walltime
        self.provisioned_blocks = 0

        self.script_dir = script_dir
        if not os.path.exists(self.script_dir):
            os.makedirs(self.script_dir)

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

    def _status(self):
        ''' Internal: Do not call. Returns the status list for a list of job_ids

        Args:
              self

        Returns:
              [status...] : Status list of all jobs
        '''

        job_id_list = ' '.join(self.resources.keys())

        jobs_missing = list(self.resources.keys())

        retcode, stdout, stderr = self.channel.execute_wait("qstat {0}".format(job_id_list), 3)
        for line in stdout.split('\n'):
            parts = line.split()
            if not parts or parts[0].upper().startswith('JOB') or parts[0].startswith('---'):
                continue
            print(parts)
            job_id = parts[0]
            status = translate_table.get(parts[4], 'UNKNOWN')
            self.resources[job_id]['status'] = status
            jobs_missing.remove(job_id)

        # squeue does not report on jobs that are not running. So we are filling in the
        # blanks for missing jobs, we might lose some information about why the jobs failed.
        for missing_job in jobs_missing:
            if self.resources[missing_job]['status'] in ['PENDING', 'RUNNING']:
                self.resources[missing_job]['status'] = translate_table['E']

    def status(self, job_ids):
        '''  Get the status of a list of jobs identified by their ids.

        Args:
            - job_ids (List of ids) : List of identifiers for the jobs

        Returns:
            - List of status codes.

        '''
        self._status()
        return [self.resources[jid]['status'] for jid in job_ids]

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
            submit_script = Template(template_string).substitute(jobname=job_name, **configs)
            with open(script_filename, 'w') as f:
                f.write(submit_script)

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

        If tasks_per_node <  1 : ! This is illegal. tasks_per_node should be integer

        If tasks_per_node == 1:
             A single node is provisioned

        If tasks_per_node >  1 :
             tasks_per_node * blocksize number of nodes are provisioned.

        Args:
             - command  :(String) Commandline invocation to be made on the remote side.
             - blocksize   :(float)

        Kwargs:
             - job_name (String): Name for job, must be unique

        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job

        '''

        if self.provisioned_blocks >= self.max_blocks:
            logger.warn("[%s] at capacity, cannot add more blocks now", self.label)
            return None

        # Note: Fix this later to avoid confusing behavior.
        # We should always allocate blocks in integer counts of node_granularity
        if blocksize < self.nodes_per_block:
            blocksize = self.nodes_per_block

        # Set job name
        job_name = "parsl.{0}.{1}".format(job_name, time.time())

        # Set script path
        script_path = "{0}/{1}.submit".format(self.script_dir, job_name)
        script_path = os.path.abspath(script_path)

        logger.debug("Requesting blocksize:%s nodes_per_block:%s tasks_per_node:%s", blocksize, self.nodes_per_block,
                     self.tasks_per_node)

        job_config = self.config["execution"]["block"]["options"]
        # TODO : script_path might need to change to accommodate script dir set via channels
        job_config["submit_script_dir"] = self.channel.script_dir
        job_config["nodes"] = self.nodes_per_block
        job_config["task_blocks"] = self.nodes_per_block * self.tasks_per_node
        job_config["walltime"] = self.walltime
        job_config["overrides"] = self.overrides
        job_config["user_script"] = command

        logger.debug("Writing submit script")
        self._write_submit_script(template_string, script_path, job_name, job_config)

        channel_script_path = self.channel.push_file(script_path, self.channel.script_dir)

        submit_options = ''
        if self.queue is not None:
            submit_options = '{0} -q {1}'.format(submit_options, self.queue)
        if self.account is not None:
            submit_options = '{0} -A {1}'.format(submit_options, self.account)

        launch_cmd = "qsub {0} {1}".format(submit_options, channel_script_path)
        retcode, stdout, stderr = self.channel.execute_wait(launch_cmd, 3)

        job_id = None
        if retcode == 0:
            for line in stdout.split('\n'):
                if line.strip():
                    job_id = line.strip()
                    self.resources[job_id] = {'job_id': job_id, 'status': 'PENDING', 'blocksize': blocksize}
        else:
            logger.error("Retcode:%s STDOUT:%s STDERR:%s", retcode, stdout.strip(), stderr.strip())

        return job_id

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
        if retcode == 0:
            for jid in job_ids:
                self.resources[jid]['status'] = translate_table['E']  # Setting state to exiting
            rets = [True for i in job_ids]
        else:
            rets = [False for i in job_ids]

        return rets

    @property
    def scaling_enabled(self):
        return True

    @property
    def current_capacity(self):
        ''' Returns the number of currently provisioned blocks.
        This may need to return more information in the futures :
        { minsize, maxsize, current_requested }
        '''
        return self.provisioned_blocks

    def _test_add_resource(self, job_id):
        self.resources.extend([{'job_id': job_id, 'status': 'PENDING', 'size': 1}])
        return True


if __name__ == "__main__":

    print("None")
