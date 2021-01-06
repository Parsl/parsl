import logging
import os
import time

from parsl.channels import LocalChannel
from parsl.launchers import AprunLauncher
from parsl.providers.provider_base import JobState, JobStatus
from parsl.providers.torque.template import template_string
from parsl.providers.cluster_provider import ClusterProvider
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)

# From the man pages for qstat for PBS/Torque systems
translate_table = {
    'B': JobState.RUNNING,  # This state is returned for running array jobs
    'R': JobState.RUNNING,
    'C': JobState.COMPLETED,  # Completed after having run
    'E': JobState.COMPLETED,  # Exiting after having run
    'H': JobState.HELD,  # Held
    'Q': JobState.PENDING,  # Queued, and eligible to run
    'W': JobState.PENDING,  # Job is waiting for it's execution time (-a option) to be reached
    'S': JobState.HELD  # Suspended
}


class TorqueProvider(ClusterProvider, RepresentationMixin):
    """Torque Execution Provider

    This provider uses sbatch to submit, squeue for status, and scancel to cancel
    jobs. The sbatch script to be used is created from a template file in this
    same module.

    Parameters
    ----------
    channel : Channel
        Channel for accessing this provider. Possible channels include
        :class:`~parsl.channels.LocalChannel` (the default),
        :class:`~parsl.channels.SSHChannel`, or
        :class:`~parsl.channels.SSHInteractiveLoginChannel`.
    account : str
        Account the job will be charged against.
    queue : str
        Torque queue to request blocks from.
    nodes_per_block : int
        Nodes to provision per block.
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
    scheduler_options : str
        String to prepend to the #PBS blocks in the submit script to the scheduler.
        WARNING: scheduler_options should only be given #PBS strings, and should not have trailing newlines.
    worker_init : str
        Command to be run before starting a worker, such as 'module load Anaconda; source activate env'.
    launcher : Launcher
        Launcher for this provider. Possible launchers include
        :class:`~parsl.launchers.AprunLauncher` (the default), or
        :class:`~parsl.launchers.SingleNodeLauncher`,

    """
    def __init__(self,
                 channel=LocalChannel(),
                 account=None,
                 queue=None,
                 scheduler_options='',
                 worker_init='',
                 nodes_per_block=1,
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=1,
                 parallelism=1,
                 launcher=AprunLauncher(),
                 walltime="00:20:00",
                 cmd_timeout=120):
        label = 'torque'
        super().__init__(label,
                         channel,
                         nodes_per_block,
                         init_blocks,
                         min_blocks,
                         max_blocks,
                         parallelism,
                         walltime,
                         launcher,
                         cmd_timeout=cmd_timeout)

        self.account = account
        self.queue = queue
        self.scheduler_options = scheduler_options
        self.worker_init = worker_init
        self.template_string = template_string

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

    def _status(self):
        ''' Internal: Do not call. Returns the status list for a list of job_ids

        Args:
              self

        Returns:
              [status...] : Status list of all jobs
        '''

        job_ids = list(self.resources.keys())
        job_id_list = ' '.join(self.resources.keys())

        jobs_missing = list(self.resources.keys())

        retcode, stdout, stderr = self.execute_wait("qstat {0}".format(job_id_list))
        for line in stdout.split('\n'):
            parts = line.split()
            if not parts or parts[0].upper().startswith('JOB') or parts[0].startswith('---'):
                continue
            job_id = parts[0]  # likely truncated
            for long_job_id in job_ids:
                if long_job_id.startswith(job_id):
                    logger.debug('coerced job_id %s -> %s', job_id, long_job_id)
                    job_id = long_job_id
                    break
            state = translate_table.get(parts[4], JobState.UNKNOWN)
            self.resources[job_id]['status'] = JobStatus(state)
            jobs_missing.remove(job_id)

        # squeue does not report on jobs that are not running. So we are filling in the
        # blanks for missing jobs, we might lose some information about why the jobs failed.
        for missing_job in jobs_missing:
            self.resources[missing_job]['status'] = JobStatus(JobState.COMPLETED)

    def submit(self, command, tasks_per_node, job_name="parsl.torque"):
        ''' Submits the command onto an Local Resource Manager job.
        Submit returns an ID that corresponds to the task that was just submitted.

        If tasks_per_node <  1 : ! This is illegal. tasks_per_node should be integer

        If tasks_per_node == 1:
             A single node is provisioned

        If tasks_per_node >  1 :
             tasks_per_node number of nodes are provisioned.

        Args:
             - command  :(String) Commandline invocation to be made on the remote side.
             - tasks_per_node (int) : command invocations to be launched per node

        Kwargs:
             - job_name (String): Name for job, must be unique

        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job

        '''

        # Set job name
        job_name = "parsl.{0}.{1}".format(job_name, time.time())

        # Set script path
        script_path = "{0}/{1}.submit".format(self.script_dir, job_name)
        script_path = os.path.abspath(script_path)

        logger.debug("Requesting nodes_per_block:%s tasks_per_node:%s", self.nodes_per_block,
                     tasks_per_node)

        job_config = {}
        # TODO : script_path might need to change to accommodate script dir set via channels
        job_config["submit_script_dir"] = self.channel.script_dir
        job_config["nodes"] = self.nodes_per_block
        job_config["task_blocks"] = self.nodes_per_block * tasks_per_node
        job_config["nodes_per_block"] = self.nodes_per_block
        job_config["tasks_per_node"] = tasks_per_node
        job_config["walltime"] = self.walltime
        job_config["scheduler_options"] = self.scheduler_options
        job_config["worker_init"] = self.worker_init
        job_config["user_script"] = command

        # Wrap the command
        job_config["user_script"] = self.launcher(command,
                                                  tasks_per_node,
                                                  self.nodes_per_block)

        logger.debug("Writing submit script")
        self._write_submit_script(self.template_string, script_path, job_name, job_config)

        channel_script_path = self.channel.push_file(script_path, self.channel.script_dir)

        submit_options = ''
        if self.queue is not None:
            submit_options = '{0} -q {1}'.format(submit_options, self.queue)
        if self.account is not None:
            submit_options = '{0} -A {1}'.format(submit_options, self.account)

        launch_cmd = "qsub {0} {1}".format(submit_options, channel_script_path)
        retcode, stdout, stderr = self.execute_wait(launch_cmd)

        job_id = None
        if retcode == 0:
            for line in stdout.split('\n'):
                if line.strip():
                    job_id = line.strip()
                    self.resources[job_id] = {'job_id': job_id, 'status': JobStatus(JobState.PENDING)}
        else:
            message = "Command '{}' failed with return code {}".format(launch_cmd, retcode)
            if (stdout is not None) and (stderr is not None):
                message += "\nstderr:{}\nstdout{}".format(stderr.strip(), stdout.strip())
            logger.error(message)

        return job_id

    def cancel(self, job_ids):
        ''' Cancels the jobs specified by a list of job ids

        Args:
        job_ids : [<job_id> ...]

        Returns :
        [True/False...] : If the cancel operation fails the entire list will be False.
        '''

        job_id_list = ' '.join(job_ids)
        retcode, stdout, stderr = self.execute_wait("qdel {0}".format(job_id_list))
        rets = None
        if retcode == 0:
            for jid in job_ids:
                self.resources[jid]['status'] = JobStatus(JobState.COMPLETED)  # Setting state to exiting
            rets = [True for i in job_ids]
        else:
            rets = [False for i in job_ids]

        return rets

    @property
    def status_polling_interval(self):
        return 60


if __name__ == "__main__":

    print("None")
