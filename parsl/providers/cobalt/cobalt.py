import logging
import os
import time

from parsl.providers.error import ScaleOutFailed
from parsl.channels import LocalChannel
from parsl.launchers import AprunLauncher
from parsl.providers.cobalt.template import template_string
from parsl.providers.cluster_provider import ClusterProvider
from parsl.providers.provider_base import JobState, JobStatus
from parsl.utils import RepresentationMixin, wtime_to_minutes

logger = logging.getLogger(__name__)

translate_table = {
    'QUEUED': JobState.PENDING,
    'STARTING': JobState.PENDING,
    'RUNNING': JobState.RUNNING,
    'EXITING': JobState.COMPLETED,
    'KILLING': JobState.COMPLETED
}


class CobaltProvider(ClusterProvider, RepresentationMixin):
    """ Cobalt Execution Provider

    This provider uses cobalt to submit (qsub), obtain the status of (qstat), and cancel (qdel)
    jobs. Theo script to be used is created from a template file in this
    same module.

    Parameters
    ----------
    channel : Channel
        Channel for accessing this provider. Possible channels include
        :class:`~parsl.channels.LocalChannel` (the default),
        :class:`~parsl.channels.SSHChannel`, or
        :class:`~parsl.channels.SSHInteractiveLoginChannel`.
    nodes_per_block : int
        Nodes to provision per block.
    min_blocks : int
        Minimum number of blocks to maintain.
    max_blocks : int
        Maximum number of blocks to maintain.
    walltime : str
        Walltime requested per block in HH:MM:SS.
    account : str
        Account that the job will be charged against.
    queue : str
        Torque queue to request blocks from.
    scheduler_options : str
        String to prepend to the submit script to the scheduler.
    worker_init : str
        Command to be run before starting a worker, such as 'module load Anaconda; source activate env'.
    launcher : Launcher
        Launcher for this provider. Possible launchers include
        :class:`~parsl.launchers.AprunLauncher` (the default) or,
        :class:`~parsl.launchers.SingleNodeLauncher`
    """
    def __init__(self,
                 channel=LocalChannel(),
                 nodes_per_block=1,
                 init_blocks=0,
                 min_blocks=0,
                 max_blocks=1,
                 parallelism=1,
                 walltime="00:10:00",
                 account=None,
                 queue=None,
                 scheduler_options='',
                 worker_init='',
                 launcher=AprunLauncher(),
                 cmd_timeout=10):
        label = 'cobalt'
        super().__init__(label,
                         channel=channel,
                         nodes_per_block=nodes_per_block,
                         init_blocks=init_blocks,
                         min_blocks=min_blocks,
                         max_blocks=max_blocks,
                         parallelism=parallelism,
                         walltime=walltime,
                         launcher=launcher,
                         cmd_timeout=cmd_timeout)

        self.account = account
        self.queue = queue
        self.scheduler_options = scheduler_options
        self.worker_init = worker_init

    def _status(self):
        """ Internal: Do not call. Returns the status list for a list of job_ids

        Args:
              self

        Returns:
              [status...] : Status list of all jobs
        """

        jobs_missing = list(self.resources.keys())

        retcode, stdout, stderr = self.execute_wait("qstat -u $USER")

        # Execute_wait failed. Do no update.
        if retcode != 0:
            return

        for line in stdout.split('\n'):
            if line.startswith('='):
                continue

            parts = line.upper().split()
            if parts and parts[0] != 'JOBID':
                job_id = parts[0]

                if job_id not in self.resources:
                    continue

                status = translate_table.get(parts[4], JobState.UNKNOWN)

                self.resources[job_id]['status'] = JobStatus(status)
                jobs_missing.remove(job_id)

        # squeue does not report on jobs that are not running. So we are filling in the
        # blanks for missing jobs, we might lose some information about why the jobs failed.
        for missing_job in jobs_missing:
            self.resources[missing_job]['status'] = JobStatus(JobState.COMPLETED)

    def submit(self, command, tasks_per_node, job_name="parsl.cobalt"):
        """ Submits the command onto an Local Resource Manager job of parallel elements.
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

        """

        if self.provisioned_blocks >= self.max_blocks:
            logger.warning("[%s] at capacity, cannot add more blocks now", self.label)
            return None

        account_opt = '-A {}'.format(self.account) if self.account is not None else ''

        job_name = "parsl.{0}.{1}".format(job_name, time.time())

        script_path = "{0}/{1}.submit".format(self.script_dir, job_name)
        script_path = os.path.abspath(script_path)

        job_config = {}
        job_config["scheduler_options"] = self.scheduler_options
        job_config["worker_init"] = self.worker_init

        logger.debug("Requesting nodes_per_block:%s tasks_per_node:%s",
                     self.nodes_per_block, tasks_per_node)

        # Wrap the command
        job_config["user_script"] = self.launcher(command, tasks_per_node, self.nodes_per_block)

        queue_opt = '-q {}'.format(self.queue) if self.queue is not None else ''

        logger.debug("Writing submit script")
        self._write_submit_script(template_string, script_path, job_name, job_config)

        channel_script_path = self.channel.push_file(script_path, self.channel.script_dir)

        command = 'qsub -n {0} {1} -t {2} {3} {4}'.format(
            self.nodes_per_block, queue_opt, wtime_to_minutes(self.walltime), account_opt, channel_script_path)
        logger.debug("Executing {}".format(command))

        retcode, stdout, stderr = self.execute_wait(command)

        # TODO : FIX this block
        if retcode != 0:
            logger.error("Failed command: {0}".format(command))
            logger.error("Launch failed stdout:\n{0} \nstderr:{1}\n".format(stdout, stderr))

        logger.debug("Retcode:%s STDOUT:%s STDERR:%s", retcode, stdout.strip(), stderr.strip())

        job_id = None

        if retcode == 0:
            # We should be getting only one line back
            job_id = stdout.strip()
            self.resources[job_id] = {'job_id': job_id, 'status': JobStatus(JobState.PENDING)}
        else:
            logger.error("Submission of command to scale_out failed: {0}".format(stderr))
            raise (ScaleOutFailed(self.__class__, "Request to submit job to local scheduler failed"))

        logger.debug("Returning job id : {0}".format(job_id))
        return job_id

    def cancel(self, job_ids):
        """ Cancels the jobs specified by a list of job ids

        Args:
        job_ids : [<job_id> ...]

        Returns :
        [True/False...] : If the cancel operation fails the entire list will be False.
        """

        job_id_list = ' '.join(job_ids)
        retcode, stdout, stderr = self.execute_wait("qdel {0}".format(job_id_list))
        rets = None
        if retcode == 0:
            for jid in job_ids:
                # ???
                # self.resources[jid]['status'] = translate_table['KILLING']  # Setting state to cancelled
                self.resources[jid]['status'] = JobStatus(JobState.COMPLETED)
            rets = [True for i in job_ids]
        else:
            rets = [False for i in job_ids]

        return rets

    @property
    def status_polling_interval(self):
        return 60
