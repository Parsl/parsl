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
from balsam.api import Job, App, BatchJob, Site, site_config


logger = logging.getLogger(__name__)

translate_table = {
    'QUEUED': JobState.PENDING,
    'STARTING': JobState.PENDING,
    'RUNNING': JobState.RUNNING,
    'EXITING': JobState.COMPLETED,
    'KILLING': JobState.COMPLETED
}


class BalsamProvider(ClusterProvider, RepresentationMixin):
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
    from multiprocessing import Condition

    lock = Condition()
    batchjobs = []

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
        self.queue = queue  # ??
        self.scheduler_options = scheduler_options
        self.worker_init = worker_init
        self.nodes_per_block = nodes_per_block
        self.walltime = walltime

        # How to get these into the provider?
        self.siteid = 1
        self.project = 'local'
        self.tags = {}

    def _status(self):
        """ Internal: Do not call. Returns the status list for a list of job_ids

        Args:
              self

        Returns:
              [status...] : Status list of all jobs
        """
        statuss = []
        try:
            self.lock.acquire()

            for batchjob in self.batchjobs:
                batchjob.refresh_from_db()
                statuss += [batchjob.state]

            return statuss
        except:
            self.lock.release()

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

        # Create BatchJob
        # NOTES: Convert walltime from incoming string to int for batchjob
        #
        try:
            self.lock.acquire()

            batchjob = BatchJob(
                num_nodes=self.nodes_per_block,
                wall_time_min=self.walltime,
                job_mode='mpi',
                queue=self.queue,
                site_id=self.siteid,
                project=self.project,
                filter_tags=self.tags
            )
            batchjob.save()
            self.batchjobs += [batchjob]
        finally:
            self.lock.release()

    def cancel(self, job_ids):
        """ Cancels the jobs specified by a list of job ids

        Args:
        job_ids : [<job_id> ...]

        Returns :
        [True/False...] : If the cancel operation fails the entire list will be False.
        """

        try:
            self.lock.acquire()
            for batchjob in self.batchjobs:
                batchjob.state = "pending_deletion"
                batchjob.save()
        finally:
            self.lock.release()

    @property
    def status_polling_interval(self):
        return 60
