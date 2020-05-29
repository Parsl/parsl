import logging
import os
import time

from parsl.channels import LocalChannel
from parsl.providers.cluster_provider import ClusterProvider
from parsl.providers.grid_engine.template import template_string
from parsl.launchers import SingleNodeLauncher
from parsl.providers.provider_base import JobState, JobStatus
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)

translate_table = {
    'qw': JobState.PENDING,
    'hqw': JobState.PENDING,
    'hrwq': JobState.PENDING,
    'r': JobState.RUNNING,
    's': JobState.FAILED,  # obsuspended
    'ts': JobState.FAILED,
    't': JobState.FAILED,  # Suspended by alarm
    'eqw': JobState.FAILED,  # Error states
    'ehqw': JobState.FAILED,  # ..
    'ehrqw': JobState.FAILED,  # ..
    'd': JobState.COMPLETED,
    'dr': JobState.COMPLETED,
    'dt': JobState.COMPLETED,
    'drt': JobState.COMPLETED,
    'ds': JobState.COMPLETED,
    'drs': JobState.COMPLETED,
}


class GridEngineProvider(ClusterProvider, RepresentationMixin):
    """A provider for the Grid Engine scheduler.

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
    parallelism : float
        Ratio of provisioned task slots to active tasks. A parallelism value of 1 represents aggressive
        scaling where as many resources as possible are used; parallelism close to 0 represents
        the opposite situation in which as few resources as possible (i.e., min_blocks) are used.
    walltime : str
        Walltime requested per block in HH:MM:SS.
    scheduler_options : str
        String to prepend to the #$$ blocks in the submit script to the scheduler.
    worker_init : str
        Command to be run before starting a worker, such as 'module load Anaconda; source activate env'.
    launcher : Launcher
        Launcher for this provider. Possible launchers include
        :class:`~parsl.launchers.SingleNodeLauncher` (the default),
    """

    def __init__(self,
                 channel=LocalChannel(),
                 nodes_per_block=1,
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=1,
                 parallelism=1,
                 walltime="00:10:00",
                 scheduler_options='',
                 worker_init='',
                 launcher=SingleNodeLauncher()):
        label = 'grid_engine'
        super().__init__(label,
                         channel,
                         nodes_per_block,
                         init_blocks,
                         min_blocks,
                         max_blocks,
                         parallelism,
                         walltime,
                         launcher)
        self.scheduler_options = scheduler_options
        self.worker_init = worker_init

        if launcher in ['srun', 'srun_mpi']:
            logger.warning("Use of {} launcher is usually appropriate for Slurm providers. "
                           "Recommended options include 'single_node' or 'aprun'.".format(launcher))

    def get_configs(self, command, tasks_per_node):
        """Compose a dictionary with information for writing the submit script."""

        logger.debug("Requesting one block with {} nodes per block and {} tasks per node".format(
            self.nodes_per_block, tasks_per_node))

        job_config = {}
        job_config["submit_script_dir"] = self.channel.script_dir
        job_config["nodes"] = self.nodes_per_block
        job_config["walltime"] = self.walltime
        job_config["scheduler_options"] = self.scheduler_options
        job_config["worker_init"] = self.worker_init
        job_config["user_script"] = command

        job_config["user_script"] = self.launcher(command,
                                                  tasks_per_node,
                                                  self.nodes_per_block)
        return job_config

    def submit(self, command, tasks_per_node, job_name="parsl.sge"):
        ''' The submit method takes the command string to be executed upon
        instantiation of a resource most often to start a pilot (such as IPP engine
        or even Swift-T engines).

        Args :
             - command (str) : The bash command string to be executed.
             - tasks_per_node (int) : command invocations to be launched per node

        KWargs:
             - job_name (str) : Human friendly name to be assigned to the job request

        Returns:
             - A job identifier, this could be an integer, string etc

        Raises:
             - ExecutionProviderException or its subclasses
        '''

        # Set job name
        job_name = "{0}.{1}".format(job_name, time.time())

        # Set script path
        script_path = "{0}/{1}.submit".format(self.script_dir, job_name)
        script_path = os.path.abspath(script_path)

        job_config = self.get_configs(command, tasks_per_node)

        logger.debug("Writing submit script")
        self._write_submit_script(template_string, script_path, job_name, job_config)

        channel_script_path = self.channel.push_file(script_path, self.channel.script_dir)
        cmd = "qsub -terse {0}".format(channel_script_path)
        retcode, stdout, stderr = self.execute_wait(cmd, 10)

        if retcode == 0:
            for line in stdout.split('\n'):
                job_id = line.strip()
                if not job_id:
                    continue
                self.resources[job_id] = {'job_id': job_id, 'status': JobStatus(JobState.PENDING)}
                return job_id
        else:
            print("[WARNING!!] Submission of command to scale_out failed")
            logger.error("Retcode:%s STDOUT:%s STDERR:%s", retcode, stdout.strip(), stderr.strip())

    def _status(self):
        ''' Get the status of a list of jobs identified by the job identifiers
        returned from the submit request.

        Returns:
             - A list of JobStatus objects corresponding to each job_id in the job_ids list.

        Raises:
             - ExecutionProviderException or its subclasses

        '''

        cmd = "qstat"

        retcode, stdout, stderr = self.execute_wait(cmd)

        # Execute_wait failed. Do no update
        if retcode != 0:
            return

        jobs_missing = list(self.resources.keys())
        for line in stdout.split('\n'):
            parts = line.split()
            if parts and parts[0].lower().lower() != 'job-id' \
                    and not parts[0].startswith('----'):
                job_id = parts[0]
                status = translate_table.get(parts[4].lower(), JobState.UNKNOWN)
                if job_id in self.resources:
                    self.resources[job_id]['status'] = status
                    jobs_missing.remove(job_id)

        # Filling in missing blanks for jobs that might have gone missing
        # we might lose some information about why the jobs failed.
        for missing_job in jobs_missing:
            self.resources[missing_job]['status'] = JobStatus(JobState.COMPLETED)

    def cancel(self, job_ids):
        ''' Cancels the resources identified by the job_ids provided by the user.

        Args:
             - job_ids (list): A list of job identifiers

        Returns:
             - A list of status from cancelling the job which can be True, False

        Raises:
             - ExecutionProviderException or its subclasses
        '''

        job_id_list = ' '.join(job_ids)
        cmd = "qdel {}".format(job_id_list)
        retcode, stdout, stderr = self.execute_wait(cmd, 3)

        rets = None
        if retcode == 0:
            for jid in job_ids:
                self.resources[jid]['status'] = JobStatus(JobState.COMPLETED)
            rets = [True for i in job_ids]
        else:
            rets = [False for i in job_ids]

        return rets

    @property
    def status_polling_interval(self):
        return 60
