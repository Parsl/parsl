import os
import time
import logging

from parsl.channels import LocalChannel
from parsl.launchers import SingleNodeLauncher
from parsl.providers.cluster_provider import ClusterProvider
from parsl.providers.lsf.template import template_string
from parsl.providers.provider_base import JobState, JobStatus
from parsl.utils import RepresentationMixin, wtime_to_minutes

logger = logging.getLogger(__name__)

translate_table = {
    'PEND': JobState.PENDING,
    'RUN': JobState.RUNNING,
    'DONE': JobState.COMPLETED,
    'EXIT': JobState.FAILED,  # (failed),
    'PSUSP': JobState.CANCELLED,
    'USUSP': JobState.CANCELLED,
    'SSUSP': JobState.CANCELLED,
}


class LSFProvider(ClusterProvider, RepresentationMixin):
    """LSF Execution Provider

    This provider uses sbatch to submit, squeue for status and scancel to cancel
    jobs. The sbatch script to be used is created from a template file in this
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
    init_blocks : int
        Number of blocks to request at the start of the run.
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
    project : str
        Project to which the resources must be charged
    scheduler_options : str
        String to prepend to the #SBATCH blocks in the submit script to the scheduler.
    worker_init : str
        Command to be run before starting a worker, such as 'module load Anaconda; source activate env'.
    cmd_timeout : int
        Seconds after which requests to the scheduler will timeout. Default: 120s
    launcher : Launcher
        Launcher for this provider. Possible launchers include
        :class:`~parsl.launchers.SingleNodeLauncher` (the default),
        :class:`~parsl.launchers.SrunLauncher`, or
        :class:`~parsl.launchers.AprunLauncher`
     move_files : Optional[Bool]: should files be moved? by default, Parsl will try to move files.
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
                 project=None,
                 cmd_timeout=120,
                 move_files=True,
                 launcher=SingleNodeLauncher()):
        label = 'LSF'
        super().__init__(label,
                         channel,
                         nodes_per_block,
                         init_blocks,
                         min_blocks,
                         max_blocks,
                         parallelism,
                         walltime,
                         cmd_timeout=cmd_timeout,
                         launcher=launcher)

        self.project = project
        self.move_files = move_files
        self.scheduler_options = scheduler_options
        self.worker_init = worker_init

    def _status(self):
        ''' Internal: Do not call. Returns the status list for a list of job_ids

        Args:
              self

        Returns:
              [status...] : Status list of all jobs
        '''
        job_id_list = ','.join(self.resources.keys())
        cmd = "bjobs {0}".format(job_id_list)

        retcode, stdout, stderr = super().execute_wait(cmd)
        # Execute_wait failed. Do no update
        if retcode != 0:
            logger.debug("Updating job status from {} failed with return code {}".format(self.label,
                                                                                         retcode))
            return

        jobs_missing = list(self.resources.keys())
        for line in stdout.split('\n'):
            parts = line.split()
            if parts and parts[0] != 'JOBID':
                job_id = parts[0]
                state = translate_table.get(parts[2], JobState.UNKNOWN)
                self.resources[job_id]['status'] = JobStatus(state)
                jobs_missing.remove(job_id)

        # squeue does not report on jobs that are not running. So we are filling in the
        # blanks for missing jobs, we might lose some information about why the jobs failed.
        for missing_job in jobs_missing:
            self.resources[missing_job]['status'] = JobStatus(JobState.COMPLETED)

    def submit(self, command, tasks_per_node, job_name="parsl.lsf"):
        """Submit the command as an LSF job.

        Parameters
        ----------
        command : str
            Command to be made on the remote side.
        tasks_per_node : int
            Command invocations to be launched per node
        job_name : str
            Name for the job (must be unique).
        Returns
        -------
        None or str
            If at capacity, returns None; otherwise, a string identifier for the job
        """

        if self.provisioned_blocks >= self.max_blocks:
            logger.warning("LSF provider '{}' is at capacity (no more blocks will be added)".format(self.label))
            return None

        job_name = "{0}.{1}".format(job_name, time.time())

        script_path = "{0}/{1}.submit".format(self.script_dir, job_name)
        script_path = os.path.abspath(script_path)

        logger.debug("Requesting one block with {} nodes".format(self.nodes_per_block))

        job_config = {}
        job_config["submit_script_dir"] = self.channel.script_dir
        job_config["nodes"] = self.nodes_per_block
        job_config["tasks_per_node"] = tasks_per_node
        job_config["walltime"] = wtime_to_minutes(self.walltime)
        job_config["scheduler_options"] = self.scheduler_options
        job_config["worker_init"] = self.worker_init
        job_config["project"] = self.project
        job_config["user_script"] = command

        # Wrap the command
        job_config["user_script"] = self.launcher(command,
                                                  tasks_per_node,
                                                  self.nodes_per_block)

        logger.debug("Writing submit script")
        self._write_submit_script(template_string, script_path, job_name, job_config)

        if self.move_files:
            logger.debug("moving files")
            channel_script_path = self.channel.push_file(script_path, self.channel.script_dir)
        else:
            logger.debug("not moving files")
            channel_script_path = script_path

        retcode, stdout, stderr = super().execute_wait("bsub {0}".format(channel_script_path))

        job_id = None
        if retcode == 0:
            for line in stdout.split('\n'):
                if line.lower().startswith("job") and "is submitted to" in line.lower():
                    job_id = line.split()[1].strip('<>')
                    self.resources[job_id] = {'job_id': job_id, 'status': JobStatus(JobState.PENDING)}
        else:
            logger.warning("Submission of command to scale_out failed")
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
        retcode, stdout, stderr = super().execute_wait("bkill {0}".format(job_id_list))
        rets = None
        if retcode == 0:
            for jid in job_ids:
                self.resources[jid]['status'] = translate_table['USUSP']  # Job suspended by user/admin
            rets = [True for i in job_ids]
        else:
            rets = [False for i in job_ids]

        return rets

    @property
    def status_polling_interval(self):
        return 60
