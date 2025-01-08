import logging
import math
import os
import re
import time
from typing import Any, Dict, Optional

import typeguard

from parsl.jobs.states import JobState, JobStatus
from parsl.launchers import SingleNodeLauncher
from parsl.launchers.base import Launcher
from parsl.providers.cluster_provider import ClusterProvider
from parsl.providers.errors import SubmitException
from parsl.providers.slurm.template import template_string
from parsl.utils import RepresentationMixin, wtime_to_minutes

logger = logging.getLogger(__name__)

# From https://slurm.schedmd.com/sacct.html#SECTION_JOB-STATE-CODES
sacct_translate_table = {
    'PENDING': JobState.PENDING,
    'RUNNING': JobState.RUNNING,
    'CANCELLED': JobState.CANCELLED,
    'COMPLETED': JobState.COMPLETED,
    'FAILED': JobState.FAILED,
    'NODE_FAIL': JobState.FAILED,
    'BOOT_FAIL': JobState.FAILED,
    'DEADLINE': JobState.TIMEOUT,
    'TIMEOUT': JobState.TIMEOUT,
    'REVOKED': JobState.FAILED,
    'OUT_OF_MEMORY': JobState.FAILED,
    'SUSPENDED': JobState.HELD,
    'PREEMPTED': JobState.TIMEOUT,
    'REQUEUED': JobState.PENDING
}

squeue_translate_table = {
    'PD': JobState.PENDING,
    'R': JobState.RUNNING,
    'CA': JobState.CANCELLED,
    'CF': JobState.PENDING,  # (configuring),
    'CG': JobState.RUNNING,  # (completing),
    'CD': JobState.COMPLETED,
    'F': JobState.FAILED,  # (failed),
    'TO': JobState.TIMEOUT,  # (timeout),
    'NF': JobState.FAILED,  # (node failure),
    'RV': JobState.FAILED,  # (revoked) and
    'SE': JobState.FAILED   # (special exit state)
}


class SlurmProvider(ClusterProvider, RepresentationMixin):
    """Slurm Execution Provider

    This provider uses sbatch to submit, sacct for status and scancel to cancel
    jobs. The sbatch script to be used is created from a template file in this
    same module.

    Parameters
    ----------
    partition : str
        Slurm partition to request blocks from. If unspecified or ``None``, no partition slurm directive will be specified.
    account : str
        Slurm account to which to charge resources used by the job. If unspecified or ``None``, the job will use the
        user's default account.
    qos : str
        Slurm queue to place job in. If unspecified or ``None``, no queue slurm directive will be specified.
    constraint : str
        Slurm job constraint, often used to choose cpu or gpu type. If unspecified or ``None``, no constraint slurm directive will be added.
    clusters : str
        Slurm cluster name, or comma seperated cluster list, used to choose between different clusters in a federated Slurm instance.
        If unspecified or ``None``, no slurm directive for clusters will be added.
    nodes_per_block : int
        Nodes to provision per block.
    cores_per_node : int
        Specify the number of cores to provision per node. If set to None, executors
        will assume all cores on the node are available for computation. Default is None.
    mem_per_node : int
        Specify the real memory to provision per node in GB. If set to None, no
        explicit request to the scheduler will be made. Default is None.
    init_blocks : int
        Number of blocks to provision at the start of the run. Default is 1.
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
        String to prepend to the #SBATCH blocks in the submit script to the scheduler.
    regex_job_id : str
        The regular expression used to extract the job ID from the ``sbatch`` standard output.
        The default is ``r"Submitted batch job (?P<id>\\S*)"``, where ``id`` is the regular expression
        symbolic group for the job ID.
    worker_init : str
        Command to be run before starting a worker, such as 'module load Anaconda; source activate env'.
    exclusive : bool (Default = True)
        Requests nodes which are not shared with other running jobs.
    launcher : Launcher
        Launcher for this provider. Possible launchers include
        :class:`~parsl.launchers.SingleNodeLauncher` (the default),
        :class:`~parsl.launchers.SrunLauncher`, or
        :class:`~parsl.launchers.AprunLauncher`
    """

    @typeguard.typechecked
    def __init__(self,
                 partition: Optional[str] = None,
                 account: Optional[str] = None,
                 qos: Optional[str] = None,
                 constraint: Optional[str] = None,
                 clusters: Optional[str] = None,
                 nodes_per_block: int = 1,
                 cores_per_node: Optional[int] = None,
                 mem_per_node: Optional[int] = None,
                 init_blocks: int = 1,
                 min_blocks: int = 0,
                 max_blocks: int = 1,
                 parallelism: float = 1,
                 walltime: str = "00:10:00",
                 scheduler_options: str = '',
                 regex_job_id: str = r"Submitted batch job (?P<id>\S*)",
                 worker_init: str = '',
                 cmd_timeout: int = 10,
                 exclusive: bool = True,
                 launcher: Launcher = SingleNodeLauncher()):
        label = 'slurm'
        super().__init__(label,
                         nodes_per_block,
                         init_blocks,
                         min_blocks,
                         max_blocks,
                         parallelism,
                         walltime,
                         cmd_timeout=cmd_timeout,
                         launcher=launcher)

        self.partition = partition
        self.cores_per_node = cores_per_node
        self.mem_per_node = mem_per_node
        self.exclusive = exclusive
        self.account = account
        self.qos = qos
        self.constraint = constraint
        self.clusters = clusters
        self.scheduler_options = scheduler_options + '\n'
        if exclusive:
            self.scheduler_options += "#SBATCH --exclusive\n"
        if partition:
            self.scheduler_options += "#SBATCH --partition={}\n".format(partition)
        if account:
            self.scheduler_options += "#SBATCH --account={}\n".format(account)
        if qos:
            self.scheduler_options += "#SBATCH --qos={}\n".format(qos)
        if constraint:
            self.scheduler_options += "#SBATCH --constraint={}\n".format(constraint)
        if clusters:
            self.scheduler_options += "#SBATCH --clusters={}\n".format(clusters)

        self.regex_job_id = regex_job_id
        self.worker_init = worker_init + '\n'
        # Check if sacct works and if not fall back to squeue
        cmd = "sacct -X"
        logger.debug("Executing %s", cmd)
        retcode, stdout, stderr = self.execute_wait(cmd)
        # If sacct fails it should return retcode=1 stderr="Slurm accounting storage is disabled"
        logger.debug(f"sacct returned retcode={retcode} stderr={stderr}")
        if retcode == 0:
            logger.debug("using sacct to get job status")
            _cmd = "sacct"
            # Add clusters option to sacct if provided
            if self.clusters:
                _cmd += f" --clusters={self.clusters}"
            # Using state%20 to get enough characters to not truncate output
            # of the state. Without output can look like "<job_id>     CANCELLED+"
            self._cmd = _cmd + " -X --noheader --format=jobid,state%20 --job '{0}'"
            self._translate_table = sacct_translate_table
        else:
            logger.debug(f"sacct failed with retcode={retcode}")
            logger.debug("falling back to using squeue to get job status")
            _cmd = "squeue"
            # Add clusters option to squeue if provided
            if self.clusters:
                _cmd += f" --clusters={self.clusters}"
            self._cmd = _cmd + " --noheader --format='%i %t' --job '{0}'"
            self._translate_table = squeue_translate_table

    def _status(self):
        '''Returns the status list for a list of job_ids

        Args:
              self

        Returns:
              [status...] : Status list of all jobs
        '''
        job_id_list = ','.join(
            [jid for jid, job in self.resources.items() if not job['status'].terminal]
        )
        if not job_id_list:
            logger.debug('No active jobs, skipping status update')
            return

        cmd = self._cmd.format(job_id_list)
        logger.debug("Executing %s", cmd)
        retcode, stdout, stderr = self.execute_wait(cmd)
        logger.debug("sacct/squeue returned %s %s", stdout, stderr)

        # Execute_wait failed. Do no update
        if retcode != 0:
            logger.warning("sacct/squeue failed with non-zero exit code {}".format(retcode))
            return

        jobs_missing = set(self.resources.keys())
        for line in stdout.split('\n'):
            if not line:
                # Blank line
                continue
            # Sacct includes extra information in some outputs
            # For example "<job_id> CANCELLED by <user_id>"
            # This splits and ignores anything past the first two unpacked values
            job_id, slurm_state, *ignore = line.split()
            if slurm_state not in self._translate_table:
                logger.warning(f"Slurm status {slurm_state} is not recognized")
            status = self._translate_table.get(slurm_state, JobState.UNKNOWN)
            logger.debug("Updating job {} with slurm status {} to parsl state {!s}".format(job_id, slurm_state, status))
            self.resources[job_id]['status'] = JobStatus(status,
                                                         stdout_path=self.resources[job_id]['job_stdout_path'],
                                                         stderr_path=self.resources[job_id]['job_stderr_path'])
            jobs_missing.remove(job_id)

        # sacct can get job info after jobs have completed so this path shouldn't be hit
        # squeue does not report on jobs that are not running. So we are filling in the
        # blanks for missing jobs, we might lose some information about why the jobs failed.
        for missing_job in jobs_missing:
            logger.debug("Updating missing job {} to completed status".format(missing_job))
            self.resources[missing_job]['status'] = JobStatus(
                JobState.COMPLETED, stdout_path=self.resources[missing_job]['job_stdout_path'],
                stderr_path=self.resources[missing_job]['job_stderr_path'])

    def submit(self, command: str, tasks_per_node: int, job_name="parsl.slurm") -> str:
        """Submit the command as a slurm job.

        Parameters
        ----------
        command : str
            Command to be made on the remote side.
        tasks_per_node : int
            Command invocations to be launched per node
        job_name : str
            Name for the job
        Returns
        -------
        job id : str
            A string identifier for the job
        """

        scheduler_options = self.scheduler_options
        worker_init = self.worker_init
        if self.mem_per_node is not None:
            scheduler_options += '#SBATCH --mem={}g\n'.format(self.mem_per_node)
            worker_init += 'export PARSL_MEMORY_GB={}\n'.format(self.mem_per_node)
        if self.cores_per_node is not None:
            cpus_per_task = math.floor(self.cores_per_node / tasks_per_node)
            scheduler_options += '#SBATCH --cpus-per-task={}'.format(cpus_per_task)
            worker_init += 'export PARSL_CORES={}\n'.format(cpus_per_task)

        job_name = "{0}.{1}".format(job_name, time.time())

        assert self.script_dir, "Expected script_dir to be set"
        script_path = os.path.join(self.script_dir, job_name)
        script_path = os.path.abspath(script_path)
        job_stdout_path = script_path + ".stdout"
        job_stderr_path = script_path + ".stderr"

        logger.debug("Requesting one block with {} nodes".format(self.nodes_per_block))

        job_config: Dict[str, Any] = {}
        job_config["submit_script_dir"] = self.script_dir
        job_config["nodes"] = self.nodes_per_block
        job_config["tasks_per_node"] = tasks_per_node
        job_config["walltime"] = wtime_to_minutes(self.walltime)
        job_config["scheduler_options"] = scheduler_options
        job_config["worker_init"] = worker_init
        job_config["user_script"] = command
        job_config["job_stdout_path"] = job_stdout_path
        job_config["job_stderr_path"] = job_stderr_path

        # Wrap the command
        job_config["user_script"] = self.launcher(command,
                                                  tasks_per_node,
                                                  self.nodes_per_block)

        logger.debug("Writing submit script")
        self._write_submit_script(template_string, script_path, job_name, job_config)

        retcode, stdout, stderr = self.execute_wait("sbatch {0}".format(script_path))

        if retcode == 0:
            for line in stdout.split('\n'):
                match = re.match(self.regex_job_id, line)
                if match:
                    job_id = match.group("id")
                    self.resources[job_id] = {'job_id': job_id,
                                              'status': JobStatus(JobState.PENDING),
                                              'job_stdout_path': job_stdout_path,
                                              'job_stderr_path': job_stderr_path,
                                              }
                    return job_id
            else:
                logger.error("Could not read job ID from submit command standard output.")
                logger.error("Retcode:%s STDOUT:%s STDERR:%s", retcode, stdout.strip(), stderr.strip())
                raise SubmitException(
                    job_name,
                    "Could not read job ID from submit command standard output",
                    stdout=stdout,
                    stderr=stderr,
                    retcode=retcode
                )
        else:
            logger.error("Submit command failed")
            logger.error("Retcode:%s STDOUT:%s STDERR:%s", retcode, stdout.strip(), stderr.strip())
            raise SubmitException(
                job_name, "Could not read job ID from submit command standard output",
                stdout=stdout,
                stderr=stderr,
                retcode=retcode
            )

    def cancel(self, job_ids):
        ''' Cancels the jobs specified by a list of job ids

        Args:
        job_ids : [<job_id> ...]

        Returns :
        [True/False...] : If the cancel operation fails the entire list will be False.
        '''

        job_id_list = ' '.join(job_ids)

        # Make the command to cancel jobs
        _cmd = "scancel"
        if self.clusters:
            _cmd += f" --clusters={self.clusters}"
        _cmd += " {0}"

        retcode, stdout, stderr = self.execute_wait(_cmd.format(job_id_list))
        rets = None
        if retcode == 0:
            for jid in job_ids:
                self.resources[jid]['status'] = JobStatus(JobState.CANCELLED)  # Setting state to cancelled
            rets = [True for i in job_ids]
        else:
            rets = [False for i in job_ids]

        return rets

    @property
    def status_polling_interval(self):
        return 60
