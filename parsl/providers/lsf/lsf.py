import logging
import math
import os
import time

from parsl.jobs.states import JobState, JobStatus
from parsl.launchers import SingleNodeLauncher
from parsl.providers.cluster_provider import ClusterProvider
from parsl.providers.lsf.template import template_string
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

    This provider uses bsub to submit, bjobs for status and bkill to cancel
    jobs. The bsub script to be used is created from a template file in this
    same module.

    Parameters
    ----------
    nodes_per_block : int
        Nodes to provision per block.
        When request_by_nodes is False, it is computed by cores_per_block / cores_per_node.
    cores_per_block : int
        Cores to provision per block. Enabled only when request_by_nodes is False.
    cores_per_node: int
        Cores to provision per node. Enabled only when request_by_nodes is False.
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
    queue : str
        Queue to which to submit the job request
    scheduler_options : str
        String to prepend to the #BSUB blocks in the submit script to the scheduler.
    worker_init : str
        Command to be run before starting a worker, such as 'module load Anaconda; source activate env'.
    cmd_timeout : int
        Seconds after which requests to the scheduler will timeout. Default: 120s
    launcher : Launcher
        Launcher for this provider. Possible launchers include
        :class:`~parsl.launchers.SingleNodeLauncher` (the default),
        :class:`~parsl.launchers.SrunLauncher`, or
        :class:`~parsl.launchers.AprunLauncher`
    bsub_redirection: Bool
        Should a redirection symbol "<" be included when submitting jobs, i.e., Bsub < job_script.
    request_by_nodes: Bool
        Request by nodes or request by cores per block.
        When this is set to false, nodes_per_block is computed by cores_per_block / cores_per_node.
        Default is True.
    """

    def __init__(self,
                 nodes_per_block=1,
                 cores_per_block=None,
                 cores_per_node=None,
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=1,
                 parallelism=1,
                 walltime="00:10:00",
                 scheduler_options='',
                 worker_init='',
                 project=None,
                 queue=None,
                 cmd_timeout=120,
                 bsub_redirection=False,
                 request_by_nodes=True,
                 launcher=SingleNodeLauncher()):
        label = 'LSF'
        super().__init__(label,
                         nodes_per_block,
                         init_blocks,
                         min_blocks,
                         max_blocks,
                         parallelism,
                         walltime,
                         cmd_timeout=cmd_timeout,
                         launcher=launcher)

        self.project = project
        self.queue = queue
        self.cores_per_block = cores_per_block
        self.cores_per_node = cores_per_node
        self.bsub_redirection = bsub_redirection
        self.request_by_nodes = request_by_nodes

        # Update scheduler options
        self.scheduler_options = scheduler_options + "\n"
        if project:
            self.scheduler_options += "#BSUB -P {}\n".format(project)
        if queue:
            self.scheduler_options += "#BSUB -q {}\n".format(queue)
        if request_by_nodes:
            self.scheduler_options += "#BSUB -nnodes {}\n".format(nodes_per_block)
        else:
            assert cores_per_block is not None and cores_per_node is not None, \
                       "Requesting resources by the number of cores. " \
                       "Need to specify cores_per_block and cores_per_node in the LSF provider."

            self.scheduler_options += "#BSUB -n {}\n".format(cores_per_block)
            self.scheduler_options += '#BSUB -R "span[ptile={}]"\n'.format(cores_per_node)

            # Set nodes_per_block manually for Parsl strategy
            assert cores_per_node != 0, "Need to specify a non-zero cores_per_node."
            self.nodes_per_block = int(math.ceil(cores_per_block / cores_per_node))

        self.worker_init = worker_init

    def _status(self):
        '''Returns the status list for a list of job_ids

        Args:
              self

        Returns:
              [status...] : Status list of all jobs
        '''
        logger.debug(f"Resources: {self.resources}")
        job_id_list = [jid for jid, job in self.resources.items() if not job['status'].terminal]
        if not job_id_list:
            logger.debug('No active jobs, skipping status update')
            return
        logger.debug(f"job_id_list: {job_id_list}")
        # only request the JOBID and STAT columns from LSF
        cmd = f"bjobs -noheader -o 'jobid stat' {' '.join(job_id_list)}"
        logger.debug(f"Executing command: {cmd}")
        retcode, stdout, stderr = self.execute_wait(cmd)
        logger.debug(f"bjobs returned:\nstdout=\n{stdout}stderr=\n{stderr}")
        # Execute_wait failed. Do no update
        if retcode != 0:
            logger.warning(f"bjobs failed with non-zero exit code: {retcode}")
            return

        jobs_missing = set(job_id_list)
        bjobs_lines = stdout.rstrip('\n').split('\n')

        for line in bjobs_lines:
            line_list = line.split()
            if len(line_list) != 2:
                logger.debug(f"{line_list} length not equal to 2, skipping")
                continue
            job_id, lsf_state = line_list
            if job_id not in job_id_list:
                logger.debug(f"job_id {job_id} not in job_id_list, skipping")
                continue
            if lsf_state not in translate_table:
                logger.warning(f"LSF status {lsf_state} is not recognized")
            state = translate_table.get(lsf_state, JobState.UNKNOWN)
            logger.debug(f"Updating job {job_id} with LSF status {lsf_state} "
                         f"to parsl state {state}")
            self.resources[job_id]['status'] = JobStatus(state)
            jobs_missing.remove(job_id)

        # bjobs does not report on jobs that are not running. So we are filling in the
        # blanks for missing jobs, we might lose some information about why the jobs failed.
        for missing_job in jobs_missing:
            logger.debug(f"Updating missing job {missing_job} to completed status")
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

        job_name = "{0}.{1}".format(job_name, time.time())

        script_path = "{0}/{1}.submit".format(self.script_dir, job_name)
        script_path = os.path.abspath(script_path)

        logger.debug("Requesting one block with {} nodes".format(self.nodes_per_block))

        job_config = {}
        job_config["submit_script_dir"] = self.script_dir
        job_config["nodes"] = self.nodes_per_block
        job_config["tasks_per_node"] = tasks_per_node
        job_config["walltime"] = wtime_to_minutes(self.walltime)
        job_config["scheduler_options"] = self.scheduler_options
        job_config["worker_init"] = self.worker_init
        job_config["user_script"] = command

        # Wrap the command
        job_config["user_script"] = self.launcher(command,
                                                  tasks_per_node,
                                                  self.nodes_per_block)

        logger.debug("Writing submit script")
        self._write_submit_script(template_string, script_path, job_name, job_config)

        if self.bsub_redirection:
            cmd = "bsub < {0}".format(script_path)
        else:
            cmd = "bsub {0}".format(script_path)
        retcode, stdout, stderr = super().execute_wait(cmd)

        job_id = None
        if retcode == 0:
            for line in stdout.split('\n'):
                if line.lower().startswith("job") and "is submitted to" in line.lower():
                    job_id = line.split()[1].strip('<>')
                    self.resources[job_id] = {'job_id': job_id, 'status': JobStatus(JobState.PENDING)}
        else:
            logger.warning("Submit command failed")
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
        retcode, stdout, stderr = self.execute_wait("bkill {0}".format(job_id_list))
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
