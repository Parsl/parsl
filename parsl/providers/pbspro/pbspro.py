import logging
import os
import time

from parsl.channels import LocalChannel
from parsl.launchers import SingleNodeLauncher
from parsl.providers.pbspro.template import template_string
from parsl.providers import TorqueProvider
from parsl.providers.provider_base import JobState, JobStatus

logger = logging.getLogger(__name__)


class PBSProProvider(TorqueProvider):
    """PBS Pro Execution Provider

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
        Queue to request blocks from.
    nodes_per_block : int
        Nodes to provision per block.
    cpus_per_node : int
        CPUs to provision per node.
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
    worker_init : str
        Command to be run before starting a worker, such as 'module load Anaconda; source activate env'.
    launcher : Launcher
        Launcher for this provider. The default is
        :class:`~parsl.launchers.SingleNodeLauncher`.
    """
    def __init__(self,
                 channel=LocalChannel(),
                 account=None,
                 queue=None,
                 scheduler_options='',
                 worker_init='',
                 nodes_per_block=1,
                 cpus_per_node=1,
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=1,
                 parallelism=1,
                 launcher=SingleNodeLauncher(),
                 walltime="00:20:00",
                 cmd_timeout=120):
        super().__init__(channel,
                         account,
                         queue,
                         scheduler_options,
                         worker_init,
                         nodes_per_block,
                         init_blocks,
                         min_blocks,
                         max_blocks,
                         parallelism,
                         launcher,
                         walltime,
                         cmd_timeout=cmd_timeout)

        self.template_string = template_string
        self._label = 'pbspro'
        self.cpus_per_node = cpus_per_node

    def submit(self, command, tasks_per_node, job_name="parsl"):
        """Submits the command job.

        Parameters
        ----------
        command : str
            Command to be executed on the remote side.
        tasks_per_node : int
            Command invocations to be launched per node.
        job_name : str
            Identifier for job.

        Returns
        -------
        None
            If at capacity and cannot provision more
        job_id : str
            Identifier for the job
        """

        if self.provisioned_blocks >= self.max_blocks:
            logger.warning("[%s] at capacity, cannot add more blocks now", self.label)
            return None

        job_name = "{0}.{1}".format(job_name, time.time())

        script_path = os.path.abspath("{0}/{1}.submit".format(self.script_dir, job_name))

        logger.debug("Requesting {} nodes_per_block, {} tasks_per_node".format(
            self.nodes_per_block, tasks_per_node)
        )

        job_config = {}
        job_config["submit_script_dir"] = self.channel.script_dir
        job_config["nodes_per_block"] = self.nodes_per_block
        job_config["ncpus"] = self.cpus_per_node
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

    @property
    def status_polling_interval(self):
        return 60
