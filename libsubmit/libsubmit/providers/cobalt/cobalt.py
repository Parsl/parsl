import logging
import os
import time

import libsubmit.error as ep_error
from libsubmit.channels import LocalChannel
from libsubmit.launchers import AprunLauncher
from libsubmit.providers.cobalt.template import template_string
from libsubmit.providers.cluster_provider import ClusterProvider
from libsubmit.utils import RepresentationMixin

logger = logging.getLogger(__name__)

translate_table = {
    'QUEUED': 'PENDING',
    'STARTING': 'PENDING',
    'RUNNING': 'RUNNING',
    'EXITING': 'COMPLETED',
    'KILLING': 'COMPLETED'
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
        :class:`~libsubmit.channels.LocalChannel` (the default),
        :class:`~libsubmit.channels.SSHChannel`, or
        :class:`~libsubmit.channels.SSHInteractiveLoginChannel`.
    label : str
        Label for this provider.
    script_dir : str
        Relative or absolute path to a directory where intermediate scripts are placed.
    nodes_per_block : int
        Nodes to provision per block.
    tasks_per_node : int
        Tasks to run per node.
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
    overrides : str
        String to append to the Torque submit script on the scheduler.
    launcher : Launcher
        Launcher for this provider. Possible launchers include
        :class:`~libsubmit.launchers.AprunLauncher` (the default) or,
        :class:`~libsubmit.launchers.SingleNodeLauncher`
    """
    def __init__(self,
                 channel=LocalChannel(),
                 label='cobalt',
                 script_dir='parsl_scripts',
                 nodes_per_block=1,
                 tasks_per_node=1,
                 init_blocks=0,
                 min_blocks=0,
                 max_blocks=10,
                 parallelism=1,
                 walltime="00:10:00",
                 account=None,
                 queue=None,
                 overrides='',
                 launcher=AprunLauncher(),
                 cmd_timeout=10):
        super().__init__(label,
                         channel=channel,
                         script_dir=script_dir,
                         nodes_per_block=nodes_per_block,
                         tasks_per_node=tasks_per_node,
                         init_blocks=init_blocks,
                         min_blocks=min_blocks,
                         max_blocks=max_blocks,
                         parallelism=parallelism,
                         walltime=walltime,
                         launcher=launcher,
                         cmd_timeout=cmd_timeout)

        self.account = account
        self.queue = queue
        self.overrides = overrides

    def _status(self):
        """ Internal: Do not call. Returns the status list for a list of job_ids

        Args:
              self

        Returns:
              [status...] : Status list of all jobs
        """

        jobs_missing = list(self.resources.keys())

        retcode, stdout, stderr = super().execute_wait("qstat -u $USER")

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

                status = translate_table.get(parts[4], 'UNKNOWN')

                self.resources[job_id]['status'] = status
                jobs_missing.remove(job_id)

        # squeue does not report on jobs that are not running. So we are filling in the
        # blanks for missing jobs, we might lose some information about why the jobs failed.
        for missing_job in jobs_missing:
            if self.resources[missing_job]['status'] in ['RUNNING', 'KILLING', 'EXITING']:
                self.resources[missing_job]['status'] = translate_table['EXITING']

    def submit(self, command, blocksize, job_name="parsl.auto"):
        """ Submits the command onto an Local Resource Manager job of blocksize parallel elements.
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

        """

        if self.provisioned_blocks >= self.max_blocks:
            logger.warn("[%s] at capacity, cannot add more blocks now", self.label)
            return None

        # Note: Fix this later to avoid confusing behavior.
        # We should always allocate blocks in integer counts of node_granularity
        if blocksize < self.nodes_per_block:
            blocksize = self.nodes_per_block

        account_opt = '-A {}'.format(self.account) if self.account is not None else ''

        job_name = "parsl.{0}.{1}".format(job_name, time.time())

        script_path = "{0}/{1}.submit".format(self.script_dir, job_name)
        script_path = os.path.abspath(script_path)

        job_config = {}
        job_config["overrides"] = self.overrides

        logger.debug("Requesting blocksize:%s nodes_per_block:%s tasks_per_node:%s",
                     blocksize, self.nodes_per_block, self.tasks_per_node)

        # Wrap the command
        job_config["user_script"] = self.launcher(command, self.tasks_per_node, self.nodes_per_block)

        queue_opt = '-q {}'.format(self.queue) if self.queue is not None else ''

        logger.debug("Writing submit script")
        self._write_submit_script(template_string, script_path, job_name, job_config)

        channel_script_path = self.channel.push_file(script_path, self.channel.script_dir)

        command = 'qsub -n {0} {1} -t {2} {3} {4}'.format(
            self.nodes_per_block, queue_opt, self.walltime, account_opt, channel_script_path)
        logger.debug("Executing {}".format(command))

        retcode, stdout, stderr = super().execute_wait(command)

        # TODO : FIX this block
        if retcode != 0:
            logger.error("Failed command: {0}".format(command))
            logger.error("Launch failed stdout:\n{0} \nstderr:{1}\n".format(stdout, stderr))

        logger.debug("Retcode:%s STDOUT:%s STDERR:%s", retcode, stdout.strip(), stderr.strip())

        job_id = None

        if retcode == 0:
            # We should be getting only one line back
            job_id = stdout.strip()
            self.resources[job_id] = {'job_id': job_id, 'status': 'PENDING', 'blocksize': blocksize}
        else:
            logger.error("Submission of command to scale_out failed: {0}".format(stderr))
            raise (ep_error.ScaleOutFailed(self.__class__, "Request to submit job to local scheduler failed"))

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
        retcode, stdout, stderr = super().execute_wait("qdel {0}".format(job_id_list))
        rets = None
        if retcode == 0:
            for jid in job_ids:
                self.resources[jid]['status'] = translate_table['KILLING']  # Setting state to cancelled
            rets = [True for i in job_ids]
        else:
            rets = [False for i in job_ids]

        return rets
