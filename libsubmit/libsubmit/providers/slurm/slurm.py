import logging
import os
import time

from libsubmit.channels import LocalChannel
from libsubmit.launchers import SingleNodeLauncher
from libsubmit.providers.cluster_provider import ClusterProvider
from libsubmit.providers.slurm.template import template_string
from libsubmit.utils import RepresentationMixin

logger = logging.getLogger(__name__)

translate_table = {
    'PD': 'PENDING',
    'R': 'RUNNING',
    'CA': 'CANCELLED',
    'CF': 'PENDING',  # (configuring),
    'CG': 'RUNNING',  # (completing),
    'CD': 'COMPLETED',
    'F': 'FAILED',  # (failed),
    'TO': 'TIMEOUT',  # (timeout),
    'NF': 'FAILED',  # (node failure),
    'RV': 'FAILED',  # (revoked) and
    'SE': 'FAILED'
}  # (special exit state


class SlurmProvider(ClusterProvider, RepresentationMixin):
    """Slurm Execution Provider

    This provider uses sbatch to submit, squeue for status and scancel to cancel
    jobs. The sbatch script to be used is created from a template file in this
    same module.

    Parameters
    ----------
    partition : str
        Slurm partition to request blocks from.
    label : str
        Label for this provider.
    channel : Channel
        Channel for accessing this provider. Possible channels include
        :class:`~libsubmit.channels.LocalChannel` (the default),
        :class:`~libsubmit.channels.SSHChannel`, or
        :class:`~libsubmit.channels.SSHInteractiveLoginChannel`.
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
    parallelism : float
        Ratio of provisioned task slots to active tasks. A parallelism value of 1 represents aggressive
        scaling where as many resources as possible are used; parallelism close to 0 represents
        the opposite situation in which as few resources as possible (i.e., min_blocks) are used.
    walltime : str
        Walltime requested per block in HH:MM:SS.
    overrides : str
        String to prepend to the #SBATCH blocks in the submit script to the scheduler.
    launcher : Launcher
        Launcher for this provider. Possible launchers include
        :class:`~libsubmit.launchers.SingleNodeLauncher` (the default),
        :class:`~libsubmit.launchers.SrunLauncher`, or
        :class:`~libsubmit.launchers.AprunLauncher`
    """

    def __init__(self,
                 partition,
                 label='slurm',
                 channel=LocalChannel(),
                 script_dir='parsl_scripts',
                 nodes_per_block=1,
                 tasks_per_node=1,
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=10,
                 parallelism=1,
                 walltime="00:10:00",
                 overrides='',
                 cmd_timeout=10,
                 launcher=SingleNodeLauncher()):
        super().__init__(label,
                         channel,
                         script_dir,
                         nodes_per_block,
                         tasks_per_node,
                         init_blocks,
                         min_blocks,
                         max_blocks,
                         parallelism,
                         walltime,
                         cmd_timeout=cmd_timeout,
                         launcher=launcher)
        self.partition = partition
        self.overrides = overrides

    def _status(self):
        ''' Internal: Do not call. Returns the status list for a list of job_ids

        Args:
              self

        Returns:
              [status...] : Status list of all jobs
        '''
        job_id_list = ','.join(self.resources.keys())
        cmd = "squeue --job {0}".format(job_id_list)

        retcode, stdout, stderr = super().execute_wait(cmd)

        # Execute_wait failed. Do no update
        if retcode != 0:
            return

        jobs_missing = list(self.resources.keys())
        for line in stdout.split('\n'):
            parts = line.split()
            if parts and parts[0] != 'JOBID':
                job_id = parts[0]
                status = translate_table.get(parts[4], 'UNKNOWN')
                self.resources[job_id]['status'] = status
                jobs_missing.remove(job_id)

        # squeue does not report on jobs that are not running. So we are filling in the
        # blanks for missing jobs, we might lose some information about why the jobs failed.
        for missing_job in jobs_missing:
            if self.resources[missing_job]['status'] in ['PENDING', 'RUNNING']:
                self.resources[missing_job]['status'] = 'COMPLETED'

    def submit(self, command, blocksize, job_name="parsl.auto"):
        """Submit the command as a slurm job of blocksize parallel elements.

        Parameters
        ----------
        command : str
            Command to be made on the remote side.
        blocksize : int
            Not implemented.
        job_name : str
            Name for the job (must be unique).

        Returns
        -------
        None or str
            If at capacity, returns None; otherwise, a string identifier for the job
        """

        if self.provisioned_blocks >= self.max_blocks:
            logger.warn("Slurm provider '{}' is at capacity (no more blocks will be added)".format(self.label))
            return None

        job_name = "{0}.{1}".format(job_name, time.time())

        script_path = "{0}/{1}.submit".format(self.script_dir, job_name)
        script_path = os.path.abspath(script_path)

        logger.debug("Requesting one block with {} nodes".format(self.nodes_per_block))

        job_config = {}
        job_config["submit_script_dir"] = self.channel.script_dir
        job_config["nodes"] = self.nodes_per_block
        job_config["walltime"] = self.walltime
        job_config["overrides"] = self.overrides
        job_config["partition"] = self.partition
        job_config["user_script"] = command

        # Wrap the command
        job_config["user_script"] = self.launcher(command,
                                                  self.tasks_per_node,
                                                  self.nodes_per_block)

        logger.debug("Writing submit script")
        self._write_submit_script(template_string, script_path, job_name, job_config)

        channel_script_path = self.channel.push_file(script_path, self.channel.script_dir)

        retcode, stdout, stderr = super().execute_wait("sbatch {0}".format(channel_script_path))

        job_id = None
        if retcode == 0:
            for line in stdout.split('\n'):
                if line.startswith("Submitted batch job"):
                    job_id = line.split("Submitted batch job")[1].strip()
                    self.resources[job_id] = {'job_id': job_id, 'status': 'PENDING', 'blocksize': blocksize}
        else:
            print("Submission of command to scale_out failed")
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
        retcode, stdout, stderr = super().execute_wait("scancel {0}".format(job_id_list))
        rets = None
        if retcode == 0:
            for jid in job_ids:
                self.resources[jid]['status'] = translate_table['CA']  # Setting state to cancelled
            rets = [True for i in job_ids]
        else:
            rets = [False for i in job_ids]

        return rets

    def _test_add_resource(self, job_id):
        self.resources.extend([{'job_id': job_id, 'status': 'PENDING', 'size': 1}])
        return True


if __name__ == "__main__":

    print("None")
