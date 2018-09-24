import logging
import os
import re
import time

from libsubmit.utils import RepresentationMixin
from libsubmit.launchers import SingleNodeLauncher
from libsubmit.providers.condor.template import template_string
from libsubmit.providers.cluster_provider import ClusterProvider

logger = logging.getLogger(__name__)

# See http://pages.cs.wisc.edu/~adesmet/status.html
translate_table = {
    '1': 'PENDING',
    '2': 'RUNNING',
    '3': 'CANCELLED',
    '4': 'COMPLETED',
    '5': 'FAILED',
    '6': 'FAILED',
}


class CondorProvider(RepresentationMixin, ClusterProvider):
    """HTCondor Execution Provider.

    Parameters
    ----------
    channel : Channel
        Channel for accessing this provider. Possible channels include
        :class:`~libsubmit.channels.LocalChannel` (the default),
        :class:`~libsubmit.channels.SSHChannel`, or
        :class:`~libsubmit.channels.SSHInteractiveLoginChannel`.
    label : str
        Label for this provider.
    nodes_per_block : int
        Nodes to provision per block.
    tasks_per_node : int
        Workers to start per node
    init_blocks : int
        Number of blocks to provision at time of initialization
    min_blocks : int
        Minimum number of blocks to maintain
    max_blocks : int
        Maximum number of blocks to maintain.
    parallelism : float
        Ratio of provisioned task slots to active tasks. A parallelism value of 1 represents aggressive
        scaling where as many resources as possible are used; parallelism close to 0 represents
        the opposite situation in which as few resources as possible (i.e., min_blocks) are used.
    environment : dict of str
        A dictionary of environmant variable name and value pairs which will be set before
        running a task.
    script_dir : str
        Relative or absolute path to a directory where intermediate scripts are placed.
    project : str
        Project which the job will be charged against
    overrides : str
        String to add specific condor attributes to the HTCondor submit script.
    worker_setup : str
        Command to be run before running a task.
    requirements : str
        Condor requirements.
    launcher : Launcher
        Launcher for this provider. Possible launchers include
        :class:`~libsubmit.launchers.SingleNodeLauncher` (the default),
    """
    def __init__(self,
                 channel=None,
                 label='condor',
                 nodes_per_block=1,
                 tasks_per_node=1,
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=10,
                 parallelism=1,
                 environment=None,
                 script_dir='parsl_scripts',
                 project='',
                 overrides='',
                 walltime="00:10:00",
                 worker_setup='',
                 launcher=SingleNodeLauncher(),
                 requirements=''):

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
                         launcher)

        self.provisioned_blocks = 0

        self.environment = environment if environment is not None else {}
        for key, value in self.environment.items():
            # To escape literal quote marks, double them
            # See: http://research.cs.wisc.edu/htcondor/manual/v8.6/condor_submit.html
            try:
                self.environment[key] = "'{}'".format(value.replace("'", '"').replace('"', '""'))
            except AttributeError:
                pass

        self.project = project
        self.overrides = overrides
        self.worker_setup = worker_setup
        self.requirements = requirements

    def _status(self):
        """Update the resource dictionary with job statuses."""

        job_id_list = ' '.join(self.resources.keys())
        cmd = "condor_q {0} -af:jr JobStatus".format(job_id_list)
        retcode, stdout, stderr = super().execute_wait(cmd)
        """
        Example output:

        $ condor_q 34524642.0 34524643.0 -af:jr JobStatus
        34524642.0 2
        34524643.0 1
        """

        for line in stdout.strip().split('\n'):
            parts = line.split()
            job_id = parts[0]
            status = translate_table.get(parts[1], 'UNKNOWN')
            self.resources[job_id]['status'] = status

    def status(self, job_ids):
        """Get the status of a list of jobs identified by their ids.

        Parameters
        ----------
        job_ids : list of int
            Identifiers of jobs for which the status will be returned.

        Returns
        -------
        List of int
            Status codes for the requested jobs.

        """
        self._status()
        return [self.resources[jid]['status'] for jid in job_ids]

    def submit(self, command, blocksize, job_name="parsl.auto"):
        """Submits the command onto an Local Resource Manager job of blocksize parallel elements.

        example file with the complex case of multiple submits per job:
            Universe =vanilla
            output = out.$(Cluster).$(Process)
            error = err.$(Cluster).$(Process)
            log = log.$(Cluster)
            leave_in_queue = true
            executable = test.sh
            queue 5
            executable = foo
            queue 1

        $ condor_submit test.sub
        Submitting job(s)......
        5 job(s) submitted to cluster 118907.
        1 job(s) submitted to cluster 118908.

        Parameters
        ----------
        command : str
            Command to execute
        blocksize : int
            Number of blocks to request.
        job_name : str
            Job name prefix.

        Returns
        -------
        None or str
            None if at capacity and cannot provision more; otherwise the identifier for the job.
        """

        logger.debug("Attempting to launch with blocksize: {}".format(blocksize))
        if self.provisioned_blocks >= self.max_blocks:
            template = "Provider {} is currently using {} blocks while max_blocks is {}; no blocks will be added"
            logger.warn(template.format(self.label, self.provisioned_blocks, self.max_blocks))
            return None

        # Note: Fix this later to avoid confusing behavior.
        # We should always allocate blocks in integer counts of node_granularity
        blocksize = max(self.nodes_per_block, blocksize)

        job_name = "parsl.{0}.{1}".format(job_name, time.time())

        script_path = "{0}/{1}.submit".format(self.script_dir, job_name)
        script_path = os.path.abspath(script_path)
        userscript_path = "{0}/{1}.script".format(self.script_dir, job_name)
        userscript_path = os.path.abspath(userscript_path)

        self.environment["JOBNAME"] = "'{}'".format(job_name)

        job_config = {}
        job_config["job_name"] = job_name
        job_config["submit_script_dir"] = self.channel.script_dir
        job_config["project"] = self.project
        job_config["nodes"] = self.nodes_per_block
        job_config["overrides"] = self.overrides
        job_config["worker_setup"] = self.worker_setup
        job_config["user_script"] = command
        job_config["tasks_per_node"] = self.tasks_per_node
        job_config["requirements"] = self.requirements
        job_config["environment"] = ' '.join(['{}={}'.format(key, value) for key, value in self.environment.items()])

        # Move the user script
        # This is where the command should be wrapped by the launchers.
        wrapped_command = self.launcher(command,
                                        self.tasks_per_node,
                                        self.nodes_per_block)

        with open(userscript_path, 'w') as f:
            f.write(job_config["worker_setup"] + '\n' + wrapped_command)

        user_script_path = self.channel.push_file(userscript_path, self.channel.script_dir)
        job_config["input_files"] = user_script_path
        job_config["job_script"] = os.path.basename(user_script_path)

        # Construct and move the submit script
        self._write_submit_script(template_string, script_path, job_name, job_config)
        channel_script_path = self.channel.push_file(script_path, self.channel.script_dir)

        cmd = "condor_submit {0}".format(channel_script_path)
        retcode, stdout, stderr = super().execute_wait(cmd, 3)
        logger.debug("Retcode:%s STDOUT:%s STDERR:%s", retcode, stdout.strip(), stderr.strip())

        job_id = []

        if retcode == 0:
            for line in stdout.split('\n'):
                if re.match('^[0-9]', line) is not None:
                    cluster = line.split(" ")[5]
                    # We know the first job id ("process" in condor terms) within a
                    # cluster is 0 and we know the total number of jobs from
                    # condor_submit, so we use some list comprehensions to expand
                    # the condor_submit output into job IDs
                    # e.g., ['118907.0', '118907.1', '118907.2', '118907.3', '118907.4', '118908.0']
                    processes = [str(x) for x in range(0, int(line[0]))]
                    job_id += [cluster + process for process in processes]

            self._add_resource(job_id)
        return job_id[0]

    def cancel(self, job_ids):
        """Cancels the jobs specified by a list of job IDs.

        Parameters
        ----------
        job_ids : list of str
            The job IDs to cancel.

        Returns
        -------
        list of bool
            Each entry in the list will be True if the job is cancelled succesfully, otherwise False.
        """

        job_id_list = ' '.join(job_ids)
        cmd = "condor_rm {0}; condor_rm -forcex {0}".format(job_id_list)
        logger.debug("Attempting removal of jobs : {0}".format(cmd))
        retcode, stdout, stderr = self.channel.execute_wait(cmd, 3)
        rets = None
        if retcode == 0:
            for jid in job_ids:
                self.resources[jid]['status'] = 'CANCELLED'
            rets = [True for i in job_ids]
        else:
            rets = [False for i in job_ids]

        return rets

    @property
    def scaling_enabled(self):
        return True

    @property
    def current_capacity(self):
        return self

    def _add_resource(self, job_id):
        for jid in job_id:
            self.resources[jid] = {'status': 'PENDING', 'size': 1}
        return True


if __name__ == "__main__":

    print("None")
