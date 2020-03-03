import logging
import os
import time

from parsl.channels import LocalChannel
from parsl.launchers import SingleNodeLauncher
from parsl.providers.provider_base import ExecutionProvider, JobState, JobStatus
from parsl.providers.error import SchedulerMissingArgs, ScriptPathError
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class LocalProvider(ExecutionProvider, RepresentationMixin):
    """ Local Execution Provider

    This provider is used to provide execution resources from the localhost.

    Parameters
    ----------

    min_blocks : int
        Minimum number of blocks to maintain.
    max_blocks : int
        Maximum number of blocks to maintain.
    parallelism : float
        Ratio of provisioned task slots to active tasks. A parallelism value of 1 represents aggressive
        scaling where as many resources as possible are used; parallelism close to 0 represents
        the opposite situation in which as few resources as possible (i.e., min_blocks) are used.
    move_files : Optional[Bool]: should files be moved? by default, Parsl will try to figure
        this out itself (= None). If True, then will always move. If False, will never move.
    worker_init : str
        Command to be run before starting a worker, such as 'module load Anaconda; source activate env'.
    """

    def __init__(self,
                 channel=LocalChannel(),
                 nodes_per_block=1,
                 launcher=SingleNodeLauncher(),
                 init_blocks=4,
                 min_blocks=0,
                 max_blocks=10,
                 walltime="00:15:00",
                 worker_init='',
                 cmd_timeout=30,
                 parallelism=1,
                 move_files=None):
        self.channel = channel
        self._label = 'local'
        self.provisioned_blocks = 0
        self.nodes_per_block = nodes_per_block
        self.launcher = launcher
        self.worker_init = worker_init
        self.init_blocks = init_blocks
        self.min_blocks = min_blocks
        self.max_blocks = max_blocks
        self.parallelism = parallelism
        self.walltime = walltime
        self.script_dir = None
        self.cmd_timeout = cmd_timeout
        self.move_files = move_files

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

    def status(self, job_ids):
        '''  Get the status of a list of jobs identified by their ids.

        Args:
            - job_ids (List of ids) : List of identifiers for the jobs

        Returns:
            - List of status codes.

        '''

        logger.debug("Checking status of: {0}".format(job_ids))
        for job_id in self.resources:

            retcode, stdout, stderr = self.channel.execute_wait('ps -p {} > /dev/null 2> /dev/null; echo "STATUS:$?" '.format(
                self.resources[job_id]['remote_pid']), self.cmd_timeout)
            for line in stdout.split('\n'):
                if line.startswith("STATUS:"):
                    status = line.split("STATUS:")[1].strip()
                    if status == "0":
                        self.resources[job_id]['status'] = JobStatus(JobState.RUNNING)
                    else:
                        self.resources[job_id]['status'] = JobStatus(JobState.FAILED)

        return [self.resources[jid]['status'] for jid in job_ids]

    def _write_submit_script(self, script_string, script_filename):
        '''
        Load the template string with config values and write the generated submit script to
        a submit script file.

        Args:
              - template_string (string) : The template string to be used for the writing submit script
              - script_filename (string) : Name of the submit script

        Returns:
              - True: on success

        Raises:
              SchedulerMissingArgs : If template is missing args
              ScriptPathError : Unable to write submit script out
        '''

        try:
            with open(script_filename, 'w') as f:
                f.write(script_string)

        except KeyError as e:
            logger.error("Missing keys for submit script: %s", e)
            raise (SchedulerMissingArgs(e.args, self.label))

        except IOError as e:
            logger.error("Failed writing to submit script: %s", script_filename)
            raise (ScriptPathError(script_filename, e))

        return True

    def submit(self, command, tasks_per_node, job_name="parsl.localprovider"):
        ''' Submits the command onto an Local Resource Manager job.
        Submit returns an ID that corresponds to the task that was just submitted.

        If tasks_per_node <  1:
             1/tasks_per_node is provisioned

        If tasks_per_node == 1:
             A single node is provisioned

        If tasks_per_node >  1 :
             tasks_per_node nodes are provisioned.

        Args:
             - command  :(String) Commandline invocation to be made on the remote side.
             - tasks_per_node (int) : command invocations to be launched per node

        Kwargs:
             - job_name (String): Name for job, must be unique

        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job

        '''

        job_name = "{0}.{1}".format(job_name, time.time())

        # Set script path
        script_path = "{0}/{1}.sh".format(self.script_dir, job_name)
        script_path = os.path.abspath(script_path)

        wrap_command = self.worker_init + '\n' + self.launcher(command, tasks_per_node, self.nodes_per_block)

        self._write_submit_script(wrap_command, script_path)

        job_id = None
        remote_pid = None
        if (self.move_files is None and not isinstance(self.channel, LocalChannel)) or (self.move_files):
            logger.debug("Pushing start script")
            script_path = self.channel.push_file(script_path, self.channel.script_dir)

        logger.debug("Launching in remote mode")
        # Bash would return until the streams are closed. So we redirect to a outs file
        cmd = 'bash {0} > {0}.out 2>&1 & \n echo "PID:$!" '.format(script_path)
        retcode, stdout, stderr = self.channel.execute_wait(cmd, self.cmd_timeout)
        for line in stdout.split('\n'):
            if line.startswith("PID:"):
                remote_pid = line.split("PID:")[1].strip()
                job_id = remote_pid
        if job_id is None:
            logger.warning("Channel failed to start remote command/retrieve PID")

        self.resources[job_id] = {'job_id': job_id, 'status': JobStatus(JobState.RUNNING),
                                  'remote_pid': remote_pid}

        return job_id

    def cancel(self, job_ids):
        ''' Cancels the jobs specified by a list of job ids

        Args:
        job_ids : [<job_id> ...]

        Returns :
        [True/False...] : If the cancel operation fails the entire list will be False.
        '''
        for job in job_ids:
            logger.debug("Terminating job/proc_id: {0}".format(job))
            cmd = "kill -- -$(ps -o pgid= {} | grep -o '[0-9]*')".format(self.resources[job]['remote_pid'])
            retcode, stdout, stderr = self.channel.execute_wait(cmd, self.cmd_timeout)
            if retcode != 0:
                logger.warning("Failed to kill PID: {} and child processes on {}".format(self.resources[job]['remote_pid'],
                                                                                         self.label))

        rets = [True for i in job_ids]
        return rets

    @property
    def current_capacity(self):
        return len(self.resources)

    @property
    def label(self):
        return self._label

    @property
    def status_polling_interval(self):
        return 5


if __name__ == "__main__":

    print("Nothing here")
