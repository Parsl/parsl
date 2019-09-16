import logging
import os
import time

from parsl.channels import LocalChannel
from parsl.launchers import SimpleLauncher
from parsl.providers.provider_base import ExecutionProvider
from parsl.providers.error import SchedulerMissingArgs, ScriptPathError
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


def _roundrobin(items):
    """ Returns items one at a time in an infinite loop
    """
    while True:
        try:
            for item in items:
                yield item
        except Exception:
            # Remove the iterator we just exhausted from the cycle.
            pass


class AdHocProvider(ExecutionProvider, RepresentationMixin):
    """ Ad-hoc execution provider

    This provider is used to provision execution resources over one or more ad hoc nodes
    that are accessible over a Channel (say, ssh) but otherwise lack a cluster scheduler.

    Each submit call invoked will go through the list channels in a round-robin fashion

    Parameters
    ----------

    worker_init : str
      Command to be run before starting a worker, such as 'module load Anaconda; source activate env'.
      Since this provider calls the same worker_init across all nodes in the ad-hoc cluster, it is
      recommended that a single script is made available across nodes such as ~/setup.sh that can
      be invoked.

    """

    def __init__(self,
                 channels=[],
                 worker_init='',
                 cmd_timeout=30,
                 parallelism=1,
                 init_blocks=0,
                 min_blocks=0,
                 max_blocks=10,
                 move_files=None):

        self.channels = channels
        self._label = 'ad-hoc'
        self.worker_init = worker_init
        self.cmd_timeout = cmd_timeout
        self.parallelism = 1
        self.move_files = move_files
        self.launcher = SimpleLauncher()
        self.init_blocks = init_blocks
        self.min_blocks = min_blocks
        self.max_blocks = max_blocks

        # This will be overridden by the DFK to the rundirs.
        self.script_dir = "."

        # In ad-hoc mode, nodes_per_block should be 1
        self.nodes_per_block = 1

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

        self.roundrobin = _roundrobin(self.channels)
        logger.debug("AdHoc provider initialized")

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

    def submit(self, command, tasks_per_node, job_name="parsl.auto"):
        ''' Submits the command onto the a channel from a round-robin arrangeement of channels

        Submit returns an ID that corresponds to the task that was just submitted.

        Args:
             - command  :(String) Commandline invocation to be made on the remote side.
             - tasks_per_node (int) : command invocations to be launched per node

        Kwargs:
             - job_name (String): Name for job, must be unique

        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job

        '''
        channel = next(self.roundrobin)
        job_name = "{0}.{1}".format(job_name, time.time())

        # Set script path
        script_path = "{0}/{1}.sh".format(self.script_dir, job_name)
        script_path = os.path.abspath(script_path)

        wrap_command = self.worker_init + '\n' + self.launcher(command, tasks_per_node, self.nodes_per_block)

        self._write_submit_script(wrap_command, script_path)

        job_id = None
        proc = None
        remote_pid = None

        if (self.move_files is None and not isinstance(channel, LocalChannel)) or (self.move_files):
            logger.debug("Pushing start script")
            script_path = channel.push_file(script_path, channel.script_dir)

        if not isinstance(channel, LocalChannel):
            logger.debug("Launching in remote mode")
            # Bash would return until the streams are closed. So we redirect to a outs file
            cmd = 'bash {0} &> {0}.out & \n echo "PID:$!" '.format(script_path)
            retcode, stdout, stderr = channel.execute_wait(cmd, self.cmd_timeout)
            for line in stdout.split('\n'):
                if line.startswith("PID:"):
                    remote_pid = line.split("PID:")[1].strip()
                    job_id = remote_pid
                    logger.info(f"Remote PID at {channel} is {remote_pid}")
            if job_id is None:
                logger.warning("Channel failed to start remote command/retrieve PID")
        else:

            try:
                job_id, proc = channel.execute_no_wait('bash {0}'.format(script_path), self.cmd_timeout)
            except Exception as e:
                logger.debug("Channel execute failed for: {}, {}".format(channel, e))
                raise

        self.resources[job_id] = {'job_id': job_id, 'status': 'RUNNING',
                                  'channel': channel,
                                  'remote_pid': remote_pid,
                                  'proc': proc}

        return job_id

    def status(self, job_ids):
        logger.info(f"Checking status of {job_ids}")
        for job_id in job_ids:
            if job_id not in self.resources:
                logger.warning(f"Job_id: {job_id} not present in resources table")
                continue
        raise Exception("Not implemented")

    def cancel(self, job_ids):
        logger.info(f"Cancel invoked on {job_ids}")
        raise Exception("Not Implemented")

    @property
    def scaling_enabled(self):
        return True

    @property
    def label(self):
        return self._label
