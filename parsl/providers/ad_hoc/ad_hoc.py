import logging
import os
import time

from parsl.channels import LocalChannel
from parsl.launchers import SimpleLauncher
from parsl.providers.provider_base import ExecutionProvider, JobStatus, JobState
from parsl.providers.error import ScriptPathError
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class AdHocProvider(ExecutionProvider, RepresentationMixin):
    """ Ad-hoc execution provider

    This provider is used to provision execution resources over one or more ad hoc nodes
    that are each accessible over a Channel (say, ssh) but otherwise lack a cluster scheduler.

    Parameters
    ----------

    channels : list of Channel ojects
      Each channel represents a connection to a remote node

    worker_init : str
      Command to be run before starting a worker, such as 'module load Anaconda; source activate env'.
      Since this provider calls the same worker_init across all nodes in the ad-hoc cluster, it is
      recommended that a single script is made available across nodes such as ~/setup.sh that can
      be invoked.

    cmd_timeout : int
      Duration for which the provider will wait for a command to be invoked on a remote system.
      Defaults to 30s

    parallelism : float
      Determines the ratio of workers to tasks as managed by the strategy component

    """

    def __init__(self,
                 channels=[],
                 worker_init='',
                 cmd_timeout=30,
                 parallelism=1,
                 move_files=None):

        self.channels = channels
        self._label = 'ad-hoc'
        self.worker_init = worker_init
        self.cmd_timeout = cmd_timeout
        self.parallelism = 1
        self.move_files = move_files
        self.launcher = SimpleLauncher()
        self.init_blocks = self.min_blocks = self.max_blocks = len(channels)

        # This will be overridden by the DFK to the rundirs.
        self.script_dir = "."

        # In ad-hoc mode, nodes_per_block should be 1
        self.nodes_per_block = 1

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

        self.least_loaded = self._least_loaded()
        logger.debug("AdHoc provider initialized")

    def _write_submit_script(self, script_string, script_filename):
        '''
        Load the template string with config values and write the generated submit script to
        a submit script file.

        Parameters
        ----------
        script_string: (string)
          The template string to be used for the writing submit script

        script_filename: (string)
          Name of the submit script

        Returns
        -------
        None: on success

        Raises
        ------
        ScriptPathError
          Unable to write submit script out
        '''

        try:
            with open(script_filename, 'w') as f:
                f.write(script_string)

        except IOError as e:
            logger.error("Failed writing to submit script: %s", script_filename)
            raise (ScriptPathError(script_filename, e))

        return None

    def _least_loaded(self):
        """ Find channels that are not in use

        Returns
        -------
        channel : Channel object
        None : When there are no more available channels
        """
        while True:
            channel_counts = {channel: 0 for channel in self.channels}
            for job_id in self.resources:
                channel = self.resources[job_id]['channel']
                if self.resources[job_id]['status'].state == JobState.RUNNING:
                    channel_counts[channel] = channel_counts.get(channel, 0) + 1
                else:
                    channel_counts[channel] = channel_counts.get(channel, 0)

            logger.debug("Channel_counts : {}".format(channel_counts))
            if 0 not in channel_counts.values():
                yield None

            for channel in channel_counts:
                if channel_counts[channel] == 0:
                    yield channel

    def submit(self, command, tasks_per_node, job_name="parsl.adhoc"):
        ''' Submits the command onto a channel from the list of channels

        Submit returns an ID that corresponds to the task that was just submitted.

        Parameters
        ----------
        command: (String)
          Commandline invocation to be made on the remote side.

        tasks_per_node: (int)
          command invocations to be launched per node

        job_name: (String)
          Name of the job. Default : parsl.adhoc


        Returns
        -------
        None
          At capacity, cannot provision more

        job_id: (string)
          Identifier for the job

        '''
        channel = next(self.least_loaded)
        if channel is None:
            logger.warning("All Channels in Ad-Hoc provider are in use")
            return None

        job_name = "{0}.{1}".format(job_name, time.time())

        # Set script path
        script_path = "{0}/{1}.sh".format(self.script_dir, job_name)
        script_path = os.path.abspath(script_path)

        wrap_command = self.worker_init + '\n' + self.launcher(command, tasks_per_node, self.nodes_per_block)

        self._write_submit_script(wrap_command, script_path)

        job_id = None
        remote_pid = None
        final_cmd = None

        if (self.move_files is None and not isinstance(channel, LocalChannel)) or (self.move_files):
            logger.debug("Pushing start script")
            script_path = channel.push_file(script_path, channel.script_dir)

        # Bash would return until the streams are closed. So we redirect to a outs file
        final_cmd = 'bash {0} > {0}.out 2>&1 & \n echo "PID:$!" '.format(script_path)
        retcode, stdout, stderr = channel.execute_wait(final_cmd, self.cmd_timeout)
        for line in stdout.split('\n'):
            if line.startswith("PID:"):
                remote_pid = line.split("PID:")[1].strip()
                job_id = remote_pid
        if job_id is None:
            logger.warning("Channel failed to start remote command/retrieve PID")

        self.resources[job_id] = {'job_id': job_id,
                                  'status': JobStatus(JobState.RUNNING),
                                  'cmd': final_cmd,
                                  'channel': channel,
                                  'remote_pid': remote_pid}

        return job_id

    def status(self, job_ids):
        """ Get status of the list of jobs with job_ids

        Parameters
        ----------
        job_ids : list of strings
          List of job id strings

        Returns
        -------
        list of JobStatus objects
        """
        for job_id in job_ids:
            channel = self.resources[job_id]['channel']
            status_command = "ps --pid {} | grep {}".format(self.resources[job_id]['job_id'],
                                                            self.resources[job_id]['cmd'].split()[0])
            retcode, stdout, stderr = channel.execute_wait(status_command)
            if retcode != 0 and self.resources[job_id]['status'].state == JobState.RUNNING:
                self.resources[job_id]['status'] = JobStatus(JobState.FAILED)

        return [self.resources[job_id]['status'] for job_id in job_ids]

    def cancel(self, job_ids):
        """ Cancel a list of jobs with job_ids

        Parameters
        ----------
        job_ids : list of strings
          List of job id strings

        Returns
        -------
        list of confirmation bools: [True, False...]
        """
        logger.debug("Cancelling jobs: {}".format(job_ids))
        rets = []
        for job_id in job_ids:
            channel = self.resources[job_id]['channel']
            cmd = "kill -TERM -$(ps -o pgid= {} | grep -o '[0-9]*')".format(self.resources[job_id]['job_id'])
            retcode, stdout, stderr = channel.execute_wait(cmd)
            if retcode == 0:
                rets.append(True)
            else:
                rets.append(False)
            self.resources[job_id]['status'] = JobStatus(JobState.COMPLETED)
        return rets

    @property
    def scaling_enabled(self):
        return True

    @property
    def label(self):
        return self._label

    @property
    def status_polling_interval(self):
        return 10
