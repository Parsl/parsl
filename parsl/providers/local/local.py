import logging
import os
import time

from parsl.channels import LocalChannel
from parsl.launchers import SingleNodeLauncher
from parsl.providers.provider_base import ExecutionProvider, JobState, JobStatus
from parsl.providers.error import SchedulerMissingArgs, ScriptPathError, SubmitException
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
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=1,
                 walltime="00:15:00",
                 worker_init='',
                 cmd_timeout=30,
                 parallelism=1,
                 move_files=None):
        self.channel = channel
        self._label = 'local'
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

        for job_id in self.resources:
            # This job dict should really be a class on its own
            job_dict = self.resources[job_id]
            if job_dict['status'] and job_dict['status'].terminal:
                # We already checked this and it can't change after that
                continue
            # Script path should point to remote path if _should_move_files() is True
            script_path = job_dict['script_path']

            alive = self._is_alive(job_dict)
            str_ec = self._read_job_file(script_path, '.ec').strip()

            status = None
            if str_ec == '-':
                if alive:
                    status = JobStatus(JobState.RUNNING)
                else:
                    # not alive but didn't get to write an exit code
                    if 'cancelled' in job_dict:
                        # because we cancelled it
                        status = JobStatus(JobState.CANCELLED)
                    else:
                        # we didn't cancel it, so it must have been killed by something outside
                        # parsl; we don't have a state for this, but we'll use CANCELLED with
                        # a specific message
                        status = JobStatus(JobState.CANCELLED, message='Killed')
            else:
                try:
                    # TODO: ensure that these files are only read once and clean them
                    ec = int(str_ec)
                    stdout_path = self._job_file_path(script_path, '.out')
                    stderr_path = self._job_file_path(script_path, '.err')
                    if ec == 0:
                        state = JobState.COMPLETED
                    else:
                        state = JobState.FAILED
                    status = JobStatus(state, exit_code=ec,
                                       stdout_path=stdout_path, stderr_path=stderr_path)
                except Exception:
                    status = JobStatus(JobState.FAILED,
                                       'Cannot parse exit code: {}'.format(str_ec))

            job_dict['status'] = status

        return [self.resources[jid]['status'] for jid in job_ids]

    def _is_alive(self, job_dict):
        retcode, stdout, stderr = self.channel.execute_wait(
            'ps -p {} > /dev/null 2> /dev/null; echo "STATUS:$?" '.format(
                job_dict['remote_pid']), self.cmd_timeout)
        for line in stdout.split('\n'):
            if line.startswith("STATUS:"):
                status = line.split("STATUS:")[1].strip()
                if status == "0":
                    return True
                else:
                    return False

    def _job_file_path(self, script_path: str, suffix: str) -> str:
        path = '{0}{1}'.format(script_path, suffix)
        if self._should_move_files():
            path = self.channel.pull_file(path, self.script_dir)
        return path

    def _read_job_file(self, script_path: str, suffix: str) -> str:
        path = self._job_file_path(script_path, suffix)

        with open(path, 'r') as f:
            return f.read()

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

        wrap_command = self.worker_init + f'\nexport JOBNAME=${job_name}\n' + self.launcher(command, tasks_per_node, self.nodes_per_block)

        self._write_submit_script(wrap_command, script_path)

        job_id = None
        remote_pid = None
        if self._should_move_files():
            logger.debug("Pushing start script")
            script_path = self.channel.push_file(script_path, self.channel.script_dir)

        logger.debug("Launching in remote mode")
        # We need to capture the exit code and the streams, so we put them in files. We also write
        # '-' to the exit code file to isolate potential problems with writing to files in the
        # script directory
        #
        # The basic flow is:
        #   1. write "-" to the exit code file. If this fails, exit
        #   2. Launch the following sequence in the background:
        #      a. the command to run
        #      b. write the exit code of the command from (a) to the exit code file
        #   3. Write the PID of the background sequence on stdout. The PID is needed if we want to
        #      cancel the task later.
        #
        # We need to do the >/dev/null 2>&1 so that bash closes stdout, otherwise
        # channel.execute_wait hangs reading the process stdout until all the
        # background commands complete.
        cmd = '/bin/bash -c \'echo - >{0}.ec && {{ {{ bash {0} 1>{0}.out 2>{0}.err ; ' \
              'echo $? > {0}.ec ; }} >/dev/null 2>&1 & echo "PID:$!" ; }}\''.format(script_path)
        retcode, stdout, stderr = self.channel.execute_wait(cmd, self.cmd_timeout)
        if retcode != 0:
            raise SubmitException(job_name, "Launch command exited with code {0}".format(retcode),
                                  stdout, stderr)
        for line in stdout.split('\n'):
            if line.startswith("PID:"):
                remote_pid = line.split("PID:")[1].strip()
                job_id = remote_pid
        if job_id is None:
            raise SubmitException(job_name, "Channel failed to start remote command/retrieve PID")

        self.resources[job_id] = {'job_id': job_id, 'status': JobStatus(JobState.RUNNING),
                                  'remote_pid': remote_pid, 'script_path': script_path}

        return job_id

    def _should_move_files(self):
        return (self.move_files is None and not isinstance(self.channel, LocalChannel)) or (self.move_files)

    def cancel(self, job_ids):
        ''' Cancels the jobs specified by a list of job ids

        Args:
        job_ids : [<job_id> ...]

        Returns :
        [True/False...] : If the cancel operation fails the entire list will be False.
        '''
        for job in job_ids:
            job_dict = self.resources[job]
            job_dict['cancelled'] = True
            logger.debug("Terminating job/proc_id: {0}".format(job))
            cmd = "kill -- -$(ps -o pgid= {} | grep -o '[0-9]*')".format(job_dict['remote_pid'])
            retcode, stdout, stderr = self.channel.execute_wait(cmd, self.cmd_timeout)
            if retcode != 0:
                logger.warning("Failed to kill PID: {} and child processes on {}".format(job_dict['remote_pid'],
                                                                                         self.label))

        rets = [True for i in job_ids]
        return rets

    @property
    def label(self):
        return self._label

    @property
    def status_polling_interval(self):
        return 5


if __name__ == "__main__":

    print("Nothing here")
