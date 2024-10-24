import copy
import logging
import os
import subprocess

from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class LocalChannel(RepresentationMixin):
    ''' This is not even really a channel, since opening a local shell is not heavy
    and done so infrequently that they do not need a persistent channel
    '''

    def __init__(self):
        ''' Initialize the local channel.

        KwArgs:
            - envs (dict) : A dictionary of env variables to be set when launching the shell
        '''
        envs = {}
        self.envs = envs
        local_env = os.environ.copy()
        self._envs = copy.deepcopy(local_env)
        self._envs.update(envs)

    def execute_wait(self, cmd, walltime=None, envs={}):
        ''' Synchronously execute a commandline string on the shell.

        Args:
            - cmd (string) : Commandline string to execute
            - walltime (int) : walltime in seconds, this is not really used now.

        Kwargs:
            - envs (dict) : Dictionary of env variables. This will be used
              to override the envs set at channel initialization.

        Returns:
            - retcode : Return code from the execution
            - stdout  : stdout string
            - stderr  : stderr string
        '''
        current_env = copy.deepcopy(self._envs)
        current_env.update(envs)

        try:
            logger.debug("Creating process with command '%s'", cmd)
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=current_env,
                shell=True,
                preexec_fn=os.setpgrp
            )
            logger.debug("Created process with pid %s. Performing communicate", proc.pid)
            (stdout, stderr) = proc.communicate(timeout=walltime)
            retcode = proc.returncode
            logger.debug("Process %s returned %s", proc.pid, proc.returncode)

        except Exception:
            logger.exception(f"Execution of command failed:\n{cmd}")
            raise
        else:
            logger.debug("Execution of command in process %s completed normally", proc.pid)

        return (retcode, stdout.decode("utf-8"), stderr.decode("utf-8"))
