import logging
import os
import subprocess

from parsl.channels.base import Channel
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class LocalChannel(Channel, RepresentationMixin):
    ''' This is not even really a channel, since opening a local shell is not heavy
    and done so infrequently that they do not need a persistent channel
    '''

    def __init__(self):
        ''' Initialize the local channel. script_dir is required by set to a default.

        KwArgs:
            - script_dir (string): Directory to place scripts
        '''
        self.script_dir = None

    def execute_wait(self, cmd, walltime=None):
        ''' Synchronously execute a commandline string on the shell.

        Args:
            - cmd (string) : Commandline string to execute
            - walltime (int) : walltime in seconds

        Returns:
            - retcode : Return code from the execution
            - stdout  : stdout string
            - stderr  : stderr string
        '''
        try:
            logger.debug("Creating process with command '%s'", cmd)
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
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

    @property
    def script_dir(self):
        return self._script_dir

    @script_dir.setter
    def script_dir(self, value):
        if value is not None:
            value = os.path.abspath(value)
        self._script_dir = value
