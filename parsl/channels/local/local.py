import copy
import logging
import os
import shutil
import subprocess

from parsl.channels.base import Channel
from parsl.channels.errors import FileCopyException
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class LocalChannel(Channel, RepresentationMixin):
    ''' This is not even really a channel, since opening a local shell is not heavy
    and done so infrequently that they do not need a persistent channel
    '''

    def __init__(self):
        ''' Initialize the local channel. script_dir is required by set to a default.

        KwArgs:
            - userhome (string): (default='.') This is provided as a way to override and set a specific userhome
            - envs (dict) : A dictionary of env variables to be set when launching the shell
            - script_dir (string): Directory to place scripts
        '''
        self.userhome = os.path.abspath(".")
        envs = {}
        self.envs = envs
        local_env = os.environ.copy()
        self._envs = copy.deepcopy(local_env)
        self._envs.update(envs)
        self.script_dir = None

    def execute_wait(self, cmd, walltime=None, envs={}):
        ''' Synchronously execute a commandline string on the shell.

        Args:
            - cmd (string) : Commandline string to execute
            - walltime (int) : walltime in seconds, this is not really used now.

        Kwargs:
            - envs (dict) : Dictionary of env variables. This will be used
              to override the envs set at channel initialization.

        Returns:
            - retcode : Return code from the execution, -1 on fail
            - stdout  : stdout string
            - stderr  : stderr string

        Raises:
        None.
        '''
        current_env = copy.deepcopy(self._envs)
        current_env.update(envs)

        try:
            logger.debug("Creating process with command '%s'", cmd)
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.userhome,
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

    def push_file(self, source, dest_dir):
        ''' If the source files dirpath is the same as dest_dir, a copy
        is not necessary, and nothing is done. Else a copy is made.

        Args:
            - source (string) : Path to the source file
            - dest_dir (string) : Path to the directory to which the files is to be copied

        Returns:
            - destination_path (String) : Absolute path of the destination file

        Raises:
            - FileCopyException : If file copy failed.
        '''

        local_dest = os.path.join(dest_dir, os.path.basename(source))

        # Only attempt to copy if the target dir and source dir are different
        logger.debug(f"push file comparing directories: {os.path.dirname(source)} vs {dest_dir}")
        if os.path.dirname(source) != dest_dir:
            try:
                shutil.copyfile(source, local_dest)
                os.chmod(local_dest, 0o700)

            except OSError as e:
                raise FileCopyException(e)

        else:
            os.chmod(local_dest, 0o700)

        return local_dest

    def pull_file(self, remote_source, local_dir):
        return self.push_file(remote_source, local_dir)

    def makedirs(self, path, mode=0o700, exist_ok=False):
        """Create a directory.

        If intermediate directories do not exist, they will be created.

        Parameters
        ----------
        path : str
            Path of directory to create.
        mode : int
            Permissions (posix-style) for the newly-created directory.
        exist_ok : bool
            If False, raise an OSError if the target directory already exists.
        """

        return os.makedirs(path, mode, exist_ok)

    @property
    def script_dir(self):
        return self._script_dir

    @script_dir.setter
    def script_dir(self, value):
        if value is not None:
            value = os.path.abspath(value)
        self._script_dir = value
