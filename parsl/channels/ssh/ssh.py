import errno
import logging
import os
import typeguard

import paramiko
from parsl.channels.base import Channel
from parsl.channels.errors import BadHostKeyException, AuthException, SSHException, BadScriptPath, BadPermsScriptPath, FileCopyException
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)

from typing import Any, Dict, List, Tuple, Optional


class NoAuthSSHClient(paramiko.SSHClient):
    def _auth(self, username: str, *args: List[Any]) -> None:
        # swapped _internal variable for get_transport accessor
        # method that I'm assuming without checking does the
        # same thing.
        transport = self.get_transport()
        if transport is None:
            raise RuntimeError("Expected a transport to be available")
        transport.auth_none(username)


class SSHChannel(Channel, RepresentationMixin):
    ''' SSH persistent channel. This enables remote execution on sites
    accessible via ssh. It is assumed that the user has setup host keys
    so as to ssh to the remote host. Which goes to say that the following
    test on the commandline should work:

    >>> ssh <username>@<hostname>

    '''

    @typeguard.typechecked
    def __init__(self,
                 hostname: str,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 script_dir: Optional[str] = None,
                 envs: Optional[Dict[str, str]] = None,
                 gssapi_auth: bool = False,
                 skip_auth: bool = False,
                 port: int = 22,
                 key_filename: Optional[str] = None,
                 host_keys_filename: Optional[str] = None):
        ''' Initialize a persistent connection to the remote system.
        We should know at this point whether ssh connectivity is possible

        Args:
            - hostname (String) : Hostname

        KWargs:
            - username (string) : Username on remote system
            - password (string) : Password for remote system
            - port : The port designated for the ssh connection. Default is 22.
            - script_dir (string) : Full path to a script dir where
              generated scripts could be sent to.
            - envs (dict) : A dictionary of environment variables to be set when executing commands
            - key_filename (string or list): the filename, or list of filenames, of optional private key(s)

        Raises:
        '''

        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port

        # if script_dir is a `str`, which it is from Channel, then can't
        # assign None to it. Here and the property accessors are changed
        # in benc-mypy to raise an error ratehr than return a None,
        # because Channel-using code assumes that script_dir will always
        # return a string and not a None. That assumption is not otherwise
        # guaranteed by the type-system...
        self._script_dir = None
        if script_dir:
            self.script_dir = script_dir

        self.skip_auth = skip_auth
        self.gssapi_auth = gssapi_auth
        self.key_filename = key_filename
        self.host_keys_filename = host_keys_filename

        if self.skip_auth:
            self.ssh_client: paramiko.SSHClient = NoAuthSSHClient()
        else:
            self.ssh_client = paramiko.SSHClient()
        self.ssh_client.load_system_host_keys(filename=host_keys_filename)
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.sftp_client: Optional[paramiko.SFTPClient] = None

        self.envs = {}  # type: Dict[str, str]
        if envs is not None:
            self.envs = envs

    def _is_connected(self) -> bool:
        transport = self.ssh_client.get_transport() if self.ssh_client else None
        return bool(transport and transport.is_active())

    def _connect(self) -> None:
        if not self._is_connected():
            logger.debug(f"connecting to {self.hostname}:{self.port}")
            try:
                self.ssh_client.connect(
                    self.hostname,
                    username=self.username,
                    password=self.password,
                    port=self.port,
                    allow_agent=True,
                    gss_auth=self.gssapi_auth,
                    gss_kex=self.gssapi_auth,
                    key_filename=self.key_filename
                )
                transport = self.ssh_client.get_transport()
                if not transport:
                    raise RuntimeError("SSH client transport is None, despite connecting")
                self.sftp_client = paramiko.SFTPClient.from_transport(transport)

            except paramiko.BadHostKeyException as e:
                raise BadHostKeyException(e, self.hostname)

            except paramiko.AuthenticationException as e:
                raise AuthException(e, self.hostname)

            except paramiko.SSHException as e:
                raise SSHException(e, self.hostname)

            except Exception as e:
                raise SSHException(e, self.hostname)

    def _valid_sftp_client(self) -> paramiko.SFTPClient:
        self._connect()
        if self.sftp_client is None:
            raise RuntimeError("Internal consistency error: self.sftp_client should be valid but is not")
        return self.sftp_client

    def _valid_ssh_client(self) -> paramiko.SSHClient:
        self._connect()
        return self.ssh_client

    def prepend_envs(self, cmd: str, env: Dict[str, str] = {}) -> str:
        env.update(self.envs)

        if len(env.keys()) > 0:
            env_vars = ' '.join(['{}={}'.format(key, value) for key, value in env.items()])
            return 'env {0} {1}'.format(env_vars, cmd)
        return cmd

    def execute_wait(self, cmd: str, walltime: int = 2, envs: Dict[str, str] = {}) -> Tuple[int, str, str]:
        ''' Synchronously execute a commandline string on the shell.

        Args:
            - cmd (string) : Commandline string to execute
            - walltime (int) : walltime in seconds

        Kwargs:
            - envs (dict) : Dictionary of env variables

        Returns:
            - retcode : Return code from the execution, -1 on fail
            - stdout  : stdout string
            - stderr  : stderr string

        Raises:
        None.
        '''

        # Execute the command
        stdin, stdout, stderr = self._valid_ssh_client().exec_command(
            self.prepend_envs(cmd, envs), bufsize=-1, timeout=walltime
        )
        # Block on exit status from the command
        exit_status = stdout.channel.recv_exit_status()
        return exit_status, stdout.read().decode("utf-8"), stderr.read().decode("utf-8")

    def push_file(self, local_source: str, remote_dir: str) -> str:
        ''' Transport a local file to a directory on a remote machine

        Args:
            - local_source (string): Path
            - remote_dir (string): Remote path

        Returns:
            - str: Path to copied file on remote machine

        Raises:
            - BadScriptPath : if script path on the remote side is bad
            - BadPermsScriptPath : You do not have perms to make the channel script dir
            - FileCopyException : FileCopy failed.

        '''
        remote_dest = os.path.join(remote_dir, os.path.basename(local_source))

        try:
            self.makedirs(remote_dir, exist_ok=True)
        except IOError as e:
            logger.exception("Pushing {0} to {1} failed".format(local_source, remote_dir))
            if e.errno == 2:
                raise BadScriptPath(e, self.hostname)
            elif e.errno == 13:
                raise BadPermsScriptPath(e, self.hostname)
            else:
                logger.exception("File push failed due to SFTP client failure")
                raise FileCopyException(e, self.hostname)
        try:
            self._valid_sftp_client().put(local_source, remote_dest, confirm=True)
            # Set perm because some systems require the script to be executable
            self._valid_sftp_client().chmod(remote_dest, 0o700)
        except Exception as e:
            logger.exception("File push from local source {} to remote destination {} failed".format(
                local_source, remote_dest))
            raise FileCopyException(e, self.hostname)

        return remote_dest

    def pull_file(self, remote_source: str, local_dir: str) -> str:
        ''' Transport file on the remote side to a local directory

        Args:
            - remote_source (string): remote_source
            - local_dir (string): Local directory to copy to


        Returns:
            - str: Local path to file

        Raises:
            - FileExists : Name collision at local directory.
            - FileCopyException : FileCopy failed.
        '''

        local_dest = os.path.join(local_dir, os.path.basename(remote_source))

        try:
            os.makedirs(local_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                logger.exception("Failed to create local_dir: {0}".format(local_dir))
                raise BadScriptPath(e, self.hostname)

        try:
            self._valid_sftp_client().get(remote_source, local_dest)
        except Exception as e:
            logger.exception("File pull failed")
            raise FileCopyException(e, self.hostname)

        return local_dest

    def close(self) -> bool:
        if self._is_connected():
            self.ssh_client.close()
        return True

    def isdir(self, path: str) -> bool:
        """Return true if the path refers to an existing directory.

        Parameters
        ----------
        path : str
            Path of directory on the remote side to check.
        """
        result = True
        try:
            self._valid_sftp_client().lstat(path)
        except FileNotFoundError:
            result = False

        return result

    def makedirs(self, path: str, mode: int = 0o700, exist_ok: bool = False) -> None:
        """Create a directory on the remote side.

        If intermediate directories do not exist, they will be created.

        Parameters
        ----------
        path : str
            Path of directory on the remote side to create.
        mode : int
            Permissions (posix-style) for the newly-created directory.
        exist_ok : bool
            If False, raise an OSError if the target directory already exists.
        """
        if exist_ok is False and self.isdir(path):
            raise OSError('Target directory {} already exists'.format(path))

        self.execute_wait('mkdir -p {}'.format(path))
        self._valid_sftp_client().chmod(path, mode)

    def abspath(self, path: str) -> str:
        """Return the absolute path on the remote side.

        Parameters
        ----------
        path : str
            Path for which the absolute path will be returned.
        """
        return self._valid_sftp_client().normalize(path)

    @property
    def script_dir(self) -> str:
        if self._script_dir:
            return self._script_dir
        else:
            raise RuntimeError("scriptdir was not set")

    @script_dir.setter
    def script_dir(self, value: Optional[str]) -> None:
        self._script_dir = value
