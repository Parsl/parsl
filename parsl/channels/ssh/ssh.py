import errno
import logging
import os

from parsl.channels.base import Channel
from parsl.channels.errors import (
    AuthException,
    BadHostKeyException,
    BadPermsScriptPath,
    BadScriptPath,
    FileCopyException,
    SSHException,
)
from parsl.errors import OptionalModuleMissing
from parsl.utils import RepresentationMixin

try:
    import paramiko
    _ssh_enabled = True
except (ImportError, NameError, FileNotFoundError):
    _ssh_enabled = False


logger = logging.getLogger(__name__)


if _ssh_enabled:
    class NoAuthSSHClient(paramiko.SSHClient):
        def _auth(self, username, *args):
            self._transport.auth_none(username)
            return


class DeprecatedSSHChannel(Channel, RepresentationMixin):
    ''' SSH persistent channel. This enables remote execution on sites
    accessible via ssh. It is assumed that the user has setup host keys
    so as to ssh to the remote host. Which goes to say that the following
    test on the commandline should work:

    >>> ssh <username>@<hostname>

    '''

    def __init__(self, hostname, username=None, password=None, script_dir=None, envs=None,
                 gssapi_auth=False, skip_auth=False, port=22, key_filename=None, host_keys_filename=None):
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
        if not _ssh_enabled:
            raise OptionalModuleMissing(['ssh'],
                                        "SSHChannel requires the ssh module and config.")

        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.script_dir = script_dir
        self.skip_auth = skip_auth
        self.gssapi_auth = gssapi_auth
        self.key_filename = key_filename
        self.host_keys_filename = host_keys_filename

        if self.skip_auth:
            self.ssh_client = NoAuthSSHClient()
        else:
            self.ssh_client = paramiko.SSHClient()
        self.ssh_client.load_system_host_keys(filename=host_keys_filename)
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.sftp_client = None

        self.envs = {}
        if envs is not None:
            self.envs = envs

    def _is_connected(self):
        transport = self.ssh_client.get_transport() if self.ssh_client else None
        return transport and transport.is_active()

    def _connect(self):
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
                self.sftp_client = paramiko.SFTPClient.from_transport(transport)

            except paramiko.BadHostKeyException as e:
                raise BadHostKeyException(e, self.hostname)

            except paramiko.AuthenticationException as e:
                raise AuthException(e, self.hostname)

            except paramiko.SSHException as e:
                raise SSHException(e, self.hostname)

            except Exception as e:
                raise SSHException(e, self.hostname)

    def _valid_sftp_client(self):
        self._connect()
        return self.sftp_client

    def _valid_ssh_client(self):
        self._connect()
        return self.ssh_client

    def prepend_envs(self, cmd, env={}):
        env.update(self.envs)

        if len(env.keys()) > 0:
            env_vars = ' '.join(['{}={}'.format(key, value) for key, value in env.items()])
            return 'env {0} {1}'.format(env_vars, cmd)
        return cmd

    def execute_wait(self, cmd, walltime=2, envs={}):
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

    def push_file(self, local_source, remote_dir):
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

    def pull_file(self, remote_source, local_dir):
        ''' Transport file on the remote side to a local directory

        Args:
            - remote_source (string): remote_source
            - local_dir (string): Local directory to copy to


        Returns:
            - str: Local path to file

        Raises:
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

    def close(self) -> None:
        if self._is_connected():
            transport = self.ssh_client.get_transport()
            self.ssh_client.close()

            # ssh_client.close calls transport.close, but transport.close does
            # not always wait for the transport thread to be stopped. See impl
            # of Transport.close in paramiko and issue
            # https://github.com/paramiko/paramiko/issues/520
            logger.debug("Waiting for transport thread to stop")
            transport.join(30)
            if transport.is_alive():
                logger.warning("SSH transport thread did not shut down")
            else:
                logger.debug("SSH transport thread stopped")

    def isdir(self, path):
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

    def makedirs(self, path, mode=0o700, exist_ok=False):
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

    @property
    def script_dir(self):
        return self._script_dir

    @script_dir.setter
    def script_dir(self, value):
        self._script_dir = value
