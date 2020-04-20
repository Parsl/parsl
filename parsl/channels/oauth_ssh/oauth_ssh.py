import logging
import paramiko
import socket

from parsl.errors import OptionalModuleMissing
from parsl.channels.ssh.ssh import SSHChannel

try:
    from oauth_ssh.ssh_service import SSHService
    from oauth_ssh.oauth_ssh_token import find_access_token
    _oauth_ssh_enabled = True
except (ImportError, NameError):
    _oauth_ssh_enabled = False


logger = logging.getLogger(__name__)


class OAuthSSHChannel(SSHChannel):
    """SSH persistent channel. This enables remote execution on sites
    accessible via ssh. This channel uses Globus based OAuth tokens for authentication.
    """

    def __init__(self, hostname, username=None, script_dir=None, envs=None, port=22):
        ''' Initialize a persistent connection to the remote system.
        We should know at this point whether ssh connectivity is possible

        Args:
            - hostname (String) : Hostname

        KWargs:
            - username (string) : Username on remote system
            - script_dir (string) : Full path to a script dir where
              generated scripts could be sent to.
            - envs (dict) : A dictionary of env variables to be set when executing commands
            - port (int) : Port at which the SSHService is running

        Raises:
        '''
        if not _oauth_ssh_enabled:
            raise OptionalModuleMissing(['oauth_ssh'],
                                        "OauthSSHChannel requires oauth_ssh module and config.")

        self.hostname = hostname
        self.username = username
        self.script_dir = script_dir
        self.port = port
        self.envs = {}
        if envs is not None:
            self.envs = envs

        try:
            access_token = find_access_token(hostname)
        except Exception:
            logger.exception("Failed to find the access token for {}".format(hostname))
            raise

        try:
            self.service = SSHService(hostname, port)
            self.transport = self.service.login(access_token, username)

        except Exception:
            logger.exception("Caught an exception in the OAuth authentication step with {}".format(hostname))
            raise

        self.sftp_client = paramiko.SFTPClient.from_transport(self.transport)

    def execute_wait(self, cmd, walltime=60, envs={}):
        ''' Synchronously execute a commandline string on the shell.

        This command does *NOT* honor walltime currently.

        Args:
            - cmd (string) : Commandline string to execute

        Kwargs:
            - walltime (int) : walltime in seconds
            - envs (dict) : Dictionary of env variables

        Returns:
            - retcode : Return code from the execution, -1 on fail
            - stdout  : stdout string
            - stderr  : stderr string

        Raises:
        None.
        '''

        session = self.transport.open_session()
        session.setblocking(0)

        nbytes = 1024
        session.exec_command(self.prepend_envs(cmd, envs))
        session.settimeout(walltime)

        try:
            # Wait until command is executed
            exit_status = session.recv_exit_status()

            stdout = session.recv(nbytes).decode('utf-8')
            stderr = session.recv_stderr(nbytes).decode('utf-8')

        except socket.timeout:
            logger.exception("Command failed to execute without timeout limit on {}".format(self))
            raise

        return exit_status, stdout, stderr

    def close(self):
        return self.transport.close()
