import getpass
import logging
import select
import paramiko

from parsl.errors import OptionalModuleMissing
from parsl.channels.ssh.ssh import SSHChannel

try:
    from oauth_ssh.ssh_service import SSHService
    from oauth_ssh.oauth_ssh_token import find_access_token
    _oauth_ssh_enabled = True
except (ImportError, NameError, FileNotFoundError):
    _oauth_ssh_enabled = False


logger = logging.getLogger(__name__)


class OAuthSSHChannel(SSHChannel):
    """SSH persistent channel. This enables remote execution on sites
    accessible via ssh. This channel uses Globus based OAuth tokens for authentication.
    """

    def __init__(self, hostname, username=None, script_dir=None, envs=None, port=22, **kwargs):
        ''' Initialize a persistent connection to the remote system.
        We should know at this point whether ssh connectivity is possible

        Args:
            - hostname (String) : Hostname

        KWargs:
            - username (string) : Username on remote system
            - password (string) : Password for remote system
            - script_dir (string) : Full path to a script dir where
              generated scripts could be sent to.
            - envs (dict) : A dictionary of env variables to be set when executing commands

        Raises:
        '''
        if not _oauth_ssh_enabled:
            raise OptionalModuleMissing(['oauth_ssh'],
                                        "OauthSSHChannel requires oauth_ssh module and config.")

        self.hostname = hostname
        self.username = username
        self.kwargs = kwargs
        self.script_dir = script_dir

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
            logger.exception("Caught an in OAuth authentication step with {}".format(hostname))
            raise

        self.sftp_client = paramiko.SFTPClient.from_transport(self.transport)

    def execute_wait(self, cmd, walltime=2, envs={}):
        ''' Synchronously execute a commandline string on the shell.

        This command does *NOT* honor walltime currently.

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

        session = self.transport.open_session()
        session.setblocking(0)

        nbytes = 10240
        session.exec_command(self.prepend_envs(cmd, envs))

        # Wait until command is executed
        exit_status = session.recv_exit_status()
        print("exit_status : ", exit_status, type(exit_status))

        stdout = session.recv(nbytes).decode('utf-8')
        stderr = session.recv_stderr(nbytes).decode('utf-8')

        return exit_status, stdout, stderr

    def execute_no_wait(self, cmd, walltime=2, envs={}):
        ''' Execute asynchronousely without waiting for exitcode

        Args:
            - cmd (string): Commandline string to be executed on the remote side
            - walltime (int): timeout to exec_command

        KWargs:
            - envs (dict): A dictionary of env variables

        Returns:
            - None, stdout (readable stream), stderr (readable stream)

        Raises:
            - ChannelExecFailed (reason)
        '''
        session = self.transport.open_session()
        session.setblocking(0)

        nbytes = 10240
        session.exec_command(self.prepend_envs(cmd, envs))

        stdout = session.recv(nbytes).decode('utf-8')
        stderr = session.recv_stderr(nbytes).decode('utf-8')

        return None, stdout, stderr

    def close(self):
        return self.transport.close()
