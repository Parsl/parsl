import getpass
import logging

import paramiko
from parsl.channels.ssh.ssh import SSHChannel

logger = logging.getLogger(__name__)


class SSHInteractiveLoginChannel(SSHChannel):
    """SSH persistent channel. This enables remote execution on sites
    accessible via ssh. This channel supports interactive login and is appropriate when
    keys are not set up.
    """

    def __init__(self, hostname, username=None, password=None, script_dir=None, envs=None):
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
        self.hostname = hostname
        self.username = username
        self.password = password

        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.load_system_host_keys()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.script_dir = script_dir

        self.envs = {}
        if envs is not None:
            self.envs = envs

        try:
            self.ssh_client.connect(
                hostname, username=username, password=password, allow_agent=True
            )

        except Exception:
            logger.debug("Caught the SSHException in SSHInteractive")
            pass
        '''
        except paramiko.BadHostKeyException as e:
            raise BadHostKeyException(e, self.hostname)

        except paramiko.AuthenticationException as e:
            raise AuthException(e, self.hostname)

        except paramiko.SSHException as e:
            logger.debug("Caught the SSHException in SSHInteractive")
            pass

        except Exception as e:
            raise SSHException(e, self.hostname)
        '''

        transport = self.ssh_client.get_transport()

        il_password = getpass.getpass('Enter {0} Logon password :'.format(hostname))
        transport.auth_password(username, il_password)

        self.sftp_client = paramiko.SFTPClient.from_transport(transport)
