import os
import logging
import paramiko
import getpass

from libsubmit.channels.ssh.ssh import SshChannel
from libsubmit.channels.channel_base import Channel
from libsubmit.channels.errors import *

logger = logging.getLogger(__name__)

class SshILChannel (SshChannel):
    ''' Ssh persistent channel. This enables remote execution on sites
    accessible via ssh. It is assumed that the user has setup host keys
    so as to ssh to the remote host. Which goes to say that the following
    test on the commandline should work :

    >>> ssh <username>@<hostname>

    '''

    def __init__ (self, hostname, username=None, password=None,
                  scriptDir=None, **kwargs):
        ''' Initialize a persistent connection to the remote system.
        We should know at this point whether ssh connectivity is possible

        Args:
            - hostname (String) : Hostname

        KWargs:
            - username (string) : Username on remote system
            - password (string) : Password for remote system
            - channel_script_dir (string) : Full path to a script dir where
              generated scripts could be sent to.

        Raises:
        '''

        self.hostname = hostname
        self.username = username
        self.password = password
        self.kwargs = kwargs

        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.load_system_host_keys()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if scriptDir:
            self.channel_script_dir = scriptDir
        else:
            self.channel_script_dir = "/tmp/{0}/scripts/".format(getpass.getuser())

        try :
            self.ssh_client.connect(hostname,
                                    username=username,
                                    password=password,
                                    allow_agent=True)

        except Exception as e:
            logger.debug("Caught the SSHException in SshInteractive")
            pass

        '''
        except paramiko.BadHostKeyException as e:
            raise BadHostKeyException(e, self.hostname)

        except paramiko.AuthenticationException as e:
            raise AuthException(e, self.hostname)

        except paramiko.SSHException as e:
            logger.debug("Caught the SSHException in SshInteractive")
            pass

        except Exception as e:
            raise SSHException(e, self.hostname)
        '''

        transport = self.ssh_client.get_transport()

        il_password = getpass.getpass('Enter {0} Logon password :'.format(hostname))
        transport.auth_password(username, il_password)

        self.sftp_client = paramiko.SFTPClient.from_transport(transport)

