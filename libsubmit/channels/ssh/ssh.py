import os
import logging
import paramiko

from libsubmit.channels.channel_base import Channel
from libsubmit.channels.errors import *

logger = logging.getLogger(__name__)

class SshChannel ():
    ''' Ssh persistent channel. This enables remote execution on sites
    accessible via ssh. It is assumed that the user has setup host keys
    so as to ssh to the remote host. Which goes to say that the following
    test on the commandline should work :

    >>> ssh <username>@<hostname>

    '''

    def __init__ (self, hostname, username=None, password=None, **kwargs):
        ''' Initialize a persistent connection to the remote system.
        We should know at this point whether ssh connectivity is possible

        Raises:
        '''

        self.hostname = hostname
        self.username = username
        self.password = password
        self.kwargs = kwargs

        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.load_system_host_keys()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try :
            self.ssh_client.connect(hostname,
                                    username=username,
                                    password=password,
                                    allow_agent=True)
            t = self.ssh_client.get_transport()
            self.sftp_client = paramiko.SFTPClient.from_transport(t)

        except paramiko.BadHostKeyException as e:
            raise BadHostKeyException(e)

        except paramiko.AuthenticationException as e:
            raise AuthException(e)
        #self.sftp_client = paramiko.SFTPClient.from_transport(self.transport)

    def execute_wait(self, cmd, walltime=2, envs={}):

        # Execute the command
        stdin, stdout, stderr = self.ssh_client.exec_command(cmd,
                                                             bufsize=-1,
                                                             timeout=walltime,
                                                             environment=envs)
        # Block on exit status from the command
        exit_status = stdout.channel.recv_exit_status()
        return  exit_status, stdout.read().decode("utf-8"), stderr.read().decode("utf-8")

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

        # Execute the command
        stdin, stdout, stderr = self.ssh_client.exec_command(cmd,
                                                             bufsize=-1,
                                                             timeout=walltime,
                                                             environment=envs)
        # Block on exit status from the command
        return  None, stdout, stderr

    def push_file(self, local_source, remote_dir):
        ''' Execute asynchronousely without waiting for exitcode
        Args:
            - local_source (string): Path
            - remote_dir (string): Remote path

        KWargs:
            - envs (dict): A dictionary of env variables

        Returns:
            - None, stdout (readable stream), stderr (readable stream)

        Raises:
            - ChannelExecFailed (reason)
        '''
        status = False
        remote_dest = remote_dir + '/' + os.path.basename(local_source)

        try:
            self.sftp_client.mkdir(remote_dir)
        except IOError as e:
            if e.errno == 2:
                raise BadScriptPath(e, self.hostname)
            elif e.errno == 13:
                raise BadPermsScriptPath(e, self.hostname)
            elif e.errno == None:
                # Directory already exists. Nothing to do
                pass
            else :
                print("Caught unknown error with script_dir e:",e)

        try:
            s = self.sftp_client.put(local_source, remote_dest, confirm=True)
            status = True
        except IOError as e:
            print ("Caught IOerror : {0}", e)

        return status

    def close(self):
        return self.ssh_client.close()
