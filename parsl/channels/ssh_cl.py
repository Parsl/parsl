import os
import time
import logging
import subprocess


logger = logging.getLogger(__name__)

class SshClClient(object):
    '''
    Stop gap while we wait for a better secure remote execution solution.
    '''

    def __init__ (self, username, remote_url, arg_string=None):
        ''' Create the SSH Commandline session
        '''

        self.username =  '{0}@'.format(username) if username else ''
        self.remote_url = remote_url
        self.arg_string = arg_string

        if self.arg_string :
            self.ssh_command = ["ssh", "{0}{1}".format(self.username, self.remote_url)]
        else:
            self.ssh_command = ["ssh", self.arg_string, "{0}{1}".format(self.username, self.remote_url)]

        print(self.ssh_command)


    def execute_wait(self, cmd):
        self.session =  subprocess.Popen(["ssh", "yadunand@swift.rcc.uchicago.edu"],
                                         stdin =subprocess.PIPE,
                                         stdout = subprocess.PIPE,
                                         stderr = subprocess.PIPE,
                                         universal_newlines=True,
                                         bufsize=0)

        self.session.stdin.write(cmd + '\n echo "##STATUS##: $?"\necho "END" | tee /dev/stderr \n')
        print("executing  : ", cmd)

        stdout, stderr, retcode = '', '', None

        prev = None
        for line in self.session.stdout:
            if line == "END\n":
                if prev.startswith("##STATUS##"):
                    retcode = int(prev.split(' ')[1])
                break
            prev = line
            stdout += line

        for line in self.session.stderr:
            if line == "END\n":
                break
            stderr += line

        return retcode, stdout, stderr


if __name__ == "__main__" :

    cl = SshClClient("yadunand", "swift.rcc.uchicago.edu", arg_string='-v')
    retcode, stdout, stderr = cl.execute_wait("ls")
    print("Stdout : ", stdout)
    print("Stderr : ", stderr)
    
    cl.execute_wait("cd /scratch/midway/yadunand; ls")
