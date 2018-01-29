import os
import subprocess
import time
import random
import logging
import signal

from parsl.executors.errors import *

logger = logging.getLogger(__name__)


class Controller(object):

    ''' Start and maintain a ipyparallel controller
    '''

    def __init__(self, publicIp=None, port=None, portRange="", reuse=False,
                 log=True, ipythonDir="~/.ipython", mode="auto", profile=None):
        ''' Initialize ipython controllers to the user specified configs

        The specifig config sections that will be used by this are in the dict
        config["controller"]

        KWargs:
              - publicIp (string): internal_ip, specify if this would be difficult to autofind.
              - interfaces (string): interfaces for zero_mq to listen on
                     Default: "*"
              - port (int): port
                     Default: A random port between 50000, 60000
              - portRange (string): <string <port_min>,<port_max>,
                     Default: 50000 - 60000
              - reuse (bool): <Reuse an existing controller>
              - ipythonDir (string) : IPython directory for IPP to store config files.
                     This will be overriden by the auto controller start.
                     Default: "~/.ipython"
              - profile (str)  : Name of the profile/site from the sites in the config.
                     Default : None
              - mode (str) : If "auto" the default automatic behavior is maintained.
                             If "manual" the controller is assumed to be created by the user.
        '''

        logger.debug("Starting ipcontroller, baseDir:%s" % ipythonDir)

        self.mode = mode
        self.range_min = 50000
        self.range_max = 60000
        self.reuse = reuse
        self.port = ''
        self.ipythonDir = None
        self.profile = 'default'
        ipp_basedir = ''
        reuse_string = ''
        profile_string = ''

        if mode == "manual":
            return
        if self.reuse:
            reuse_string = '--reuse'

        if ipythonDir:
            self.ipythonDir = os.path.abspath(os.path.expanduser(ipythonDir))
            ipp_basedir = '--ipython-dir={0}'.format(self.ipythonDir)

        if profile:
            # Profile string is set only if non-"default" profile is specified
            self.profile = profile
            profile_string = '--profile={0}'.format(profile)

        # If portRange is specified pick a random port from that range
        try:
            if portRange:
                self.range_min, self.range_max = map(int, portRange.split(','))
                self.port = '--port={0}'.format(random.randint(self.range_min, self.range_max))
        except Exception as e:
            msg = "Bad portRange. IPPController needs portA,portB. Got {0}. Error {1}".format(portRange,
                                                                                              e)
            logger.error(msg)
            raise ControllerErr(msg)

        # If port is specified use it, this will override the portRange
        if port:
            self.port = '--port={0}'.format(port)

        # We have a publicIp default always = None
        if publicIp:
            self.publicIp = '--location={0}'.format(publicIp)
        else:
            self.publicIp = ''

        if log:
            stdout = open(os.path.join(self.ipythonDir, "{0}.controller.out".format(self.profile)), 'w')
            stderr = open(os.path.join(self.ipythonDir, "{0}.controller.err".format(self.profile)), 'w')
        else:
            stdout = open(os.devnull, 'w')
            stderr = open(os.devnull, 'w')

        interfaces = '--ip=*'

        try:
            opts = ['ipcontroller', ipp_basedir, interfaces, profile_string,
                    reuse_string, self.port, self.publicIp]
            logger.debug("Start opts: %s" % opts)
            self.proc = subprocess.Popen(opts, stdout=stdout, stderr=stderr, preexec_fn=os.setsid)

        except FileNotFoundError as e:
            msg = "Could not find ipcontroller. Please make sure that ipyparallel is installed and available in your env"
            logger.error(msg)
            raise ControllerErr(msg)

        except Exception as e:
            msg = "IPPController failed to start: {0}".format(e)
            logger.error(msg)
            raise ControllerErr(msg)

        return

    @property
    def engine_file(self):
        ''' Engine_file attribute specifies the file path to the specific ipython_dir/profile folders in
        which the ipcontroller-engine.json file is stored.

        Returns :
              - str, File path to engine file
        '''
        return os.path.join(self.ipythonDir,
                            'profile_{0}'.format(self.profile),
                            'security/ipcontroller-engine.json')

    @property
    def client_file(self):
        ''' Client_file attribute specifies the file path to the specific ipython_dir/profile folders in
        which the ipcontroller-client.json file is stored.

        Returns :
              - str, File path to client file
        '''

        return os.path.join(self.ipythonDir,
                            'profile_{0}'.format(self.profile),
                            'security/ipcontroller-client.json')

    def close(self):
        ''' Terminate the controller process and it's child processes.

        Args:
              - None
        '''
        if self.reuse:
            logger.debug("Ipcontroller not shutting down: reuse enabled")
            return

        if self.mode == "manual":
            logger.debug("Ipcontroller not shutting down: Manual mode")
            return

        try:
            pgid = os.getpgid(self.proc.pid)
            os.killpg(pgid, signal.SIGTERM)
            time.sleep(0.2)
            os.killpg(pgid, signal.SIGKILL)
            try:
                self.proc.wait(timeout=1)
                x = self.proc.returncode
                logger.debug("Controller exited with {0}".format(x))
            except subprocess.TimeoutExpired:
                logger.warn("Ipcontroller process:{0} cleanup failed. May require manual cleanup".format(self.proc.pid))

        except Exception as e:
            logger.warn("Failed to kill the ipcontroller process[{0}]: {1}".format(self.proc.pid,
                                                                                   e))
