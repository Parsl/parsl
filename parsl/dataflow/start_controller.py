import os
import sys
import subprocess
import time
import random
import logging
import signal

from parsl.dataflow.error import *

logger = logging.getLogger(__name__)

class Controller(object):

    ''' Start and maintain a ipyparallel controller
    '''

    def __init__ (self, publicIp="*", port=None, portRange="", reuse=False, log=True):
        ''' Initialize ipython controllers to the user specified configs

        The specifig config sections that will be used by this are in the dict
        config["controller"]

        KWargs:
              "publicIp" (string): <internal_ip |Default: "*">,
              "interfaces" (string): <interfaces for zero_mq to listen on| Default: "*">,
              "port" (int): <port |Default: rand between 50000, 60000,
              "portRange" (string): <string <port_min>,<port_max>,
              "reuse" (bool): <Reuse an existing controller>

        '''
        logger.debug("Starting ipcontroller")

        self.range_min = 50000
        self.range_max = 60000
        self.reuse     = reuse
        self.port      = ''
        reuse_string   = ''

        if self.reuse:
            reuse_string = '--reuse'

        self.public_ip = publicIp

        # If portRange is specified pick a random port from that range
        try:
            if portRange :
                self.range_min, self.range_max = portRange.split(',')
                self.port = '--port={0}'.format(random.randint(self.range_min, self.range_max))
        except Exception as e:
            msg = "Bad portRange. IPPController needs portA,portB. Got {0}".format(portRange)
            logger.error(msg)
            raise ControllerErr(msg)

        # If port is specified use it, this will override the portRange
        if port :
            self.port = '--port={0}'.format(port)

        # We have a publicIp default always, ie "*"
        self.publicIp = '--ip={0}'.format(publicIp)

        if log :
            stdout = open(".controller.out", 'w')
            stderr = open(".controller.err", 'w')
        else:
            stdout = open(os.devnull, 'w')
            stderr = open(os.devnull, 'w')

        try:
            opts = ['ipcontroller', reuse_string, self.port, self.publicIp]
            logger.debug("Start opts: %s" % opts)
            self.proc = subprocess.Popen(opts, stdout=stdout, stderr=stderr, preexec_fn=os.setsid)
        except Exception as e:
            msg = "IPPController failed to start: {0}".format(e)
            logger.error(msg)
            raise ControllerErr(msg)

        return


    def close(self):
        ''' Process id of the controller to kill
        '''
        if self.reuse :
            logger.debug("Ipcontroller not shutting down: reuse enabled")
            return

        try:
            pgid = os.getpgpid(self.proc.pid)
            os.killpg(pgid, signal.SIGTERM)
            time.sleep(0.1)
            os.killpg(pgid, signal.SIGKILL)
        except:
            logger.error("Failed to kill the ipcontroller process[{0}]".format(self.proc.pid))
