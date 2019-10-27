import logging
import os
import random
import signal
import subprocess
import time

from parsl.executors.errors import ControllerError
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class Controller(RepresentationMixin):
    """Start and maintain a IPythonParallel controller.

    Parameters
    ----------

    public_ip : str, optional
        Specific IP address of the controller, as seen from the engines. If `None`, an attempt will
        be made to guess the correct value. Default is None.
    interfaces : str, optional
        Interfaces for ZeroMQ to listen on. Default is "*".
    port : int or str, optional
        Port on which the iPython hub listens for registration. If set to `None`, the IPython default will be used. Default
        is None.
    port_range : str, optional
        The minimum and maximum port values to use, in the format '<min>,<max>' (for example: '50000,60000').
        If this does not equal None, random ports in `port_range` will be selected for all HubFactory listening services.
        This option overrides the port setting value for registration.
    reuse : bool, optional
        Reuse an existing controller.
    ipython_dir : str, optional
        IPython directory for IPythonParallel to store config files. This will be overriden by the auto controller
        start. Default is "~/.ipython".
    profile : str, optional
        Path to an IPython profile to use. Default is 'default'.
    mode : str, optional
        If "auto", controller will be created and managed automatically. If "manual" the controller
        is assumed to be created by the user. Default is auto.
    """
    def __init__(self, public_ip=None, interfaces=None, port=None, port_range=None, reuse=False,
                 log=True, ipython_dir="~/.ipython", mode="auto", profile='default'):
        self.public_ip = public_ip
        self.interfaces = interfaces
        self.port = port
        self.port_range = port_range

        if port_range is not None:
            port_min, port_max = [int(x) for x in port_range.split(',')]
            (
                self.port,
                self.hb_ping, self.hb_pong,
                self.control_client, self.control_engine,
                self.mux_client, self.mux_engine,
                self.task_client, self.task_engine
            ) = random.sample(range(port_min, port_max), 9)

        self.reuse = reuse
        self.log = log
        self.ipython_dir = ipython_dir
        self.mode = mode
        self.profile = profile

    def start(self):
        """Start the controller."""

        if self.mode == "manual":
            return

        if self.ipython_dir != '~/.ipython':
            self.ipython_dir = os.path.abspath(os.path.expanduser(self.ipython_dir))

        if self.log:
            stdout = open(os.path.join(self.ipython_dir, "{0}.controller.out".format(self.profile)), 'w')
            stderr = open(os.path.join(self.ipython_dir, "{0}.controller.err".format(self.profile)), 'w')
        else:
            stdout = open(os.devnull, 'w')
            stderr = open(os.devnull, 'w')

        try:
            opts = [
                'ipcontroller',
                '' if self.ipython_dir == '~/.ipython' else '--ipython-dir={}'.format(self.ipython_dir),
                self.interfaces if self.interfaces is not None else '--ip=*',
                '' if self.profile == 'default' else '--profile={0}'.format(self.profile),
                '--reuse' if self.reuse else '',
                '--location={}'.format(self.public_ip) if self.public_ip else '',
                '--port={}'.format(self.port) if self.port is not None else ''
            ]
            if self.port_range is not None:
                opts += [
                    '--HubFactory.hb={0},{1}'.format(self.hb_ping, self.hb_pong),
                    '--HubFactory.control={0},{1}'.format(self.control_client, self.control_engine),
                    '--HubFactory.mux={0},{1}'.format(self.mux_client, self.mux_engine),
                    '--HubFactory.task={0},{1}'.format(self.task_client, self.task_engine)
                ]
            logger.debug("Starting ipcontroller with '{}'".format(' '.join([str(x) for x in opts])))
            self.proc = subprocess.Popen(opts, stdout=stdout, stderr=stderr, preexec_fn=os.setsid)
        except FileNotFoundError:
            msg = "Could not find ipcontroller. Please make sure that ipyparallel is installed and available in your env"
            logger.error(msg)
            raise ControllerError(msg)
        except Exception as e:
            msg = "IPPController failed to start: {0}".format(e)
            logger.error(msg)
            raise ControllerError(msg)

    @property
    def engine_file(self):
        """Specify path to the ipcontroller-engine.json file.

        This file is stored in in the ipython_dir/profile folders.

        Returns :
              - str, File path to engine file
        """
        return os.path.join(self.ipython_dir,
                            'profile_{0}'.format(self.profile),
                            'security/ipcontroller-engine.json')

    @property
    def client_file(self):
        """Specify path to the ipcontroller-client.json file.

        This file is stored in in the ipython_dir/profile folders.

        Returns :
              - str, File path to client file
        """
        return os.path.join(self.ipython_dir,
                            'profile_{0}'.format(self.profile),
                            'security/ipcontroller-client.json')

    def close(self):
        """Terminate the controller process and its child processes.

        Args:
              - None
        """
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
                if x == 0:
                    logger.debug("Controller exited with {0}".format(x))
                else:
                    logger.error("Controller exited with {0}. May require manual cleanup".format(x))
            except subprocess.TimeoutExpired:
                logger.warning("Ipcontroller process:{0} cleanup failed. May require manual cleanup".format(self.proc.pid))

        except Exception as e:
            logger.warning("Failed to kill the ipcontroller process[{0}]: {1}".format(self.proc.pid, e))
