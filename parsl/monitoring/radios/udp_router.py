from __future__ import annotations

import logging
import multiprocessing.queues as mpq
import os
import pickle
import socket
import time
from multiprocessing.synchronize import Event
from typing import Optional

import typeguard

from parsl.log_utils import set_file_logger
from parsl.monitoring.radios.multiprocessing import MultiprocessingQueueRadioSender
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

logger = logging.getLogger(__name__)


class MonitoringRouter:

    def __init__(self,
                 *,
                 hub_address: str,
                 udp_port: Optional[int] = None,

                 monitoring_hub_address: str = "127.0.0.1",
                 run_dir: str = ".",
                 logging_level: int = logging.INFO,
                 atexit_timeout: int = 3,   # in seconds
                 resource_msgs: mpq.Queue,
                 exit_event: Event,
                 ):
        """ Initializes a monitoring configuration class.

        Parameters
        ----------
        hub_address : str
             The ip address at which the workers will be able to reach the Hub.
        udp_port : int
             The specific port at which workers will be able to reach the Hub via UDP. Default: None
        run_dir : str
             Parsl log directory paths. Logs and temp files go here. Default: '.'
        logging_level : int
             Logging level as defined in the logging module. Default: logging.INFO
        atexit_timeout : float, optional
            The amount of time in seconds to terminate the hub without receiving any messages, after the last dfk workflow message is received.
        resource_msgs : multiprocessing.Queue
            A multiprocessing queue to receive messages to be routed onwards to the database process
        exit_event : Event
            An event that the main Parsl process will set to signal that the monitoring router should shut down.
        """
        os.makedirs(run_dir, exist_ok=True)
        self.logger = set_file_logger(f"{run_dir}/monitoring_udp_router.log",
                                      name="monitoring_router",
                                      level=logging_level)
        self.logger.debug("Monitoring router starting")

        self.hub_address = hub_address
        self.atexit_timeout = atexit_timeout

        self.loop_freq = 10.0  # milliseconds

        # Initialize the UDP socket
        self.udp_sock = socket.socket(socket.AF_INET,
                                      socket.SOCK_DGRAM,
                                      socket.IPPROTO_UDP)

        # We are trying to bind to all interfaces with 0.0.0.0
        if not udp_port:
            self.udp_sock.bind(('0.0.0.0', 0))
            self.udp_port = self.udp_sock.getsockname()[1]
        else:
            self.udp_port = udp_port
            try:
                self.udp_sock.bind(('0.0.0.0', self.udp_port))
            except Exception as e:
                raise RuntimeError(f"Could not bind to udp_port {udp_port} because: {e}")
        self.udp_sock.settimeout(self.loop_freq / 1000)
        self.logger.info("Initialized the UDP socket on 0.0.0.0:{}".format(self.udp_port))

        self.target_radio = MultiprocessingQueueRadioSender(resource_msgs)
        self.exit_event = exit_event

    @wrap_with_logs(target="monitoring_router")
    def start(self) -> None:
        self.logger.info("Starting UDP listener")
        try:
            while not self.exit_event.is_set():
                try:
                    data, addr = self.udp_sock.recvfrom(2048)
                    resource_msg = pickle.loads(data)
                    self.logger.debug("Got UDP Message from {}: {}".format(addr, resource_msg))
                    self.target_radio.send(resource_msg)
                except socket.timeout:
                    pass

            self.logger.info("UDP listener draining")
            last_msg_received_time = time.time()
            while time.time() - last_msg_received_time < self.atexit_timeout:
                try:
                    data, addr = self.udp_sock.recvfrom(2048)
                    msg = pickle.loads(data)
                    self.logger.debug("Got UDP Message from {}: {}".format(addr, msg))
                    self.target_radio.send(msg)
                    last_msg_received_time = time.time()
                except socket.timeout:
                    pass

            self.logger.info("UDP listener finishing normally")
        finally:
            self.logger.info("UDP listener finished")


@wrap_with_logs
@typeguard.typechecked
def udp_router_starter(*,
                       comm_q: mpq.Queue,
                       resource_msgs: mpq.Queue,
                       exit_event: Event,

                       hub_address: str,
                       udp_port: Optional[int],

                       run_dir: str,
                       logging_level: int) -> None:
    setproctitle("parsl: monitoring UDP router")
    try:
        router = MonitoringRouter(hub_address=hub_address,
                                  udp_port=udp_port,
                                  run_dir=run_dir,
                                  logging_level=logging_level,
                                  resource_msgs=resource_msgs,
                                  exit_event=exit_event)
    except Exception as e:
        logger.error("MonitoringRouter construction failed.", exc_info=True)
        comm_q.put(f"Monitoring router construction failed: {e}")
    else:
        comm_q.put(router.udp_port)

        router.logger.info("Starting MonitoringRouter in router_starter")
        try:
            router.start()
        except Exception:
            router.logger.exception("UDP router start exception")
