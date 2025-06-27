from __future__ import annotations

import hmac
import logging
import multiprocessing.queues as mpq
import os
import pickle
import queue
import socket
import time
from multiprocessing.context import SpawnProcess as SpawnProcessType
from multiprocessing.queues import Queue
from multiprocessing.synchronize import Event
from multiprocessing.synchronize import Event as EventType
from typing import Optional, Union

import typeguard

from parsl.log_utils import set_file_logger
from parsl.monitoring.errors import MonitoringRouterStartError
from parsl.monitoring.radios.base import MonitoringRadioReceiver
from parsl.monitoring.radios.multiprocessing import MultiprocessingQueueRadioSender
from parsl.multiprocessing import (
    SizedQueue,
    SpawnEvent,
    SpawnProcess,
    join_terminate_close_proc,
)
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

logger = logging.getLogger(__name__)


class MonitoringRouter:

    def __init__(self,
                 *,
                 udp_port: Optional[int] = None,
                 run_dir: str = ".",
                 logging_level: int = logging.INFO,
                 atexit_timeout: int,   # in seconds
                 resource_msgs: mpq.Queue,
                 exit_event: Event,
                 hmac_key: bytes,
                 hmac_digest: str,
                 ):
        """ Initializes a monitoring configuration class.

        Parameters
        ----------
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
        set_file_logger(f"{run_dir}/monitoring_udp_router.log",
                        level=logging_level)
        logger.debug("Monitoring router starting")

        self.atexit_timeout = atexit_timeout

        self.hmac_key = hmac_key
        self.hmac_digest = hmac_digest

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
        logger.info("Initialized the UDP socket on 0.0.0.0:{}".format(self.udp_port))

        self.target_radio = MultiprocessingQueueRadioSender(resource_msgs)
        self.exit_event = exit_event

    @wrap_with_logs
    def start(self) -> None:
        logger.info("Starting UDP listener")
        try:
            while not self.exit_event.is_set():
                try:
                    self.process_message()
                except socket.timeout:
                    pass

            logger.info("UDP listener draining")
            last_msg_received_time = time.time()
            while time.time() - last_msg_received_time < self.atexit_timeout:
                try:
                    self.process_message()
                    last_msg_received_time = time.time()
                except socket.timeout:
                    pass

            logger.info("UDP listener finishing normally")
        finally:
            logger.info("UDP listener finished")

    def process_message(self) -> None:
        hmdata, addr = self.udp_sock.recvfrom(2048)
        h = hmac.HMAC(key=self.hmac_key, digestmod=self.hmac_digest)
        origin_hmac = hmdata[0:h.digest_size]
        h.update(hmdata[h.digest_size:])
        data = hmdata[h.digest_size:]

        # Check hmac before pickle load.
        # If data is wrong, do not log it because it is suspect,
        # but it should be safe to log the addr, at error level.

        recomputed_hmac = h.digest()

        if not hmac.compare_digest(origin_hmac, recomputed_hmac):
            logger.error("HMAC does not match on received message")
            # No exception, because this can be arbitrary network noise
            # that shouldn't break the receiver.
        else:
            resource_msg = pickle.loads(data)
            logger.debug("Got UDP Message from {}: {}".format(addr, resource_msg))
            self.target_radio.send(resource_msg)


@wrap_with_logs
@typeguard.typechecked
def udp_router_starter(*,
                       comm_q: mpq.Queue,
                       resource_msgs: mpq.Queue,
                       exit_event: Event,

                       udp_port: Optional[int],
                       hmac_key: bytes,
                       run_dir: str,
                       logging_level: int,
                       atexit_timeout: int,
                       hmac_digest: str) -> None:
    setproctitle("parsl: monitoring UDP router")
    try:
        router = MonitoringRouter(udp_port=udp_port,
                                  run_dir=run_dir,
                                  logging_level=logging_level,
                                  resource_msgs=resource_msgs,
                                  exit_event=exit_event,
                                  atexit_timeout=atexit_timeout,
                                  hmac_key=hmac_key,
                                  hmac_digest=hmac_digest)
    except Exception as e:
        logger.error("MonitoringRouter construction failed.", exc_info=True)
        comm_q.put(f"Monitoring router construction failed: {e}")
    else:
        comm_q.put(router.udp_port)

        logger.info("Starting MonitoringRouter in router_starter")
        try:
            router.start()
        except Exception:
            logger.exception("UDP router start exception")


class UDPRadioReceiver(MonitoringRadioReceiver):
    def __init__(self, *, process: SpawnProcessType, exit_event: EventType, port: int) -> None:
        self.process = process
        self.exit_event = exit_event
        self.port = port

    def shutdown(self) -> None:
        self.exit_event.set()
        join_terminate_close_proc(self.process)


def start_udp_receiver(*,
                       monitoring_messages: Queue,
                       port: Optional[int],
                       logdir: str,
                       debug: bool,
                       atexit_timeout: int,
                       hmac_key: bytes,
                       hmac_digest: str) -> UDPRadioReceiver:

    udp_comm_q: Queue[Union[int, str]]
    udp_comm_q = SizedQueue(maxsize=10)

    router_exit_event = SpawnEvent()

    router_proc = SpawnProcess(target=udp_router_starter,
                               kwargs={"comm_q": udp_comm_q,
                                       "resource_msgs": monitoring_messages,
                                       "exit_event": router_exit_event,
                                       "udp_port": port,
                                       "run_dir": logdir,
                                       "logging_level": logging.DEBUG if debug else logging.INFO,
                                       "atexit_timeout": atexit_timeout,
                                       "hmac_key": hmac_key,
                                       "hmac_digest": hmac_digest,
                                       },
                               name="Monitoring-UDP-Router-Process",
                               daemon=True,
                               )
    router_proc.start()

    try:
        udp_comm_q_result = udp_comm_q.get(block=True, timeout=120)
        udp_comm_q.close()
        udp_comm_q.join_thread()
    except queue.Empty:
        logger.error("Monitoring UDP router has not reported port in 120s. Aborting")
        raise MonitoringRouterStartError()

    if isinstance(udp_comm_q_result, str):
        logger.error("MonitoringRouter sent an error message: %s", udp_comm_q_result)
        raise RuntimeError(f"MonitoringRouter failed to start: {udp_comm_q_result}")

    return UDPRadioReceiver(process=router_proc, exit_event=router_exit_event, port=udp_comm_q_result)
