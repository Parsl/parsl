from __future__ import annotations

import logging
import multiprocessing.queues as mpq
import os
import queue
import time
from multiprocessing.context import SpawnProcess as SpawnProcessType
from multiprocessing.queues import Queue as QueueType
from multiprocessing.synchronize import Event as EventType
from typing import Tuple

import typeguard
import zmq

from parsl.addresses import tcp_url
from parsl.log_utils import set_file_logger
from parsl.monitoring.errors import MonitoringRouterStartError
from parsl.monitoring.radios.multiprocessing import MultiprocessingQueueRadioSender
from parsl.monitoring.types import TaggedMonitoringMessage
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
                 address: str,
                 port_range: Tuple[int, int] = (55050, 56000),

                 run_dir: str = ".",
                 logging_level: int = logging.INFO,
                 resource_msgs: mpq.Queue,
                 exit_event: EventType,
                 ):
        """ Initializes a monitoring configuration class.

        Parameters
        ----------
        address : str
             The ip address at which the workers will be able to reach the Hub.
        port_range : tuple(int, int)
             The MonitoringHub picks ports at random from the range which will be used by Hub.
             Default: (55050, 56000)
        run_dir : str
             Parsl log directory paths. Logs and temp files go here. Default: '.'
        logging_level : int
             Logging level as defined in the logging module. Default: logging.INFO
        resource_msgs : multiprocessing.Queue
            A multiprocessing queue to receive messages to be routed onwards to the database process
        exit_event : Event
            An event that the main Parsl process will set to signal that the monitoring router should shut down.
        """
        os.makedirs(run_dir, exist_ok=True)
        self.logger = set_file_logger(f"{run_dir}/monitoring_zmq_router.log",
                                      name="zmq_monitoring_router",
                                      level=logging_level)
        self.logger.debug("Monitoring router starting")

        self.address = address

        self.loop_freq = 10.0  # milliseconds

        self._context = zmq.Context()
        self.zmq_receiver_channel = self._context.socket(zmq.DEALER)
        self.zmq_receiver_channel.setsockopt(zmq.LINGER, 0)
        self.zmq_receiver_channel.set_hwm(0)
        self.zmq_receiver_channel.RCVTIMEO = int(self.loop_freq)  # in milliseconds
        self.logger.debug("address: {}. port_range {}".format(address, port_range))
        self.zmq_receiver_port = self.zmq_receiver_channel.bind_to_random_port(tcp_url(address),
                                                                               min_port=port_range[0],
                                                                               max_port=port_range[1])

        self.target_radio = MultiprocessingQueueRadioSender(resource_msgs)
        self.exit_event = exit_event

    @wrap_with_logs(target="zmq_monitoring_router")
    def start(self) -> None:
        self.logger.info("Starting ZMQ listener")
        try:
            while not self.exit_event.is_set():
                try:
                    dfk_loop_start = time.time()
                    while time.time() - dfk_loop_start < 1.0:  # TODO make configurable
                        # note that nothing checks that msg really is of the annotated type
                        msg: TaggedMonitoringMessage
                        msg = self.zmq_receiver_channel.recv_pyobj()

                        assert isinstance(msg, tuple), "ZMQ Receiver expects only tuples, got {}".format(msg)
                        assert len(msg) >= 1, "ZMQ Receiver expects tuples of length at least 1, got {}".format(msg)
                        assert len(msg) == 2, "ZMQ Receiver expects message tuples of exactly length 2, got {}".format(msg)

                        self.target_radio.send(msg)
                except zmq.Again:
                    pass
                except Exception:
                    # This will catch malformed messages. What happens if the
                    # channel is broken in such a way that it always raises
                    # an exception? Looping on this would maybe be the wrong
                    # thing to do.
                    self.logger.warning("Failure processing a ZMQ message", exc_info=True)

            self.logger.info("ZMQ listener finishing normally")
        finally:
            self.logger.info("ZMQ listener finished")


@wrap_with_logs
@typeguard.typechecked
def zmq_router_starter(*,
                       comm_q: mpq.Queue,
                       resource_msgs: mpq.Queue,
                       exit_event: EventType,

                       address: str,
                       port_range: Tuple[int, int],

                       run_dir: str,
                       logging_level: int) -> None:
    setproctitle("parsl: monitoring zmq router")
    try:
        router = MonitoringRouter(address=address,
                                  port_range=port_range,
                                  run_dir=run_dir,
                                  logging_level=logging_level,
                                  resource_msgs=resource_msgs,
                                  exit_event=exit_event)
    except Exception as e:
        logger.error("MonitoringRouter construction failed.", exc_info=True)
        comm_q.put(f"Monitoring router construction failed: {e}")
    else:
        comm_q.put(router.zmq_receiver_port)
        router.start()


class ZMQRadioReceiver():
    def __init__(self, *, process: SpawnProcessType, exit_event: EventType, port: int) -> None:
        self.process = process
        self.exit_event = exit_event
        self.port = port

    def close(self) -> None:
        self.exit_event.set()
        join_terminate_close_proc(self.process)


def start_zmq_receiver(*,
                       monitoring_messages: QueueType,
                       loopback_address: str,
                       port_range: Tuple[int, int],
                       logdir: str,
                       worker_debug: bool) -> ZMQRadioReceiver:
    comm_q = SizedQueue(maxsize=10)

    router_exit_event = SpawnEvent()

    router_proc = SpawnProcess(target=zmq_router_starter,
                               kwargs={"comm_q": comm_q,
                                       "resource_msgs": monitoring_messages,
                                       "exit_event": router_exit_event,
                                       "address": loopback_address,
                                       "port_range": port_range,
                                       "run_dir": logdir,
                                       "logging_level": logging.DEBUG if worker_debug else logging.INFO,
                                       },
                               name="Monitoring-ZMQ-Router-Process",
                               daemon=True,
                               )
    router_proc.start()

    try:
        logger.debug("Waiting for router process to report port")
        comm_q_result = comm_q.get(block=True, timeout=120)
        comm_q.close()
        comm_q.join_thread()
    except queue.Empty:
        logger.error("Monitoring ZMQ Router has not reported port in 120s")
        raise MonitoringRouterStartError()

    if isinstance(comm_q_result, str):
        logger.error("MonitoringRouter sent an error message: %s", comm_q_result)
        raise RuntimeError(f"MonitoringRouter failed to start: {comm_q_result}")

    return ZMQRadioReceiver(process=router_proc, exit_event=router_exit_event, port=comm_q_result)
