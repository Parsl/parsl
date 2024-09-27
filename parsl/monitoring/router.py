from __future__ import annotations

import logging
import multiprocessing.queues as mpq
import os
import threading
import time
from multiprocessing.synchronize import Event
from typing import Tuple

import typeguard
import zmq

from parsl.log_utils import set_file_logger
from parsl.monitoring.types import AddressedMonitoringMessage, TaggedMonitoringMessage
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

logger = logging.getLogger(__name__)


class MonitoringRouter:

    def __init__(self,
                 *,
                 hub_address: str,
                 zmq_port_range: Tuple[int, int] = (55050, 56000),

                 monitoring_hub_address: str = "127.0.0.1",
                 logdir: str = ".",
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
        zmq_port_range : tuple(int, int)
             The MonitoringHub picks ports at random from the range which will be used by Hub.
             Default: (55050, 56000)
        logdir : str
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
        os.makedirs(logdir, exist_ok=True)
        self.logger = set_file_logger("{}/monitoring_router.log".format(logdir),
                                      name="monitoring_router",
                                      level=logging_level)
        self.logger.debug("Monitoring router starting")

        self.hub_address = hub_address
        self.atexit_timeout = atexit_timeout

        self.loop_freq = 10.0  # milliseconds

        self._context = zmq.Context()
        self.zmq_receiver_channel = self._context.socket(zmq.DEALER)
        self.zmq_receiver_channel.setsockopt(zmq.LINGER, 0)
        self.zmq_receiver_channel.set_hwm(0)
        self.zmq_receiver_channel.RCVTIMEO = int(self.loop_freq)  # in milliseconds
        self.logger.debug("hub_address: {}. zmq_port_range {}".format(hub_address, zmq_port_range))
        self.zmq_receiver_port = self.zmq_receiver_channel.bind_to_random_port("tcp://*",
                                                                               min_port=zmq_port_range[0],
                                                                               max_port=zmq_port_range[1])

        self.resource_msgs = resource_msgs
        self.exit_event = exit_event

    @wrap_with_logs(target="monitoring_router")
    def start(self) -> None:
        self.logger.info("Starting ZMQ listener thread")
        zmq_radio_receiver_thread = threading.Thread(target=self.start_zmq_listener, daemon=True)
        zmq_radio_receiver_thread.start()

        self.logger.info("Joining on ZMQ listener thread")
        zmq_radio_receiver_thread.join()
        self.logger.info("Joined on ZMQ listener thread")

    @wrap_with_logs(target="monitoring_router")
    def start_zmq_listener(self) -> None:
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

                        msg_0: AddressedMonitoringMessage
                        msg_0 = (msg, 0)

                        self.resource_msgs.put(msg_0)
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
def router_starter(*,
                   comm_q: mpq.Queue,
                   exception_q: mpq.Queue,
                   resource_msgs: mpq.Queue,
                   exit_event: Event,

                   hub_address: str,
                   zmq_port_range: Tuple[int, int],

                   logdir: str,
                   logging_level: int) -> None:
    setproctitle("parsl: monitoring router")
    try:
        router = MonitoringRouter(hub_address=hub_address,
                                  zmq_port_range=zmq_port_range,
                                  logdir=logdir,
                                  logging_level=logging_level,
                                  resource_msgs=resource_msgs,
                                  exit_event=exit_event)
    except Exception as e:
        logger.error("MonitoringRouter construction failed.", exc_info=True)
        comm_q.put(f"Monitoring router construction failed: {e}")
    else:
        comm_q.put(router.zmq_receiver_port)

        router.logger.info("Starting MonitoringRouter in router_starter")
        try:
            router.start()
        except Exception as e:
            router.logger.exception("router.start exception")
            exception_q.put(('Hub', str(e)))
