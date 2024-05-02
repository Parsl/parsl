from __future__ import annotations

import os
import socket
import time
import pickle
import logging
import zmq

import queue

from parsl.log_utils import set_file_logger
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

from parsl.monitoring.message_type import MessageType
from parsl.monitoring.types import AddressedMonitoringMessage, TaggedMonitoringMessage

from multiprocessing.synchronize import Event
from typing import Optional, Tuple, Union


logger = logging.getLogger(__name__)


class MonitoringRouter:

    def __init__(self,
                 *,
                 hub_address: str,
                 udp_port: Optional[int] = None,
                 zmq_port_range: Tuple[int, int] = (55050, 56000),

                 monitoring_hub_address: str = "127.0.0.1",
                 logdir: str = ".",
                 run_id: str,
                 logging_level: int = logging.INFO,
                 atexit_timeout: int = 3    # in seconds
                 ):
        """ Initializes a monitoring configuration class.

        Parameters
        ----------
        hub_address : str
             The ip address at which the workers will be able to reach the Hub.
        udp_port : int
             The specific port at which workers will be able to reach the Hub via UDP. Default: None
        zmq_port_range : tuple(int, int)
             The MonitoringHub picks ports at random from the range which will be used by Hub.
             Default: (55050, 56000)
        logdir : str
             Parsl log directory paths. Logs and temp files go here. Default: '.'
        logging_level : int
             Logging level as defined in the logging module. Default: logging.INFO
        atexit_timeout : float, optional
            The amount of time in seconds to terminate the hub without receiving any messages, after the last dfk workflow message is received.

        """
        os.makedirs(logdir, exist_ok=True)
        self.logger = set_file_logger("{}/monitoring_router.log".format(logdir),
                                      name="monitoring_router",
                                      level=logging_level)
        self.logger.debug("Monitoring router starting")

        self.hub_address = hub_address
        self.atexit_timeout = atexit_timeout
        self.run_id = run_id

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

        self._context = zmq.Context()
        self.zmq_receiver_channel = self._context.socket(zmq.DEALER)
        self.zmq_receiver_channel.setsockopt(zmq.LINGER, 0)
        self.zmq_receiver_channel.set_hwm(0)
        self.zmq_receiver_channel.RCVTIMEO = int(self.loop_freq)  # in milliseconds
        self.logger.debug("hub_address: {}. zmq_port_range {}".format(hub_address, zmq_port_range))
        self.zmq_receiver_port = self.zmq_receiver_channel.bind_to_random_port("tcp://*",
                                                                               min_port=zmq_port_range[0],
                                                                               max_port=zmq_port_range[1])

    def start(self,
              priority_msgs: "queue.Queue[AddressedMonitoringMessage]",
              node_msgs: "queue.Queue[AddressedMonitoringMessage]",
              block_msgs: "queue.Queue[AddressedMonitoringMessage]",
              resource_msgs: "queue.Queue[AddressedMonitoringMessage]",
              exit_event: Event) -> None:
        try:
            while not exit_event.is_set():
                try:
                    data, addr = self.udp_sock.recvfrom(2048)
                    resource_msg = pickle.loads(data)
                    self.logger.debug("Got UDP Message from {}: {}".format(addr, resource_msg))
                    resource_msgs.put((resource_msg, addr))
                except socket.timeout:
                    pass

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

                        if msg[0] == MessageType.NODE_INFO:
                            msg[1]['run_id'] = self.run_id
                            node_msgs.put(msg_0)
                        elif msg[0] == MessageType.RESOURCE_INFO:
                            resource_msgs.put(msg_0)
                        elif msg[0] == MessageType.BLOCK_INFO:
                            block_msgs.put(msg_0)
                        elif msg[0] == MessageType.TASK_INFO:
                            priority_msgs.put(msg_0)
                        elif msg[0] == MessageType.WORKFLOW_INFO:
                            priority_msgs.put(msg_0)
                        else:
                            # There is a type: ignore here because if msg[0]
                            # is of the correct type, this code is unreachable,
                            # but there is no verification that the message
                            # received from zmq_receiver_channel.recv_pyobj() is actually
                            # of that type.
                            self.logger.error("Discarding message "  # type: ignore[unreachable]
                                              f"from interchange with unknown type {msg[0].value}")
                except zmq.Again:
                    pass
                except Exception:
                    # This will catch malformed messages. What happens if the
                    # channel is broken in such a way that it always raises
                    # an exception? Looping on this would maybe be the wrong
                    # thing to do.
                    self.logger.warning("Failure processing a ZMQ message", exc_info=True)

            self.logger.info("Monitoring router draining")
            last_msg_received_time = time.time()
            while time.time() - last_msg_received_time < self.atexit_timeout:
                try:
                    data, addr = self.udp_sock.recvfrom(2048)
                    msg = pickle.loads(data)
                    self.logger.debug("Got UDP Message from {}: {}".format(addr, msg))
                    resource_msgs.put((msg, addr))
                    last_msg_received_time = time.time()
                except socket.timeout:
                    pass

            self.logger.info("Monitoring router finishing normally")
        finally:
            self.logger.info("Monitoring router finished")


@wrap_with_logs
def router_starter(comm_q: "queue.Queue[Union[Tuple[int, int], str]]",
                   exception_q: "queue.Queue[Tuple[str, str]]",
                   priority_msgs: "queue.Queue[AddressedMonitoringMessage]",
                   node_msgs: "queue.Queue[AddressedMonitoringMessage]",
                   block_msgs: "queue.Queue[AddressedMonitoringMessage]",
                   resource_msgs: "queue.Queue[AddressedMonitoringMessage]",
                   exit_event: Event,

                   hub_address: str,
                   udp_port: Optional[int],
                   zmq_port_range: Tuple[int, int],

                   logdir: str,
                   logging_level: int,
                   run_id: str) -> None:
    setproctitle("parsl: monitoring router")
    try:
        router = MonitoringRouter(hub_address=hub_address,
                                  udp_port=udp_port,
                                  zmq_port_range=zmq_port_range,
                                  logdir=logdir,
                                  logging_level=logging_level,
                                  run_id=run_id)
    except Exception as e:
        logger.error("MonitoringRouter construction failed.", exc_info=True)
        comm_q.put(f"Monitoring router construction failed: {e}")
    else:
        comm_q.put((router.udp_port, router.zmq_receiver_port))

        router.logger.info("Starting MonitoringRouter in router_starter")
        try:
            router.start(priority_msgs, node_msgs, block_msgs, resource_msgs, exit_event)
        except Exception as e:
            router.logger.exception("router.start exception")
            exception_q.put(('Hub', str(e)))
