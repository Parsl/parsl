import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.queues import Queue

from academy.agent import Agent, action
from academy.exchange import HttpExchangeFactory
from academy.handle import Handle
from academy.manager import Manager

from parsl.monitoring.radios.base import (
    MonitoringRadioReceiver,
    MonitoringRadioSender,
    RadioConfig,
)
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class AcademyRadio(RadioConfig, RepresentationMixin):
    def __init__(self, *, atexit_timeout: int = 3, debug: bool = False):
        self.atexit_timeout = atexit_timeout
        self.debug = debug

    def create_sender(self) -> MonitoringRadioSender:
        assert self.handle is not None, "self.handle should have been initialized by create_receiver"
        return AcademyRadioSender(self.handle)

    def create_receiver(self, run_dir: str, resource_msgs: Queue) -> MonitoringRadioReceiver:
        # RFC 2104 section 2 recommends that the key length be at
        # least as long as the hash output (64 bytes in the case of SHA512).
        # RFC 2014 section 3 talks about periodic key refreshing. This key is
        # not refreshed inside a workflow run, but each separate workflow run
        # uses a new key.
        # keysize = hashlib.new(self.hmac_digest).digest_size
        # self.hmac_key = secrets.token_bytes(keysize)

        academy_receiver = start_academy_receiver(logdir=run_dir,
                                                  monitoring_messages=resource_msgs,
                                                  debug=self.debug,
                                                  atexit_timeout=self.atexit_timeout,
                                                  )
        self.handle = academy_receiver.handle
        return academy_receiver


class AcademyRadioSender(MonitoringRadioSender):

    def __init__(self, handle: Handle) -> None:
        self.handle = handle

    def send(self, message: object) -> None:
        """ Sends a message to the UDP receiver

        Parameter
        ---------

        message: object
            Arbitrary pickle-able object that is to be sent

        Returns:
            None
        """
        logger.info(f"Starting Academy radio message send to handle {self.handle}")

        # TODO: probably can do a whole manager startup here which is awfully inefficient
        # but lets me get around fiddling with concurreny model jamming together right now.
        asyncio.run(self.async_send(message))

        logger.info("Normal ending for Academy radio message send")
        return

    async def async_send(self, message: object) -> None:
        async with await Manager.from_exchange_factory(
                             factory=HttpExchangeFactory(auth_method='globus',
                                                         url="https://exchange.academy-agents.org"),
                             executors=ThreadPoolExecutor()):

            await self.handle.monmsg(message)


import logging
import multiprocessing.queues as mpq
import os
import queue
from multiprocessing.context import SpawnProcess as SpawnProcessType
from multiprocessing.queues import Queue
from multiprocessing.synchronize import Event
from multiprocessing.synchronize import Event as EventType
from typing import Union

import typeguard

from parsl.log_utils import set_file_logger
from parsl.monitoring.errors import MonitoringRouterStartError
from parsl.monitoring.radios.base import MonitoringRadioReceiver
from parsl.monitoring.radios.multiprocessing import MultiprocessingQueueRadioSender
from parsl.multiprocessing import (
    SpawnEvent,
    SpawnProcess,
    SpawnQueue,
    join_terminate_close_proc,
)
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

logger = logging.getLogger(__name__)


class MonitoringReceiverAgent(Agent):

    def __init__(self, resource_msgs: Queue):
        logger.info("monitoring router agent init")
        self.resource_msgs = resource_msgs

    async def agent_on_startup(self) -> None:
        logger.info("monitoring router agent_on_startup")

    @action
    async def monmsg(self, o: object) -> None:
        logger.info("got a monitoring message - putting into resource queue")
        self.resource_msgs.put(o)


class MonitoringRouter:

    def __init__(self,
                 *,
                 comm_q: mpq.Queue,
                 run_dir: str = ".",
                 logging_level: int = logging.INFO,
                 atexit_timeout: int,   # in seconds
                 resource_msgs: mpq.Queue,
                 exit_event: Event,
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
        set_file_logger(f"{run_dir}/monitoring_academy_router.log",
                        level=logging.DEBUG, name='')
        logger.debug("Monitoring router starting")

        self.atexit_timeout = atexit_timeout

        # instead of starting a UDP socket with a loop running in start()
        # i want to get an agent constructed, with an academy handle that
        # i can hand over to the sending side.

        # That probably means starting an event loop in a new thread here...
        # except we're actually already in our own process so we can "Just"
        # run an event loop right here.
        # and move the start() handling from there into here?

        self.target_radio = MultiprocessingQueueRadioSender(resource_msgs)
        self.exit_event = exit_event
        self.comm_q = comm_q
        self.resource_msgs = resource_msgs

        asyncio.run(self.receiver_async())

    async def receiver_async(self) -> None:

        logger.error("main event loop for the Academy radio receiver")

        # TODO: manager and start an agent in the event loop. it'll probably run in the thread pool, though?
        # which i guess is fine but there's no reason it shouldn't run directly alongside this main body?
        # i.e. the agent-alongside concept, but alongside this non-agent.

        async with await Manager.from_exchange_factory(factory=HttpExchangeFactory(auth_method='globus',
                                                                                   url="https://exchange.academy-agents.org"),
                                                       executors=ThreadPoolExecutor()) as m:
            print(f"got manager {m!r}")
            a = MonitoringReceiverAgent(self.resource_msgs)
            ah = await m.launch(a)
            self.comm_q.put(ah)
            logger.info(f"Launched monitoring receiver agent, with handle {ah}")

            # now we sit here till the exit event happens. which is threading oriented, not asyncio oriented.

            logger.info("waiting for exit event")
            await asyncio.to_thread(self.exit_event.wait)
            logger.info("exit event fired")
        logger.info("out of academy Manager with-block")


@wrap_with_logs
@typeguard.typechecked
def academy_router_starter(*,
                           comm_q: mpq.Queue,
                           resource_msgs: mpq.Queue,
                           exit_event: Event,
                           run_dir: str,
                           logging_level: int,
                           atexit_timeout: int) -> None:
    setproctitle("parsl: monitoring academy router")
    try:
        MonitoringRouter(run_dir=run_dir,
                         comm_q=comm_q,
                         logging_level=logging_level,
                         resource_msgs=resource_msgs,
                         exit_event=exit_event,
                         atexit_timeout=atexit_timeout)
    except Exception as e:
        logger.error("MonitoringRouter construction failed", exc_info=True)
        comm_q.put(f"Monitoring router construction failed: {e}")
    else:
        logger.info("MonitoringRouter construction ended.", exc_info=True)


class AcademyRadioReceiver(MonitoringRadioReceiver):
    def __init__(self, *, process: SpawnProcessType, exit_event: EventType, handle: Handle) -> None:
        self.process = process
        self.exit_event = exit_event
        self.handle = handle

    def shutdown(self) -> None:
        self.exit_event.set()
        join_terminate_close_proc(self.process)


def start_academy_receiver(*,
                           monitoring_messages: Queue,
                           logdir: str,
                           debug: bool,
                           atexit_timeout: int,
                           ) -> AcademyRadioReceiver:

    udp_comm_q: Queue[Union[Handle, str]]
    udp_comm_q = SpawnQueue(maxsize=10)

    router_exit_event = SpawnEvent()

    router_proc = SpawnProcess(target=academy_router_starter,
                               kwargs={"comm_q": udp_comm_q,
                                       "resource_msgs": monitoring_messages,
                                       "exit_event": router_exit_event,
                                       "run_dir": logdir,
                                       "logging_level": logging.DEBUG if debug else logging.INFO,
                                       "atexit_timeout": atexit_timeout,
                                       },
                               name="Monitoring-Academy-Router-Process",
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

    return AcademyRadioReceiver(process=router_proc, exit_event=router_exit_event, handle=udp_comm_q_result)
