import logging
import math
import pickle
import subprocess
import threading
import typing
import warnings
from collections import defaultdict
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

import typeguard

from parsl import curvezmq
from parsl.addresses import get_all_addresses
from parsl.app.errors import RemoteExceptionWrapper
from parsl.data_provider.staging import Staging
from parsl.executors.errors import (
    BadMessage,
    InvalidResourceSpecification,
    ScalingFailed,
)
from parsl.executors.high_throughput import zmq_pipes
from parsl.executors.high_throughput.errors import CommandClientTimeoutError
from parsl.executors.high_throughput.manager_selector import (
    ManagerSelector,
    RandomManagerSelector,
)
from parsl.executors.status_handling import BlockProviderExecutor
from parsl.jobs.states import TERMINAL_STATES, JobState, JobStatus
from parsl.process_loggers import wrap_with_logs
from parsl.providers import LocalProvider
from parsl.providers.base import ExecutionProvider
from parsl.serialize import deserialize, pack_res_spec_apply_message
from parsl.serialize.errors import DeserializationError, SerializationError
from parsl.usage_tracking.api import UsageInformation
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)

DEFAULT_LAUNCH_CMD = ("process_worker_pool.py {debug} {max_workers_per_node} "
                      "-a {addresses} "
                      "-p {prefetch_capacity} "
                      "-c {cores_per_worker} "
                      "-m {mem_per_worker} "
                      "--poll {poll_period} "
                      "--task_port={task_port} "
                      "--result_port={result_port} "
                      "--cert_dir {cert_dir} "
                      "--logdir={logdir} "
                      "--block_id={{block_id}} "
                      "--hb_period={heartbeat_period} "
                      "{address_probe_timeout_string} "
                      "--hb_threshold={heartbeat_threshold} "
                      "--drain_period={drain_period} "
                      "--cpu-affinity {cpu_affinity} "
                      "{enable_mpi_mode} "
                      "--mpi-launcher={mpi_launcher} "
                      "--available-accelerators {accelerators}")

DEFAULT_INTERCHANGE_LAUNCH_CMD = ["interchange.py"]

GENERAL_HTEX_PARAM_DOCS = """provider : :class:`~parsl.providers.base.ExecutionProvider`
       Provider to access computation resources. Can be one of :class:`~parsl.providers.aws.aws.EC2Provider`,
        :class:`~parsl.providers.condor.condor.Condor`,
        :class:`~parsl.providers.googlecloud.googlecloud.GoogleCloud`,
        :class:`~parsl.providers.gridEngine.gridEngine.GridEngine`,
        :class:`~parsl.providers.local.local.Local`,
        :class:`~parsl.providers.sge.sge.GridEngine`,
        :class:`~parsl.providers.slurm.slurm.Slurm`, or
        :class:`~parsl.providers.torque.torque.Torque`.

    label : str
        Label for this executor instance.

    launch_cmd : str
        Command line string to launch the process_worker_pool from the provider. The command line string
        will be formatted with appropriate values for the following values (debug, task_url, result_url,
        cores_per_worker, nodes_per_block, heartbeat_period ,heartbeat_threshold, logdir). For example:
        launch_cmd="process_worker_pool.py {debug} -c {cores_per_worker} --task_url={task_url} --result_url={result_url}"

    interchange_launch_cmd : Sequence[str]
        Custom sequence of command line tokens to launch the interchange process from the executor. If
        undefined, the executor will use the default "interchange.py" command.

    address : string
        An address to connect to the main Parsl process which is reachable from the network in which
        workers will be running. This field expects an IPv4 or IPv6 address.
        Most login nodes on clusters have several network interfaces available, only some of which
        can be reached from the compute nodes. This field can be used to limit the executor to listen
        only on a specific interface, and limiting connections to the internal network.
        By default, the executor will attempt to enumerate and connect through all possible addresses.
        Setting an address here overrides the default behavior.
        default=None

    loopback_address: string
        Specify address used for internal communication between executor and interchange.
        Supports IPv4 and IPv6 addresses
        default=127.0.0.1

    worker_ports : (int, int)
        Specify the ports to be used by workers to connect to Parsl. If this option is specified,
        worker_port_range will not be honored.

    worker_port_range : (int, int)
        Worker ports will be chosen between the two integers provided.

    interchange_port_range : (int, int)
        Port range used by Parsl to communicate with the Interchange.

    working_dir : str
        Working dir to be used by the executor.

    worker_debug : Bool
        Enables worker debug logging.

    prefetch_capacity : int
        Number of tasks that could be prefetched over available worker capacity.
        When there are a few tasks (<100) or when tasks are long running, this option should
        be set to 0 for better load balancing. Default is 0.

    address_probe_timeout : int | None
        Managers attempt connecting over many different addresses to determine a viable address.
        This option sets a time limit in seconds on the connection attempt.
        Default of None implies 30s timeout set on worker.

    heartbeat_threshold : int
        Seconds since the last message from the counterpart in the communication pair:
        (interchange, manager) after which the counterpart is assumed to be un-available. Default: 120s

    heartbeat_period : int
        Number of seconds after which a heartbeat message indicating liveness is sent to the
        counterpart (interchange, manager). Default: 30s

    poll_period : int
        Timeout period to be used by the executor components in milliseconds. Increasing poll_periods
        trades performance for cpu efficiency. Default: 10ms

    drain_period : int
        The number of seconds after start when workers will begin to drain
        and then exit. Set this to a time that is slightly less than the
        maximum walltime of batch jobs to avoid killing tasks while they
        execute. For example, you could set this to the walltime minus a grace
        period for the batch job to start the workers, minus the expected
        maximum length of an individual task.

    worker_logdir_root : string
        In case of a remote file system, specify the path to where logs will be kept.

    encrypted : bool
        Flag to enable/disable encryption (CurveZMQ). Default is False.

    manager_selector: ManagerSelector
        Determines what strategy the interchange uses to select managers during task distribution.
        See API reference under "Manager Selectors" regarding the various manager selectors.
        Default: 'RandomManagerSelector'
"""  # Documentation for params used by both HTEx and MPIEx


class HighThroughputExecutor(BlockProviderExecutor, RepresentationMixin, UsageInformation):
    __doc__ = f"""Executor designed for cluster-scale

    The HighThroughputExecutor system has the following components:
      1. The HighThroughputExecutor instance which is run as part of the Parsl script.
      2. The Interchange which acts as a load-balancing proxy between workers and Parsl
      3. The multiprocessing based worker pool which coordinates task execution over several
         cores on a node.
      4. ZeroMQ pipes connect the HighThroughputExecutor, Interchange and the process_worker_pool

    Here is a diagram

    .. code:: python


                        |  Data   |  Executor   |  Interchange  | External Process(es)
                        |  Flow   |             |               |
                   Task | Kernel  |             |               |
                 +----->|-------->|------------>|->outgoing_q---|-> process_worker_pool
                 |      |         |             | batching      |    |         |
           Parsl<---Fut-|         |             | load-balancing|  result   exception
                     ^  |         |             | watchdogs     |    |         |
                     |  |         |    Result   |               |    |         |
                     |  |         |    Queue    |               |    V         V
                     |  |         |    Thread<--|-incoming_q<---|--- +---------+
                     |  |         |      |      |               |
                     |  |         |      |      |               |
                     +----update_fut-----+


    Each of the workers in each process_worker_pool has access to its local rank through
    an environmental variable, ``PARSL_WORKER_RANK``. The local rank is unique for each process
    and is an integer in the range from 0 to the number of workers per in the pool minus 1.
    The workers also have access to the ID of the worker pool as ``PARSL_WORKER_POOL_ID``
    and the size of the worker pool as ``PARSL_WORKER_COUNT``.


    Parameters
    ----------

    {GENERAL_HTEX_PARAM_DOCS}

    cores_per_worker : float
        cores to be assigned to each worker. Oversubscription is possible
        by setting cores_per_worker < 1.0. Default=1

    mem_per_worker : float
        GB of memory required per worker. If this option is specified, the node manager
        will check the available memory at startup and limit the number of workers such that
        the there's sufficient memory for each worker. Default: None

    max_workers_per_node : int
        Caps the number of workers launched per node. Default: None

    cpu_affinity: string
        Whether or how each worker process sets thread affinity. Options include "none" to forgo
        any CPU affinity configuration, "block" to assign adjacent cores to workers
        (ex: assign 0-1 to worker 0, 2-3 to worker 1), and
        "alternating" to assign cores to workers in round-robin
        (ex: assign 0,2 to worker 0, 1,3 to worker 1).
        The "block-reverse" option assigns adjacent cores to workers, but assigns
        the CPUs with large indices to low index workers (ex: assign 2-3 to worker 1, 0,1 to worker 2)

    available_accelerators: int | list
        Accelerators available for workers to use. Each worker will be pinned to exactly one of the provided
        accelerators, and no more workers will be launched than the number of accelerators.

        Either provide the list of accelerator names or the number available. If a number is provided,
        Parsl will create names as integers starting with 0.

        default: empty list

    """

    @typeguard.typechecked
    def __init__(self,
                 label: str = 'HighThroughputExecutor',
                 provider: ExecutionProvider = LocalProvider(),
                 launch_cmd: Optional[str] = None,
                 interchange_launch_cmd: Optional[Sequence[str]] = None,
                 address: Optional[str] = None,
                 loopback_address: str = "127.0.0.1",
                 worker_ports: Optional[Tuple[int, int]] = None,
                 worker_port_range: Optional[Tuple[int, int]] = (54000, 55000),
                 interchange_port_range: Optional[Tuple[int, int]] = (55000, 56000),
                 storage_access: Optional[List[Staging]] = None,
                 working_dir: Optional[str] = None,
                 worker_debug: bool = False,
                 cores_per_worker: float = 1.0,
                 mem_per_worker: Optional[float] = None,
                 max_workers_per_node: Optional[Union[int, float]] = None,
                 cpu_affinity: str = 'none',
                 available_accelerators: Union[int, Sequence[str]] = (),
                 prefetch_capacity: int = 0,
                 heartbeat_threshold: int = 120,
                 heartbeat_period: int = 30,
                 drain_period: Optional[int] = None,
                 poll_period: int = 10,
                 address_probe_timeout: Optional[int] = None,
                 worker_logdir_root: Optional[str] = None,
                 manager_selector: ManagerSelector = RandomManagerSelector(),
                 block_error_handler: Union[bool, Callable[[BlockProviderExecutor, Dict[str, JobStatus]], None]] = True,
                 encrypted: bool = False):

        logger.debug("Initializing HighThroughputExecutor")

        BlockProviderExecutor.__init__(self, provider=provider, block_error_handler=block_error_handler)
        self.label = label
        self.worker_debug = worker_debug
        self.storage_access = storage_access
        self.working_dir = working_dir
        self.cores_per_worker = cores_per_worker
        self.mem_per_worker = mem_per_worker
        self.prefetch_capacity = prefetch_capacity
        self.address = address
        self.address_probe_timeout = address_probe_timeout
        self.manager_selector = manager_selector
        self.loopback_address = loopback_address

        if self.address:
            self.all_addresses = address
        else:
            self.all_addresses = ','.join(get_all_addresses())

        self.max_workers_per_node = max_workers_per_node or float("inf")

        mem_slots = self.max_workers_per_node
        cpu_slots = self.max_workers_per_node
        if hasattr(self.provider, 'mem_per_node') and \
                self.provider.mem_per_node is not None and \
                mem_per_worker is not None and \
                mem_per_worker > 0:
            mem_slots = math.floor(self.provider.mem_per_node / mem_per_worker)
        if hasattr(self.provider, 'cores_per_node') and \
                self.provider.cores_per_node is not None:
            cpu_slots = math.floor(self.provider.cores_per_node / cores_per_worker)

        # Set the list of available accelerators
        if isinstance(available_accelerators, int):
            # If the user provide an integer, create some names for them
            available_accelerators = list(map(str, range(available_accelerators)))
        self.available_accelerators = list(available_accelerators)

        # Determine the number of workers per node
        self._workers_per_node = min(self.max_workers_per_node, mem_slots, cpu_slots)
        if len(self.available_accelerators) > 0:
            self._workers_per_node = min(self._workers_per_node, len(available_accelerators))
        if self._workers_per_node == float('inf'):
            self._workers_per_node = 1  # our best guess-- we do not have any provider hints

        self._task_counter = 0
        self.worker_ports = worker_ports
        self.worker_port_range = worker_port_range
        self.interchange_proc: Optional[subprocess.Popen] = None
        self.interchange_port_range = interchange_port_range
        self.heartbeat_threshold = heartbeat_threshold
        self.heartbeat_period = heartbeat_period
        self.drain_period = drain_period
        self.poll_period = poll_period
        self.run_dir = '.'
        self.worker_logdir_root = worker_logdir_root
        self.cpu_affinity = cpu_affinity
        self.encrypted = encrypted
        self.cert_dir = None

        if not launch_cmd:
            launch_cmd = DEFAULT_LAUNCH_CMD
        self.launch_cmd = launch_cmd

        if not interchange_launch_cmd:
            interchange_launch_cmd = DEFAULT_INTERCHANGE_LAUNCH_CMD
        self.interchange_launch_cmd = interchange_launch_cmd

        self._result_queue_thread_exit = threading.Event()
        self._result_queue_thread: Optional[threading.Thread] = None

    radio_mode = "htex"
    enable_mpi_mode: bool = False
    mpi_launcher: str = "mpiexec"

    def _warn_deprecated(self, old: str, new: str):
        warnings.warn(
            f"{old} is deprecated and will be removed in a future release. "
            f"Please use {new} instead.",
            DeprecationWarning,
            stacklevel=2
        )

    @property
    def logdir(self):
        return "{}/{}".format(self.run_dir, self.label)

    @property
    def worker_logdir(self):
        if self.worker_logdir_root is not None:
            return "{}/{}".format(self.worker_logdir_root, self.label)
        return self.logdir

    def validate_resource_spec(self, resource_specification: dict):
        if resource_specification:
            acceptable_fields: Set[str] = set()  # add new resource spec field names here to make htex accept them
            keys = set(resource_specification.keys())
            invalid_keys = keys - acceptable_fields
            if invalid_keys:
                message = "Task resource specification only accepts these types of resources: {}".format(
                    ', '.join(acceptable_fields))
                logger.error(message)
                raise InvalidResourceSpecification(set(invalid_keys), message)
        return

    def initialize_scaling(self):
        """Compose the launch command and scale out the initial blocks.
        """
        debug_opts = "--debug" if self.worker_debug else ""
        max_workers_per_node = "" if self.max_workers_per_node == float('inf') else "--max_workers_per_node={}".format(self.max_workers_per_node)
        enable_mpi_opts = "--enable_mpi_mode " if self.enable_mpi_mode else ""

        address_probe_timeout_string = ""
        if self.address_probe_timeout:
            address_probe_timeout_string = "--address_probe_timeout={}".format(self.address_probe_timeout)

        l_cmd = self.launch_cmd.format(debug=debug_opts,
                                       prefetch_capacity=self.prefetch_capacity,
                                       address_probe_timeout_string=address_probe_timeout_string,
                                       addresses=self.all_addresses,
                                       task_port=self.worker_task_port,
                                       result_port=self.worker_result_port,
                                       cores_per_worker=self.cores_per_worker,
                                       mem_per_worker=self.mem_per_worker,
                                       max_workers_per_node=max_workers_per_node,
                                       nodes_per_block=self.provider.nodes_per_block,
                                       heartbeat_period=self.heartbeat_period,
                                       heartbeat_threshold=self.heartbeat_threshold,
                                       drain_period=self.drain_period,
                                       poll_period=self.poll_period,
                                       cert_dir=self.cert_dir,
                                       logdir=self.worker_logdir,
                                       cpu_affinity=self.cpu_affinity,
                                       enable_mpi_mode=enable_mpi_opts,
                                       mpi_launcher=self.mpi_launcher,
                                       accelerators=" ".join(self.available_accelerators))
        self.launch_cmd = l_cmd
        logger.debug("Launch command: {}".format(self.launch_cmd))

        logger.debug("Starting HighThroughputExecutor with provider:\n%s", self.provider)

    def start(self):
        """Create the Interchange process and connect to it.
        """
        if self.encrypted and self.cert_dir is None:
            logger.debug("Creating CurveZMQ certificates")
            self.cert_dir = curvezmq.create_certificates(self.logdir)
        elif not self.encrypted and self.cert_dir:
            raise AttributeError(
                "The certificates directory path attribute (cert_dir) is defined, but the "
                "encrypted attribute is set to False. You must either change cert_dir to "
                "None or encrypted to True."
            )

        self.outgoing_q = zmq_pipes.TasksOutgoing(
            self.loopback_address, self.interchange_port_range, self.cert_dir
        )
        self.incoming_q = zmq_pipes.ResultsIncoming(
            self.loopback_address, self.interchange_port_range, self.cert_dir
        )
        self.command_client = zmq_pipes.CommandClient(
            self.loopback_address, self.interchange_port_range, self.cert_dir
        )

        self._result_queue_thread = None
        self._start_result_queue_thread()
        self._start_local_interchange_process()

        self.initialize_scaling()

    @wrap_with_logs
    def _result_queue_worker(self):
        """Listen to the queue for task result messages and handle them.

        Depending on the message, tasks will be updated with results or exceptions.

        .. code:: python

            {
               "task_id" : <task_id>
               "result"  : serialized result object, if task succeeded
               ... more tags could be added later
            }

            {
               "task_id" : <task_id>
               "exception" : serialized exception object, on failure
            }
        """
        logger.debug("Result queue worker starting")

        while not self.bad_state_is_set and not self._result_queue_thread_exit.is_set():
            try:
                msgs = self.incoming_q.get(timeout_ms=self.poll_period)
                if msgs is None:  # timeout
                    continue

            except IOError as e:
                logger.exception("Caught broken queue with exception code {}: {}".format(e.errno, e))
                return

            except Exception as e:
                logger.exception("Caught unknown exception: {}".format(e))
                return

            else:

                for serialized_msg in msgs:
                    try:
                        msg = pickle.loads(serialized_msg)
                    except pickle.UnpicklingError:
                        raise BadMessage("Message received could not be unpickled")

                    if msg['type'] == 'result':
                        try:
                            tid = msg['task_id']
                        except Exception:
                            raise BadMessage("Message received does not contain 'task_id' field")

                        if tid == -1 and 'exception' in msg:
                            logger.warning("Executor shutting down due to exception from interchange")
                            exception = deserialize(msg['exception'])
                            self.set_bad_state_and_fail_all(exception)
                            break

                        task_fut = self.tasks.pop(tid)

                        if 'result' in msg:
                            result = deserialize(msg['result'])
                            task_fut.set_result(result)

                        elif 'exception' in msg:
                            try:
                                s = deserialize(msg['exception'])
                                # s should be a RemoteExceptionWrapper... so we can reraise it
                                if isinstance(s, RemoteExceptionWrapper):
                                    try:
                                        s.reraise()
                                    except Exception as e:
                                        task_fut.set_exception(e)
                                elif isinstance(s, Exception):
                                    task_fut.set_exception(s)
                                else:
                                    raise ValueError("Unknown exception-like type received: {}".format(type(s)))
                            except Exception as e:
                                # TODO could be a proper wrapped exception?
                                task_fut.set_exception(
                                    DeserializationError("Received exception, but handling also threw an exception: {}".format(e)))
                        else:
                            raise BadMessage("Message received is neither result or exception")
                    else:
                        raise BadMessage("Message received with unknown type {}".format(msg['type']))

        logger.info("Closing result ZMQ pipe")
        self.incoming_q.close()
        logger.info("Result queue worker finished")

    def _start_local_interchange_process(self) -> None:
        """ Starts the interchange process locally

        Starts the interchange process locally and uses the command queue to
        get the worker task and result ports that the interchange has bound to.
        """

        assert self.interchange_proc is None, f"Already exists! {self.interchange_proc!r}"

        interchange_config = {"client_address": self.loopback_address,
                              "client_ports": (self.outgoing_q.port,
                                               self.incoming_q.port,
                                               self.command_client.port),
                              "interchange_address": self.address,
                              "worker_ports": self.worker_ports,
                              "worker_port_range": self.worker_port_range,
                              "hub_address": self.loopback_address,
                              "hub_zmq_port": self.hub_zmq_port,
                              "logdir": self.logdir,
                              "heartbeat_threshold": self.heartbeat_threshold,
                              "poll_period": self.poll_period,
                              "logging_level": logging.DEBUG if self.worker_debug else logging.INFO,
                              "cert_dir": self.cert_dir,
                              "manager_selector": self.manager_selector,
                              "run_id": self.run_id,
                              }

        config_pickle = pickle.dumps(interchange_config)

        self.interchange_proc = subprocess.Popen(self.interchange_launch_cmd, stdin=subprocess.PIPE)
        stdin = self.interchange_proc.stdin
        assert stdin is not None, "Popen should have created an IO object (vs default None) because of PIPE mode"

        logger.debug("Popened interchange process. Writing config object")
        stdin.write(config_pickle)
        stdin.flush()
        stdin.close()
        logger.debug("Sent config object. Requesting worker ports")
        try:
            (self.worker_task_port, self.worker_result_port) = self.command_client.run("WORKER_PORTS", timeout_s=120)
        except CommandClientTimeoutError:
            logger.error("Interchange has not completed initialization. Aborting")
            raise Exception("Interchange failed to start")
        logger.debug(
            "Interchange process started (%r).  Worker ports: %d, %d",
            self.interchange_proc,
            self.worker_task_port,
            self.worker_result_port
        )

    def _start_result_queue_thread(self):
        """Method to start the result queue thread as a daemon.

        Checks if a thread already exists, then starts it.
        Could be used later as a restart if the result queue thread dies.
        """
        assert self._result_queue_thread is None, f"Already exists! {self._result_queue_thread!r}"

        logger.debug("Starting result queue thread")
        self._result_queue_thread = threading.Thread(target=self._result_queue_worker, name="HTEX-Result-Queue-Thread")
        self._result_queue_thread.daemon = True
        self._result_queue_thread.start()
        logger.debug("Started result queue thread: %r", self._result_queue_thread)

    def hold_worker(self, worker_id: str) -> None:
        """Puts a worker on hold, preventing scheduling of additional tasks to it.

        This is called "hold" mostly because this only stops scheduling of tasks,
        and does not actually kill the worker.

        Parameters
        ----------

        worker_id : str
            Worker id to be put on hold
        """
        self.command_client.run("HOLD_WORKER;{}".format(worker_id))
        logger.debug("Sent hold request to manager: {}".format(worker_id))

    @property
    def outstanding(self) -> int:
        """Returns the count of tasks outstanding across the interchange
        and managers"""
        return len(self.tasks)

    @property
    def connected_workers(self) -> int:
        """Returns the count of workers across all connected managers"""
        return self.command_client.run("WORKERS")

    def connected_managers(self) -> List[Dict[str, typing.Any]]:
        """Returns a list of dicts one for each connected managers.
        The dict contains info on manager(str:manager_id), block_id,
        worker_count, tasks(int), idle_durations(float), active(bool)
        """
        return self.command_client.run("MANAGERS")

    def connected_managers_packages(self) -> Dict[str, Dict[str, str]]:
        """Returns a dict mapping each manager ID to a dict of installed
        packages and their versions
        """
        return self.command_client.run("MANAGERS_PACKAGES")

    def connected_blocks(self) -> List[str]:
        """List of connected block ids"""
        return self.command_client.run("CONNECTED_BLOCKS")

    def _hold_block(self, block_id):
        """ Sends hold command to all managers which are in a specific block

        Parameters
        ----------
        block_id : str
             Block identifier of the block to be put on hold
        """

        managers = self.connected_managers()

        for manager in managers:
            if manager['block_id'] == block_id:
                logger.debug("Sending hold to manager: {}".format(manager['manager']))
                self.hold_worker(manager['manager'])

    def submit(self, func, resource_specification, *args, **kwargs):
        """Submits work to the outgoing_q.

        The outgoing_q is an external process listens on this
        queue for new work. This method behaves like a submit call as described here `Python docs: <https://docs.python.org/3/
        library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_

        Args:
            - func (callable) : Callable function
            - resource_specification (dict): Dictionary containing relevant info about task that is needed by underlying executors.
            - args (list) : List of arbitrary positional arguments.

        Kwargs:
            - kwargs (dict) : A dictionary of arbitrary keyword args for func.

        Returns:
              Future
        """

        self.validate_resource_spec(resource_specification)

        if self.bad_state_is_set:
            raise self.executor_exception

        self._task_counter += 1
        task_id = self._task_counter

        # handle people sending blobs gracefully
        if logger.getEffectiveLevel() <= logging.DEBUG:
            args_to_print = tuple([ar if len(ar := repr(arg)) < 100 else (ar[:100] + '...') for arg in args])
            logger.debug("Pushing function {} to queue with args {}".format(func, args_to_print))

        fut = Future()
        fut.parsl_executor_task_id = task_id
        self.tasks[task_id] = fut

        try:
            fn_buf = pack_res_spec_apply_message(func, args, kwargs,
                                                 resource_specification=resource_specification,
                                                 buffer_threshold=1024 * 1024)
        except TypeError:
            raise SerializationError(func.__name__)

        msg = {"task_id": task_id, "resource_spec": resource_specification, "buffer": fn_buf}

        # Post task to the outgoing queue
        self.outgoing_q.put(msg)

        # Return the future
        return fut

    @property
    def workers_per_node(self) -> Union[int, float]:
        return self._workers_per_node

    def scale_in(self, blocks: int, max_idletime: Optional[float] = None) -> List[str]:
        """Scale in the number of active blocks by specified amount.

        The scale in method here is very rude. It doesn't give the workers
        the opportunity to finish current tasks or cleanup. This is tracked
        in issue #530

        Parameters
        ----------

        blocks : int
             Number of blocks to terminate and scale_in by

        max_idletime: float
             A time to indicate how long a block should be idle to be a
             candidate for scaling in.

             If None then blocks will be force scaled in even if they are busy.

             If a float, then only idle blocks will be terminated, which may be less than
             the requested number.

        Returns
        -------
        List of block IDs scaled in
        """
        logger.debug(f"Scale in called, blocks={blocks}")

        @dataclass
        class BlockInfo:
            tasks: int  # sum of tasks in this block
            idle: float  # shortest idle time of any manager in this block

        # block_info will be populated from two sources:
        # the Job Status Poller mutable block list, and the list of blocks
        # which have connected to the interchange.

        def new_block_info():
            return BlockInfo(tasks=0, idle=float('inf'))

        block_info: Dict[str, BlockInfo] = defaultdict(new_block_info)

        for block_id, job_status in self._status.items():
            if job_status.state not in TERMINAL_STATES:
                block_info[block_id] = new_block_info()

        managers = self.connected_managers()
        for manager in managers:
            if not manager['active']:
                continue
            b_id = manager['block_id']
            block_info[b_id].tasks += manager['tasks']
            block_info[b_id].idle = min(block_info[b_id].idle, manager['idle_duration'])

        # The scaling policy is that longest idle blocks should be scaled down
        # in preference to least idle (most recently used) blocks.
        # Other policies could be implemented here.

        sorted_blocks = sorted(block_info.items(), key=lambda item: (-item[1].idle, item[1].tasks))

        logger.debug(f"Scale in selecting from {len(sorted_blocks)} blocks")
        if max_idletime is None:
            block_ids_to_kill = [x[0] for x in sorted_blocks[:blocks]]
        else:
            block_ids_to_kill = []
            for x in sorted_blocks:
                if x[1].idle > max_idletime and x[1].tasks == 0:
                    block_ids_to_kill.append(x[0])
                    if len(block_ids_to_kill) == blocks:
                        break

            logger.debug("Selected idle block ids to kill: {}".format(
                block_ids_to_kill))
            if len(block_ids_to_kill) < blocks:
                logger.warning(f"Could not find enough blocks to kill: wanted {blocks} but only selected {len(block_ids_to_kill)}")

        # Hold the block
        for block_id in block_ids_to_kill:
            self._hold_block(block_id)

        # Now kill via provider
        # Potential issue with multiple threads trying to remove the same blocks
        to_kill = [self.blocks_to_job_id[bid] for bid in block_ids_to_kill if bid in self.blocks_to_job_id]

        r = self.provider.cancel(to_kill)
        job_ids = self._filter_scale_in_ids(to_kill, r)

        # to_kill block_ids are fetched from self.blocks_to_job_id
        # If a block_id is in self.blocks_to_job_id, it must exist in self.job_ids_to_block
        block_ids_killed = [self.job_ids_to_block[jid] for jid in job_ids]

        return block_ids_killed

    def _get_launch_command(self, block_id: str) -> str:
        if self.launch_cmd is None:
            raise ScalingFailed(self, "No launch command")
        launch_cmd = self.launch_cmd.format(block_id=block_id)
        return launch_cmd

    def status(self) -> Dict[str, JobStatus]:
        job_status = super().status()
        connected_blocks = self.connected_blocks()
        for job_id in job_status:
            job_info = job_status[job_id]
            if job_info.terminal and job_id not in connected_blocks and job_info.state != JobState.SCALED_IN:
                logger.debug("Rewriting job %s from status %s to MISSING", job_id, job_info)
                job_status[job_id].state = JobState.MISSING
                if job_status[job_id].message is None:
                    job_status[job_id].message = (
                        "Job is marked as MISSING since the workers failed to register "
                        "to the executor. Check the stdout/stderr logs in the submit_scripts "
                        "directory for more debug information"
                    )
        return job_status

    def shutdown(self, timeout: float = 10.0):
        """Shutdown the executor, including the interchange. This does not
        shut down any workers directly - workers should be terminated by the
        scaling mechanism or by heartbeat timeout.

        Parameters
        ----------

        timeout : float
            Amount of time to wait for the Interchange process to terminate before
            we forcefully kill it.
        """
        if self.interchange_proc is None:
            logger.info("HighThroughputExecutor has not started; skipping shutdown")
            return

        logger.info("Attempting HighThroughputExecutor shutdown")

        logger.info("Terminating interchange and result queue thread")
        self._result_queue_thread_exit.set()
        self.interchange_proc.terminate()
        try:
            self.interchange_proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            logger.warning("Unable to terminate Interchange process; sending SIGKILL")
            self.interchange_proc.kill()

        logger.info("Closing ZMQ pipes")

        # These pipes are used in a thread unsafe manner. If you have traced a
        # problem to this block of code, you might consider what is happening
        # with other threads that access these.

        # incoming_q is not closed here because it is used by the results queue
        # worker which is not shut down at this point.

        if hasattr(self, 'outgoing_q'):
            logger.info("Closing outgoing_q")
            self.outgoing_q.close()

        if hasattr(self, 'command_client'):
            logger.info("Closing command client")
            self.command_client.close()

        logger.info("Waiting for result queue thread exit")
        if self._result_queue_thread:
            self._result_queue_thread.join()

        logger.info("Finished HighThroughputExecutor shutdown attempt")

    def get_usage_information(self):
        return {"mpi": self.enable_mpi_mode}
