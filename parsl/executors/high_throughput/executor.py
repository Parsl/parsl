from concurrent.futures import Future
import typeguard
import logging
import threading
import queue
import pickle
from multiprocessing import Process, Queue
from typing import Dict, List, Optional, Tuple, Union
import math

from ipyparallel.serialize import pack_apply_message
from ipyparallel.serialize import deserialize_object

from parsl.app.errors import RemoteExceptionWrapper
from parsl.executors.high_throughput import zmq_pipes
from parsl.executors.high_throughput import interchange
from parsl.executors.errors import (
    BadMessage, ScalingFailed,
    DeserializationError, SerializationError,
    UnsupportedFeatureError
)

from parsl.executors.status_handling import StatusHandlingExecutor
from parsl.providers.provider_base import ExecutionProvider
from parsl.data_provider.staging import Staging
from parsl.addresses import get_all_addresses

from parsl.utils import RepresentationMixin
from parsl.providers import LocalProvider

logger = logging.getLogger(__name__)

BUFFER_THRESHOLD = 1024 * 1024
ITEM_THRESHOLD = 1024


class HighThroughputExecutor(StatusHandlingExecutor, RepresentationMixin):
    """Executor designed for cluster-scale

    The HighThroughputExecutor system has the following components:
      1. The HighThroughputExecutor instance which is run as part of the Parsl script.
      2. The Interchange which is acts as a load-balancing proxy between workers and Parsl
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
                     |  |         |   Q_mngmnt  |               |    V         V
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

    provider : :class:`~parsl.providers.provider_base.ExecutionProvider`
       Provider to access computation resources. Can be one of :class:`~parsl.providers.aws.aws.EC2Provider`,
        :class:`~parsl.providers.cobalt.cobalt.Cobalt`,
        :class:`~parsl.providers.condor.condor.Condor`,
        :class:`~parsl.providers.googlecloud.googlecloud.GoogleCloud`,
        :class:`~parsl.providers.gridEngine.gridEngine.GridEngine`,
        :class:`~parsl.providers.jetstream.jetstream.Jetstream`,
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

    address : string
        An address to connect to the main Parsl process which is reachable from the network in which
        workers will be running. This can be either a hostname as returned by `hostname` or an
        IP address. Most login nodes on clusters have several network interfaces available, only
        some of which can be reached from the compute nodes.
        By default, the executor will attempt to enumerate and connect through all possible addresses.
        Setting an address here overrides the default behavior.
        default=None

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

    managed : Bool
        If this executor is managed by the DFK or externally handled.

    cores_per_worker : float
        cores to be assigned to each worker. Oversubscription is possible
        by setting cores_per_worker < 1.0. Default=1

    mem_per_worker : float
        GB of memory required per worker. If this option is specified, the node manager
        will check the available memory at startup and limit the number of workers such that
        the there's sufficient memory for each worker. Default: None

    max_workers : int
        Caps the number of workers launched by the manager. Default: infinity

    prefetch_capacity : int
        Number of tasks that could be prefetched over available worker capacity.
        When there are a few tasks (<100) or when tasks are long running, this option should
        be set to 0 for better load balancing. Default is 0.

    address_probe_timeout : int | None
        Managers attempt connecting over many different addesses to determine a viable address.
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

    worker_logdir_root : string
        In case of a remote file system, specify the path to where logs will be kept.
    """

    @typeguard.typechecked
    def __init__(self,
                 label: str = 'HighThroughputExecutor',
                 provider: ExecutionProvider = LocalProvider(),
                 launch_cmd: Optional[str] = None,
                 address: Optional[str] = None,
                 worker_ports: Optional[Tuple[int, int]] = None,
                 worker_port_range: Optional[Tuple[int, int]] = (54000, 55000),
                 interchange_port_range: Optional[Tuple[int, int]] = (55000, 56000),
                 storage_access: Optional[List[Staging]] = None,
                 working_dir: Optional[str] = None,
                 worker_debug: bool = False,
                 cores_per_worker: float = 1.0,
                 mem_per_worker: Optional[float] = None,
                 max_workers: Union[int, float] = float('inf'),
                 prefetch_capacity: int = 0,
                 heartbeat_threshold: int = 120,
                 heartbeat_period: int = 30,
                 poll_period: int = 10,
                 address_probe_timeout: Optional[int] = None,
                 managed: bool = True,
                 worker_logdir_root: Optional[str] = None):

        logger.debug("Initializing HighThroughputExecutor")

        StatusHandlingExecutor.__init__(self, provider)
        self.label = label
        self.launch_cmd = launch_cmd
        self.worker_debug = worker_debug
        self.storage_access = storage_access
        self.working_dir = working_dir
        self.managed = managed
        self.blocks = {}  # type: Dict[str, str]
        self.cores_per_worker = cores_per_worker
        self.mem_per_worker = mem_per_worker
        self.max_workers = max_workers
        self.prefetch_capacity = prefetch_capacity
        self.address = address
        self.address_probe_timeout = address_probe_timeout
        if self.address:
            self.all_addresses = address
        else:
            self.all_addresses = ','.join(get_all_addresses())

        mem_slots = max_workers
        cpu_slots = max_workers
        if hasattr(self.provider, 'mem_per_node') and \
                self.provider.mem_per_node is not None and \
                mem_per_worker is not None and \
                mem_per_worker > 0:
            mem_slots = math.floor(self.provider.mem_per_node / mem_per_worker)
        if hasattr(self.provider, 'cores_per_node') and \
                self.provider.cores_per_node is not None:
            cpu_slots = math.floor(self.provider.cores_per_node / cores_per_worker)

        self.workers_per_node = min(max_workers, mem_slots, cpu_slots)
        if self.workers_per_node == float('inf'):
            self.workers_per_node = 1  # our best guess-- we do not have any provider hints

        self._task_counter = 0
        self.hub_address = None  # set to the correct hub address in dfk
        self.hub_port = None  # set to the correct hub port in dfk
        self.worker_ports = worker_ports
        self.worker_port_range = worker_port_range
        self.interchange_port_range = interchange_port_range
        self.heartbeat_threshold = heartbeat_threshold
        self.heartbeat_period = heartbeat_period
        self.poll_period = poll_period
        self.run_dir = '.'
        self.worker_logdir_root = worker_logdir_root

        if not launch_cmd:
            self.launch_cmd = ("process_worker_pool.py {debug} {max_workers} "
                               "-a {addresses} "
                               "-p {prefetch_capacity} "
                               "-c {cores_per_worker} "
                               "-m {mem_per_worker} "
                               "--poll {poll_period} "
                               "--task_port={task_port} "
                               "--result_port={result_port} "
                               "--logdir={logdir} "
                               "--block_id={{block_id}} "
                               "--hb_period={heartbeat_period} "
                               "{address_probe_timeout_string} "
                               "--hb_threshold={heartbeat_threshold} ")

    def initialize_scaling(self):
        """ Compose the launch command and call the scale_out

        This should be implemented in the child classes to take care of
        executor specific oddities.
        """
        debug_opts = "--debug" if self.worker_debug else ""
        max_workers = "" if self.max_workers == float('inf') else "--max_workers={}".format(self.max_workers)

        address_probe_timeout_string = ""
        if self.address_probe_timeout:
            address_probe_timeout_string = "--address_probe_timeout={}".format(self.address_probe_timeout)
        worker_logdir = "{}/{}".format(self.run_dir, self.label)
        if self.worker_logdir_root is not None:
            worker_logdir = "{}/{}".format(self.worker_logdir_root, self.label)

        l_cmd = self.launch_cmd.format(debug=debug_opts,
                                       prefetch_capacity=self.prefetch_capacity,
                                       address_probe_timeout_string=address_probe_timeout_string,
                                       addresses=self.all_addresses,
                                       task_port=self.worker_task_port,
                                       result_port=self.worker_result_port,
                                       cores_per_worker=self.cores_per_worker,
                                       mem_per_worker=self.mem_per_worker,
                                       max_workers=max_workers,
                                       nodes_per_block=self.provider.nodes_per_block,
                                       heartbeat_period=self.heartbeat_period,
                                       heartbeat_threshold=self.heartbeat_threshold,
                                       poll_period=self.poll_period,
                                       logdir=worker_logdir)
        self.launch_cmd = l_cmd
        logger.debug("Launch command: {}".format(self.launch_cmd))

        self._scaling_enabled = True
        logger.debug("Starting HighThroughputExecutor with provider:\n%s", self.provider)
        if hasattr(self.provider, 'init_blocks'):
            try:
                self.scale_out(blocks=self.provider.init_blocks)
            except Exception as e:
                logger.error("Scaling out failed: {}".format(e))
                raise e

    def start(self):
        """Create the Interchange process and connect to it.
        """
        self.outgoing_q = zmq_pipes.TasksOutgoing("127.0.0.1", self.interchange_port_range)
        self.incoming_q = zmq_pipes.ResultsIncoming("127.0.0.1", self.interchange_port_range)
        self.command_client = zmq_pipes.CommandClient("127.0.0.1", self.interchange_port_range)

        self.is_alive = True

        self._queue_management_thread = None
        self._start_queue_management_thread()
        self._start_local_queue_process()

        logger.debug("Created management thread: {}".format(self._queue_management_thread))

        self.initialize_scaling()

    def _queue_management_worker(self):
        """Listen to the queue for task status messages and handle them.

        Depending on the message, tasks will be updated with results, exceptions,
        or updates. It expects the following messages:

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

        We do not support these yet, but they could be added easily.

        .. code:: python

            {
               "task_id" : <task_id>
               "cpu_stat" : <>
               "mem_stat" : <>
               "io_stat"  : <>
               "started"  : tstamp
            }

        The `None` message is a die request.
        """
        logger.debug("[MTHREAD] queue management worker starting")

        while not self.bad_state_is_set:
            try:
                msgs = self.incoming_q.get(timeout=1)

            except queue.Empty:
                logger.debug("[MTHREAD] queue empty")
                # Timed out.
                pass

            except IOError as e:
                logger.exception("[MTHREAD] Caught broken queue with exception code {}: {}".format(e.errno, e))
                return

            except Exception as e:
                logger.exception("[MTHREAD] Caught unknown exception: {}".format(e))
                return

            else:

                if msgs is None:
                    logger.debug("[MTHREAD] Got None, exiting")
                    return

                else:
                    for serialized_msg in msgs:
                        try:
                            msg = pickle.loads(serialized_msg)
                            tid = msg['task_id']
                        except pickle.UnpicklingError:
                            raise BadMessage("Message received could not be unpickled")

                        except Exception:
                            raise BadMessage("Message received does not contain 'task_id' field")

                        if tid == -1 and 'exception' in msg:
                            logger.warning("Executor shutting down due to exception from interchange")
                            exception, _ = deserialize_object(msg['exception'])
                            self.set_bad_state_and_fail_all(exception)
                            break

                        task_fut = self.tasks[tid]

                        if 'result' in msg:
                            result, _ = deserialize_object(msg['result'])
                            task_fut.set_result(result)

                        elif 'exception' in msg:
                            try:
                                s, _ = deserialize_object(msg['exception'])
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

            if not self.is_alive:
                break
        logger.info("[MTHREAD] queue management worker finished")

    # When the executor gets lost, the weakref callback will wake up
    # the queue management thread.
    def weakref_cb(self, q=None):
        """We do not use this yet."""
        q.put(None)

    def _start_local_queue_process(self):
        """ Starts the interchange process locally

        Starts the interchange process locally and uses an internal command queue to
        get the worker task and result ports that the interchange has bound to.
        """
        comm_q = Queue(maxsize=10)
        self.queue_proc = Process(target=interchange.starter,
                                  args=(comm_q,),
                                  kwargs={"client_ports": (self.outgoing_q.port,
                                                           self.incoming_q.port,
                                                           self.command_client.port),
                                          "worker_ports": self.worker_ports,
                                          "worker_port_range": self.worker_port_range,
                                          "hub_address": self.hub_address,
                                          "hub_port": self.hub_port,
                                          "logdir": "{}/{}".format(self.run_dir, self.label),
                                          "heartbeat_threshold": self.heartbeat_threshold,
                                          "poll_period": self.poll_period,
                                          "logging_level": logging.DEBUG if self.worker_debug else logging.INFO
                                  },
                                  daemon=True,
                                  name="HTEX-Interchange"
        )
        self.queue_proc.start()
        try:
            (self.worker_task_port, self.worker_result_port) = comm_q.get(block=True, timeout=120)
        except queue.Empty:
            logger.error("Interchange has not completed initialization in 120s. Aborting")
            raise Exception("Interchange failed to start")

    def _start_queue_management_thread(self):
        """Method to start the management thread as a daemon.

        Checks if a thread already exists, then starts it.
        Could be used later as a restart if the management thread dies.
        """
        if self._queue_management_thread is None:
            logger.debug("Starting queue management thread")
            self._queue_management_thread = threading.Thread(target=self._queue_management_worker, name="HTEX-Queue-Management-Thread")
            self._queue_management_thread.daemon = True
            self._queue_management_thread.start()
            logger.debug("Started queue management thread")

        else:
            logger.debug("Management thread already exists, returning")

    def hold_worker(self, worker_id):
        """Puts a worker on hold, preventing scheduling of additional tasks to it.

        This is called "hold" mostly because this only stops scheduling of tasks,
        and does not actually kill the worker.

        Parameters
        ----------

        worker_id : str
            Worker id to be put on hold
        """
        c = self.command_client.run("HOLD_WORKER;{}".format(worker_id))
        logger.debug("Sent hold request to worker: {}".format(worker_id))
        return c

    @property
    def outstanding(self):
        outstanding_c = self.command_client.run("OUTSTANDING_C")
        return outstanding_c

    @property
    def connected_workers(self):
        workers = self.command_client.run("WORKERS")
        return workers

    @property
    def connected_managers(self):
        workers = self.command_client.run("MANAGERS")
        return workers

    def _hold_block(self, block_id):
        """ Sends hold command to all managers which are in a specific block

        Parameters
        ----------
        block_id : str
             Block identifier of the block to be put on hold
        """

        managers = self.connected_managers

        for manager in managers:
            if manager['block_id'] == block_id:
                logger.debug("[HOLD_BLOCK]: Sending hold to manager: {}".format(manager['manager']))
                self.hold_worker(manager['manager'])

    def submit(self, func, resource_specification, *args, **kwargs):
        """Submits work to the the outgoing_q.

        The outgoing_q is an external process listens on this
        queue for new work. This method behaves like a
        submit call as described here `Python docs: <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_

        Args:
            - func (callable) : Callable function
            - *args (list) : List of arbitrary positional arguments.

        Kwargs:
            - **kwargs (dict) : A dictionary of arbitrary keyword args for func.

        Returns:
              Future
        """
        if resource_specification:
            logger.error("Ignoring the resource specification. "
                         "Parsl resource specification is not supported in HighThroughput Executor. "
                         "Please check WorkQueueExecutor if resource specification is needed.")
            raise UnsupportedFeatureError('resource specification', 'HighThroughput Executor', 'WorkQueue Executor')

        if self.bad_state_is_set:
            raise self.executor_exception

        self._task_counter += 1
        task_id = self._task_counter

        # handle people sending blobs gracefully
        args_to_print = args
        if logger.getEffectiveLevel() >= logging.DEBUG:
            args_to_print = tuple([arg if len(repr(arg)) < 100 else (repr(arg)[:100] + '...') for arg in args])
        logger.debug("Pushing function {} to queue with args {}".format(func, args_to_print))

        self.tasks[task_id] = Future()

        try:
            fn_buf = pack_apply_message(func, args, kwargs,
                                        buffer_threshold=1024 * 1024,
                                        item_threshold=1024)
        except TypeError:
            raise SerializationError(func.__name__)

        msg = {"task_id": task_id,
               "buffer": fn_buf}

        # Post task to the the outgoing queue
        self.outgoing_q.put(msg)

        # Return the future
        return self.tasks[task_id]

    @property
    def scaling_enabled(self):
        return self._scaling_enabled

    def scale_out(self, blocks=1):
        """Scales out the number of blocks by "blocks"

        Raises:
             NotImplementedError
        """
        r = []
        for i in range(blocks):
            external_block_id = str(len(self.blocks))
            launch_cmd = self.launch_cmd.format(block_id=external_block_id)
            internal_block = self.provider.submit(launch_cmd, 1)
            logger.debug("Launched block {}->{}".format(external_block_id, internal_block))
            if not internal_block:
                raise(ScalingFailed(self.provider.label,
                                    "Attempts to provision nodes via provider has failed"))
            r.extend([external_block_id])
            self.blocks[external_block_id] = internal_block
        return r

    def scale_in(self, blocks=None, block_ids=[]):
        """Scale in the number of active blocks by specified amount.

        The scale in method here is very rude. It doesn't give the workers
        the opportunity to finish current tasks or cleanup. This is tracked
        in issue #530

        Parameters
        ----------

        blocks : int
             Number of blocks to terminate and scale_in by

        block_ids : list
             List of specific block ids to terminate. Optional

        Raises:
             NotImplementedError
        """

        if block_ids:
            block_ids_to_kill = block_ids
        else:
            block_ids_to_kill = list(self.blocks.keys())[:blocks]

        # Hold the block
        for block_id in block_ids_to_kill:
            self._hold_block(block_id)

        # Now kill via provider
        to_kill = [self.blocks.pop(bid) for bid in block_ids_to_kill]

        r = self.provider.cancel(to_kill)

        return self._filter_scale_in_ids(to_kill, r)

    def _get_job_ids(self) -> List[object]:
        return list(self.blocks.values())

    def shutdown(self, hub=True, targets='all', block=False):
        """Shutdown the executor, including all workers and controllers.

        This is not implemented.

        Kwargs:
            - hub (Bool): Whether the hub should be shutdown, Default: True,
            - targets (list of ints| 'all'): List of block id's to kill, Default: 'all'
            - block (Bool): To block for confirmations or not

        Raises:
             NotImplementedError
        """

        logger.info("Attempting HighThroughputExecutor shutdown")
        self.queue_proc.terminate()
        logger.info("Finished HighThroughputExecutor shutdown attempt")
        return True
