#!/usr/bin/env python3

import argparse
import json
import logging
import math
import multiprocessing
import os
import pickle
import platform
import queue
import subprocess
import sys
import threading
import time
import uuid
from importlib.metadata import distributions
from multiprocessing.context import SpawnProcess
from multiprocessing.managers import DictProxy
from multiprocessing.sharedctypes import Synchronized
from typing import Dict, List, Optional, Sequence

import psutil
import zmq

from parsl import curvezmq
from parsl.addresses import tcp_url
from parsl.app.errors import RemoteExceptionWrapper
from parsl.executors.execute_task import execute_task
from parsl.executors.high_throughput.errors import WorkerLost
from parsl.executors.high_throughput.mpi_prefix_composer import (
    VALID_LAUNCHERS,
    compose_all,
)
from parsl.executors.high_throughput.mpi_resource_management import (
    MPITaskScheduler,
    TaskScheduler,
)
from parsl.executors.high_throughput.probe import probe_addresses
from parsl.multiprocessing import SpawnContext
from parsl.process_loggers import wrap_with_logs
from parsl.serialize import serialize
from parsl.version import VERSION as PARSL_VERSION

HEARTBEAT_CODE = (2 ** 32) - 1
DRAINED_CODE = (2 ** 32) - 2


class Manager:
    """ Manager manages task execution by the workers

                |         zmq              |    Manager         |   Worker Processes
                |                          |                    |
                | <-----Register with -----+                    |      Request task<--+
                |       N task capacity    |                    |          |          |
    Interchange | -------------------------+->Receive task batch|          |          |
                |                          |  Distribute tasks--+----> Get(block) &   |
                |                          |                    |      Execute task   |
                |                          |                    |          |          |
                | <------------------------+--Return results----+----  Post result    |
                |                          |                    |          |          |
                |                          |                    |          +----------+
                |                          |                IPC-Qeueues

    """
    def __init__(self, *,
                 addresses,
                 address_probe_timeout,
                 task_port,
                 result_port,
                 cores_per_worker,
                 mem_per_worker,
                 max_workers_per_node,
                 prefetch_capacity,
                 uid,
                 block_id,
                 heartbeat_threshold,
                 heartbeat_period,
                 poll_period,
                 cpu_affinity,
                 enable_mpi_mode: bool = False,
                 mpi_launcher: str = "mpiexec",
                 available_accelerators: Sequence[str],
                 cert_dir: Optional[str],
                 drain_period: Optional[int]):
        """
        Parameters
        ----------
        addresses : str
             comma separated list of addresses for the interchange

        address_probe_timeout : int
             Timeout in seconds for the address probe to detect viable addresses
             to the interchange.

        uid : str
             string unique identifier

        block_id : str
             Block identifier that maps managers to the provider blocks they belong to.

        cores_per_worker : float
             cores to be assigned to each worker. Oversubscription is possible
             by setting cores_per_worker < 1.0.

        mem_per_worker : float
             GB of memory required per worker. If this option is specified, the node manager
             will check the available memory at startup and limit the number of workers such that
             the there's sufficient memory for each worker. If set to None, memory on node is not
             considered in the determination of workers to be launched on node by the manager.

        max_workers_per_node : int | float
             Caps the maximum number of workers that can be launched.

        prefetch_capacity : int
             Number of tasks that could be prefetched over available worker capacity.
             When there are a few tasks (<100) or when tasks are long running, this option should
             be set to 0 for better load balancing.

        heartbeat_threshold : int
             Seconds since the last message from the interchange after which the
             interchange is assumed to be un-available, and the manager initiates shutdown.

             Number of seconds since the last message from the interchange after which the worker
             assumes that the interchange is lost and the manager shuts down.

        heartbeat_period : int
             Number of seconds after which a heartbeat message is sent to the interchange, and workers
             are checked for liveness.

        poll_period : int
             Timeout period used by the manager in milliseconds.

        cpu_affinity : str
             Whether or how each worker should force its affinity to different CPUs

        available_accelerators: list of str
            List of accelerators available to the workers.

        enable_mpi_mode: bool
            When set to true, the manager assumes ownership of the batch job and each worker
            claims a subset of nodes from a shared pool to execute multi-node mpi tasks. Node
            info is made available to workers via env vars.

        mpi_launcher: str
            Set to one of the supported MPI launchers: ("srun", "aprun", "mpiexec")

        cert_dir : str | None
            Path to the certificate directory.

        drain_period: int | None
            Number of seconds to drain after  TODO: could be a nicer timespec involving m,s,h qualifiers for user friendliness?
        """

        logger.info("Manager initializing")

        self._start_time = time.time()

        try:
            ix_address = probe_addresses(addresses.split(','), task_port, timeout=address_probe_timeout)
            if not ix_address:
                raise Exception("No viable address found")
            else:
                logger.info("Connection to Interchange successful on {}".format(ix_address))
                task_q_url = tcp_url(ix_address, task_port)
                result_q_url = tcp_url(ix_address, result_port)
                logger.info("Task url : {}".format(task_q_url))
                logger.info("Result url : {}".format(result_q_url))
        except Exception:
            logger.exception("Caught exception while trying to determine viable address to interchange")
            print("Failed to find a viable address to connect to interchange. Exiting")
            exit(5)

        self.cert_dir = cert_dir
        self.zmq_context = curvezmq.ClientContext(self.cert_dir)

        self._task_q_url = task_q_url
        self._result_q_url = result_q_url

        self.uid = uid
        self.block_id = block_id
        self.start_time = time.time()

        self.enable_mpi_mode = enable_mpi_mode
        self.mpi_launcher = mpi_launcher

        if os.environ.get('PARSL_CORES'):
            cores_on_node = int(os.environ['PARSL_CORES'])
        else:
            cores_on_node = SpawnContext.cpu_count()

        if os.environ.get('PARSL_MEMORY_GB'):
            available_mem_on_node = float(os.environ['PARSL_MEMORY_GB'])
        else:
            available_mem_on_node = round(psutil.virtual_memory().available / (2**30), 1)

        self.max_workers_per_node = max_workers_per_node
        self.prefetch_capacity = prefetch_capacity

        mem_slots = max_workers_per_node
        # Avoid a divide by 0 error.
        if mem_per_worker and mem_per_worker > 0:
            mem_slots = math.floor(available_mem_on_node / mem_per_worker)

        self.worker_count: int = min(max_workers_per_node,
                                     mem_slots,
                                     math.floor(cores_on_node / cores_per_worker))

        self._mp_manager = SpawnContext.Manager()  # Starts a server process
        self._tasks_in_progress = self._mp_manager.dict()
        self._stop_event = threading.Event()  # when set, will begin shutdown process

        self.monitoring_queue = self._mp_manager.Queue()
        self.pending_task_queue = SpawnContext.Queue()
        self.pending_result_queue = SpawnContext.Queue()
        self.task_scheduler: TaskScheduler
        if self.enable_mpi_mode:
            self.task_scheduler = MPITaskScheduler(
                self.pending_task_queue,
                self.pending_result_queue,
            )
        else:
            self.task_scheduler = TaskScheduler(
                self.pending_task_queue,
                self.pending_result_queue
            )
        self.ready_worker_count = SpawnContext.Value("i", 0)

        self.max_queue_size = self.prefetch_capacity + self.worker_count

        self.tasks_per_round = 1

        self.heartbeat_period = heartbeat_period
        self.heartbeat_threshold = heartbeat_threshold
        self.poll_period = poll_period

        self.drain_time: float
        if drain_period:
            self.drain_time = self._start_time + drain_period
            logger.info(f"Will request drain at {self.drain_time}")
        else:
            self.drain_time = float('inf')

        self.cpu_affinity = cpu_affinity

        # Define accelerator available, adjust worker count accordingly
        self.available_accelerators = available_accelerators
        self.accelerators_available = len(available_accelerators) > 0
        if self.accelerators_available:
            self.worker_count = min(len(self.available_accelerators), self.worker_count)
        logger.info("Manager will spawn {} workers".format(self.worker_count))

    def create_reg_message(self):
        """ Creates a registration message to identify the worker to the interchange
        """
        msg = {'type': 'registration',
               'parsl_v': PARSL_VERSION,
               'python_v': "{}.{}.{}".format(sys.version_info.major,
                                             sys.version_info.minor,
                                             sys.version_info.micro),
               'packages': {dist.metadata['Name']: dist.version for dist in distributions()},
               'worker_count': self.worker_count,
               'uid': self.uid,
               'block_id': self.block_id,
               'start_time': self.start_time,
               'prefetch_capacity': self.prefetch_capacity,
               'max_capacity': self.worker_count + self.prefetch_capacity,
               'os': platform.system(),
               'hostname': platform.node(),
               'dir': os.getcwd(),
               'cpu_count': psutil.cpu_count(logical=False),
               'total_memory': psutil.virtual_memory().total,
               }
        b_msg = json.dumps(msg).encode('utf-8')
        return b_msg

    @staticmethod
    def heartbeat_to_incoming(task_incoming: zmq.Socket) -> None:
        """ Send heartbeat to the incoming task queue
        """
        msg = {'type': 'heartbeat'}
        # don't need to dumps and encode this every time - could do as a global on import?
        b_msg = json.dumps(msg).encode('utf-8')
        task_incoming.send(b_msg)
        logger.debug("Sent heartbeat")

    @staticmethod
    def drain_to_incoming(task_incoming: zmq.Socket) -> None:
        """ Send heartbeat to the incoming task queue
        """
        msg = {'type': 'drain'}
        b_msg = json.dumps(msg).encode('utf-8')
        task_incoming.send(b_msg)
        logger.debug("Sent drain")

    @wrap_with_logs
    def pull_tasks(self):
        """ Pull tasks from the incoming tasks zmq pipe onto the internal
        pending task queue
        """
        logger.info("starting")

        # Linger is set to 0, so that the manager can exit even when there might be
        # messages in the pipe
        task_incoming = self.zmq_context.socket(zmq.DEALER)
        task_incoming.setsockopt(zmq.IDENTITY, self.uid.encode('utf-8'))
        task_incoming.setsockopt(zmq.LINGER, 0)
        task_incoming.connect(self._task_q_url)
        logger.info("Manager task pipe connected to interchange")

        poller = zmq.Poller()
        poller.register(task_incoming, zmq.POLLIN)

        # Send a registration message
        msg = self.create_reg_message()
        logger.debug("Sending registration message: {}".format(msg))
        task_incoming.send(msg)
        last_beat = time.time()
        last_interchange_contact = time.time()
        task_recv_counter = 0

        while not self._stop_event.is_set():

            # This loop will sit inside poller.poll until either a message
            # arrives or one of these event times is reached. This code
            # assumes that the event times won't change except on iteration
            # of this loop - so will break if a different thread does
            # anything to bring one of the event times earlier - and that the
            # time here are correctly copy-pasted from the relevant if
            # statements.
            next_interesting_event_time = min(last_beat + self.heartbeat_period,
                                              self.drain_time,
                                              last_interchange_contact + self.heartbeat_threshold)
            try:
                pending_task_count = self.pending_task_queue.qsize()
            except NotImplementedError:
                # Ref: https://github.com/python/cpython/blob/6d5e0dc0e330f4009e8dc3d1642e46b129788877/Lib/multiprocessing/queues.py#L125
                pending_task_count = f"pending task count is not available on {platform.system()}"

            logger.debug("ready workers: {}, pending tasks: {}".format(self.ready_worker_count.value,
                                                                       pending_task_count))

            if time.time() >= last_beat + self.heartbeat_period:
                self.heartbeat_to_incoming(task_incoming)
                last_beat = time.time()

            if time.time() > self.drain_time:
                logger.info("Requesting drain")
                self.drain_to_incoming(task_incoming)
                # This will start the pool draining...
                # Drained exit behaviour does not happen here. It will be
                # driven by the interchange sending a DRAINED_CODE message.

                # now set drain time to the far future so we don't send a drain
                # message every iteration.
                self.drain_time = float('inf')

            poll_duration_s = max(0, next_interesting_event_time - time.time())
            socks = dict(poller.poll(timeout=poll_duration_s * 1000))

            if socks.get(task_incoming) == zmq.POLLIN:
                _, pkl_msg = task_incoming.recv_multipart()
                tasks = pickle.loads(pkl_msg)
                last_interchange_contact = time.time()

                if tasks == HEARTBEAT_CODE:
                    logger.debug("Got heartbeat from interchange")
                elif tasks == DRAINED_CODE:
                    logger.info("Got fully drained message from interchange - setting kill flag")
                    self._stop_event.set()
                else:
                    task_recv_counter += len(tasks)
                    logger.debug("Got executor tasks: {}, cumulative count of tasks: {}".format(
                        [t['task_id'] for t in tasks], task_recv_counter
                    ))

                    for task in tasks:
                        self.task_scheduler.put_task(task)

            else:
                logger.debug("No incoming tasks")

                # Only check if no messages were received.
                if time.time() >= last_interchange_contact + self.heartbeat_threshold:
                    logger.critical("Missing contact with interchange beyond heartbeat_threshold")
                    self._stop_event.set()
                    logger.critical("Exiting")
                    break

        task_incoming.close()
        logger.info("Exiting")

    @wrap_with_logs
    def push_results(self):
        """ Listens on the pending_result_queue and sends out results via zmq
        """
        logger.debug("Starting result push thread")

        # Linger is set to 0, so that the manager can exit even when there might be
        # messages in the pipe
        result_outgoing = self.zmq_context.socket(zmq.DEALER)
        result_outgoing.setsockopt(zmq.IDENTITY, self.uid.encode('utf-8'))
        result_outgoing.setsockopt(zmq.LINGER, 0)
        result_outgoing.connect(self._result_q_url)
        logger.info("Manager result pipe connected to interchange")

        while not self._stop_event.is_set():
            logger.debug("Starting pending_result_queue get")
            try:
                r = self.task_scheduler.get_result()
                if r is None:
                    continue
                logger.debug("Result received from worker: %s", id(r))
                result_outgoing.send(r)
                logger.debug("Result sent to interchange: %s", id(r))
            except Exception:
                logger.exception("Failed to send result to interchange")

        result_outgoing.close()
        logger.debug("Exiting")

    @wrap_with_logs
    def heartbeater(self):
        while not self._stop_event.wait(self.heartbeat_period):
            heartbeat_message = f"heartbeat_period={self.heartbeat_period} seconds"
            logger.info(f"Sending heartbeat via results connection: {heartbeat_message}")
            self.pending_result_queue.put(pickle.dumps({'type': 'heartbeat'}))

    def worker_watchdog(self, procs: dict[int, SpawnProcess]):
        """Keeps workers alive."""
        logger.debug("Starting worker watchdog")

        while not self._stop_event.wait(self.heartbeat_period):
            for worker_id, p in procs.items():
                if not p.is_alive():
                    logger.error("Worker {} has died".format(worker_id))
                    try:
                        task = self._tasks_in_progress.pop(worker_id)
                        logger.info("Worker {} was busy when it died".format(worker_id))
                        try:
                            raise WorkerLost(worker_id, platform.node())
                        except Exception:
                            logger.info("Putting exception for executor task {} in the pending result queue".format(task['task_id']))
                            result_package = {'type': 'result',
                                              'task_id': task['task_id'],
                                              'exception': serialize(RemoteExceptionWrapper(*sys.exc_info()))}
                            pkl_package = pickle.dumps(result_package)
                            self.pending_result_queue.put(pkl_package)
                    except KeyError:
                        logger.info("Worker {} was not busy when it died".format(worker_id))

                    procs[worker_id] = self._start_worker(worker_id)
                    logger.info("Worker {} has been restarted".format(worker_id))

        logger.debug("Exiting")

    @wrap_with_logs
    def handle_monitoring_messages(self):
        """Transfer messages from the managed monitoring queue to the result queue.

        We separate the queues so that the result queue does not rely on a manager
        process, which adds overhead that causes slower queue operations but enables
        use across processes started in fork and spawn contexts.

        We transfer the messages to the result queue to reuse the ZMQ connection between
        the manager and the interchange.
        """
        logger.debug("Starting monitoring handler thread")

        while not self._stop_event.is_set():
            try:
                logger.debug("Starting monitor_queue.get()")
                msg = self.monitoring_queue.get(block=True)
                if msg is None:
                    continue
                logger.debug("Got a monitoring message")
                self.pending_result_queue.put(msg)
                logger.debug("Put monitoring message on pending_result_queue")
            except Exception:
                logger.exception("Failed to forward monitoring message")

        logger.debug("Exiting")

    def start(self):
        """ Start the worker processes.

        TODO: Move task receiving to a thread
        """
        procs: dict[int, SpawnProcess] = {}
        for worker_id in range(self.worker_count):
            procs[worker_id] = self._start_worker(worker_id)

        logger.debug("Workers started")

        thr_task_puller = threading.Thread(target=self.pull_tasks, name="Task-Puller")
        thr_result_pusher = threading.Thread(
            target=self.push_results, name="Result-Pusher"
        )
        thr_worker_watchdog = threading.Thread(
            target=self.worker_watchdog, args=(procs,), name="worker-watchdog"
        )
        thr_monitoring_handler = threading.Thread(
            target=self.handle_monitoring_messages, name="Monitoring-Handler"
        )
        thr_heartbeater = threading.Thread(target=self.heartbeater, name="Heartbeater")

        thr_task_puller.start()
        thr_result_pusher.start()
        thr_worker_watchdog.start()
        thr_monitoring_handler.start()
        thr_heartbeater.start()

        logger.info("Manager threads started")

        # This might need a multiprocessing event to signal back.
        self._stop_event.wait()
        logger.info("Stop event set; terminating worker processes")

        # Invite blocking threads to quit
        self.monitoring_queue.put(None)
        self.pending_result_queue.put(None)

        thr_heartbeater.join()
        thr_task_puller.join()
        thr_result_pusher.join()
        thr_worker_watchdog.join()
        thr_monitoring_handler.join()

        for worker_id in procs:
            p = procs[worker_id]
            proc_info = f"(PID: {p.pid}, Worker ID: {worker_id})"
            logger.debug(f"Signaling worker {p.name} (TERM). {proc_info}")
            p.terminate()

        self.zmq_context.term()

        # give processes 1 second to gracefully shut themselves down, based on the
        # SIGTERM (.terminate()) just sent; after then, we pull the plug.
        force_child_shutdown_at = time.monotonic() + 1
        while procs:
            worker_id, p = procs.popitem()
            timeout = max(force_child_shutdown_at - time.monotonic(), 0.000001)
            p.join(timeout=timeout)
            proc_info = f"(PID: {p.pid}, Worker ID: {worker_id})"
            if p.exitcode is not None:
                logger.debug(
                    "Worker joined successfully.  %s (exitcode: %s)", proc_info, p.exitcode
                )

            else:
                logger.warning(
                    f"Worker {p.name} ({worker_id}) failed to terminate in a timely"
                    f" manner; sending KILL signal to process. {proc_info}"
                )
                p.kill()
                p.join()
            p.close()

        delta = time.time() - self._start_time
        logger.info("process_worker_pool ran for {} seconds".format(delta))

    def _start_worker(self, worker_id: int) -> SpawnProcess:
        p = SpawnContext.Process(
            target=worker,
            args=(
                worker_id,
                self.uid,
                self.worker_count,
                self.pending_task_queue,
                self.pending_result_queue,
                self.monitoring_queue,
                self.ready_worker_count,
                self._tasks_in_progress,
                self.cpu_affinity,
                self.available_accelerators[worker_id] if self.accelerators_available else None,
                self.block_id,
                self.heartbeat_period,
                os.getpid(),
                args.logdir,
                args.debug,
                self.mpi_launcher,
            ),
            name="HTEX-Worker-{}".format(worker_id),
        )
        p.start()
        return p


def update_resource_spec_env_vars(mpi_launcher: str, resource_spec: Dict, node_info: List[str]) -> None:
    prefix_table = compose_all(mpi_launcher, resource_spec=resource_spec, node_hostnames=node_info)
    for key in prefix_table:
        os.environ[key] = prefix_table[key]


def _init_mpi_env(mpi_launcher: str, resource_spec: Dict):
    node_list = resource_spec.get("MPI_NODELIST")
    if node_list is None:
        return
    nodes_for_task = node_list.split(',')
    logger.info(f"Launching task on provisioned nodes: {nodes_for_task}")
    update_resource_spec_env_vars(mpi_launcher=mpi_launcher, resource_spec=resource_spec, node_info=nodes_for_task)


@wrap_with_logs(target="worker_log")
def worker(
    worker_id: int,
    pool_id: str,
    pool_size: int,
    task_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
    monitoring_queue: queue.Queue,
    ready_worker_count: Synchronized,
    tasks_in_progress: DictProxy,
    cpu_affinity: str,
    accelerator: Optional[str],
    block_id: str,
    task_queue_timeout: int,
    manager_pid: int,
    logdir: str,
    debug: bool,
    mpi_launcher: str,
):
    # override the global logger inherited from the __main__ process (which
    # usually logs to manager.log) with one specific to this worker.
    global logger
    logger = start_file_logger('{}/block-{}/{}/worker_{}.log'.format(logdir, block_id, pool_id, worker_id),
                               worker_id,
                               name="worker_log",
                               level=logging.DEBUG if debug else logging.INFO)

    # Store worker ID as an environment variable
    os.environ['PARSL_WORKER_RANK'] = str(worker_id)
    os.environ['PARSL_WORKER_COUNT'] = str(pool_size)
    os.environ['PARSL_WORKER_POOL_ID'] = str(pool_id)
    os.environ['PARSL_WORKER_BLOCK_ID'] = str(block_id)

    import parsl.executors.high_throughput.monitoring_info as mi
    mi.result_queue = monitoring_queue

    logger.info('Worker {} started'.format(worker_id))
    if debug:
        logger.debug("Debug logging enabled")

    # If desired, set process affinity
    if cpu_affinity != "none":
        # Count the number of cores per worker
        # OSX does not implement os.sched_getaffinity
        avail_cores = sorted(os.sched_getaffinity(0))  # type: ignore[attr-defined, unused-ignore]
        cores_per_worker = len(avail_cores) // pool_size
        assert cores_per_worker > 0, "Affinity does not work if there are more workers than cores"

        # Determine this worker's cores
        if cpu_affinity == "block":
            my_cores = avail_cores[cores_per_worker * worker_id:cores_per_worker * (worker_id + 1)]
        elif cpu_affinity == "block-reverse":
            cpu_worker_id = pool_size - worker_id - 1  # To assign in reverse order
            my_cores = avail_cores[cores_per_worker * cpu_worker_id:cores_per_worker * (cpu_worker_id + 1)]
        elif cpu_affinity == "alternating":
            my_cores = avail_cores[worker_id::pool_size]
        elif cpu_affinity[0:4] == "list":
            thread_ranks = cpu_affinity.split(":")[1:]
            if len(thread_ranks) != pool_size:
                raise ValueError("Affinity list {} has wrong number of thread ranks".format(cpu_affinity))
            threads = thread_ranks[worker_id]
            thread_list = threads.split(",")
            my_cores = []
            for tl in thread_list:
                thread_range = tl.split("-")
                if len(thread_range) == 1:
                    my_cores.append(int(thread_range[0]))
                elif len(thread_range) == 2:
                    start_thread = int(thread_range[0])
                    end_thread = int(thread_range[1]) + 1
                    my_cores += list(range(start_thread, end_thread))
                else:
                    raise ValueError("Affinity list formatting is not expected {}".format(cpu_affinity))
        else:
            raise ValueError("Affinity strategy {} is not supported".format(cpu_affinity))

        # Set the affinity for OpenMP
        #  See: https://hpc-tutorials.llnl.gov/openmp/ProcessThreadAffinity.pdf
        proc_list = ",".join(map(str, my_cores))
        os.environ["OMP_NUM_THREADS"] = str(len(my_cores))
        os.environ["GOMP_CPU_AFFINITY"] = proc_list  # Compatible with GCC OpenMP
        os.environ["KMP_AFFINITY"] = f"explicit,proclist=[{proc_list}]"  # For Intel OpenMP

        # Set the affinity for this worker
        # OSX does not implement os.sched_setaffinity so type checking
        # is ignored here in two ways:
        # On a platform without sched_setaffinity, that attribute will not
        # be defined, so ignore[attr-defined] will tell mypy to ignore this
        # incorrect-for-OS X attribute access.
        # On a platform with sched_setaffinity, that type: ignore message
        # will be redundant, and ignore[unused-ignore] tells mypy to ignore
        # that this ignore is unneeded.
        os.sched_setaffinity(0, my_cores)  # type: ignore[attr-defined, unused-ignore]
        logger.info("Set worker CPU affinity to {}".format(my_cores))

    # If desired, pin to accelerator
    if accelerator is not None:

        # If CUDA devices, find total number of devices to allow for MPS
        # See: https://developer.nvidia.com/system-management-interface
        nvidia_smi_cmd = "nvidia-smi -L > /dev/null && nvidia-smi -L | wc -l"
        nvidia_smi_ret = subprocess.run(nvidia_smi_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if nvidia_smi_ret.returncode == 0:
            num_cuda_devices = int(nvidia_smi_ret.stdout.split()[0])
        else:
            num_cuda_devices = None

        try:
            if num_cuda_devices is not None:
                procs_per_cuda_device = pool_size // num_cuda_devices
                partitioned_accelerator = str(int(accelerator) // procs_per_cuda_device)  # multiple workers will share a GPU
                os.environ["CUDA_VISIBLE_DEVICES"] = partitioned_accelerator
                logger.info(f'Pinned worker to partitioned cuda device: {partitioned_accelerator}')
            else:
                os.environ["CUDA_VISIBLE_DEVICES"] = accelerator
        except (TypeError, ValueError, ZeroDivisionError):
            os.environ["CUDA_VISIBLE_DEVICES"] = accelerator
        os.environ["ROCR_VISIBLE_DEVICES"] = accelerator
        os.environ["ZE_AFFINITY_MASK"] = accelerator
        os.environ["ZE_ENABLE_PCI_ID_DEVICE_ORDER"] = '1'

        logger.info(f'Pinned worker to accelerator: {accelerator}')

    def manager_is_alive():
        try:
            # This does not kill the process, but instead raises
            # an exception if the process doesn't exist
            os.kill(manager_pid, 0)
        except OSError:
            logger.critical(f"Manager ({manager_pid}) died; worker {worker_id} shutting down")
            return False
        else:
            return True

    worker_enqueued = False
    while manager_is_alive():
        if not worker_enqueued:
            with ready_worker_count.get_lock():
                ready_worker_count.value += 1
            worker_enqueued = True

        try:
            # The worker will receive {'task_id':<tid>, 'buffer':<buf>}
            req = task_queue.get(timeout=task_queue_timeout)
        except queue.Empty:
            continue

        tasks_in_progress[worker_id] = req
        tid = req['task_id']
        logger.info("Received executor task {}".format(tid))

        with ready_worker_count.get_lock():
            ready_worker_count.value -= 1
        worker_enqueued = False

        _init_mpi_env(mpi_launcher=mpi_launcher, resource_spec=req["resource_spec"])

        try:
            result = execute_task(req['buffer'])
            serialized_result = serialize(result, buffer_threshold=1000000)
        except Exception as e:
            logger.info('Caught an exception: {}'.format(e))
            result_package = {'type': 'result', 'task_id': tid, 'exception': serialize(RemoteExceptionWrapper(*sys.exc_info()))}
        else:
            result_package = {'type': 'result', 'task_id': tid, 'result': serialized_result}
            # logger.debug("Result: {}".format(result))

        logger.info("Completed executor task {}".format(tid))
        try:
            pkl_package = pickle.dumps(result_package)
        except Exception:
            logger.exception("Caught exception while trying to pickle the result package")
            pkl_package = pickle.dumps({'type': 'result', 'task_id': tid,
                                        'exception': serialize(RemoteExceptionWrapper(*sys.exc_info()))
                                        })

        result_queue.put(pkl_package)
        tasks_in_progress.pop(worker_id)
        logger.info("All processing finished for executor task {}".format(tid))


def start_file_logger(filename, rank, name='parsl', level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
       -  None
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d " \
                        "%(process)d %(threadName)s " \
                        "[%(levelname)s]  %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def get_arg_parser() -> argparse.ArgumentParser:

    def strategyorlist(s: str):
        s = s.lower()
        allowed_strategies = ("none", "block", "alternating", "block-reverse")
        if s in allowed_strategies:
            return s
        elif s[0:4] == "list":
            return s
        err_msg = f"cpu-affinity must be one of {allowed_strategies} or a list format"
        raise argparse.ArgumentTypeError(err_msg)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--debug", action='store_true', help="Enable logging at DEBUG level",
    )
    parser.add_argument(
        "-a",
        "--addresses",
        required=True,
        help="Comma separated list of addresses at which the interchange could be reached",
    )
    parser.add_argument(
        "--cert_dir", required=True, help="Path to certificate directory."
    )
    parser.add_argument(
        "-l",
        "--logdir",
        default="process_worker_pool_logs",
        help="Process worker pool log directory",
    )
    parser.add_argument(
        "-u",
        "--uid",
        default=str(uuid.uuid4()).split('-')[-1],
        help="Unique identifier string for Manager",
    )
    parser.add_argument(
        "-b", "--block_id", default=None, help="Block identifier for Manager"
    )
    parser.add_argument(
        "-c",
        "--cores_per_worker",
        default="1.0",
        help="Number of cores assigned to each worker process. Default=1.0",
    )
    parser.add_argument(
        "-m",
        "--mem_per_worker",
        default=0,
        help="GB of memory assigned to each worker process. Default=0, no assignment",
    )
    parser.add_argument(
        "-t",
        "--task_port",
        required=True,
        help="Task port for receiving tasks from the interchange",
    )
    parser.add_argument(
        "--max_workers_per_node",
        default=float('inf'),
        help="Caps the maximum workers that can be launched, default:infinity",
    )
    parser.add_argument(
        "-p",
        "--prefetch_capacity",
        default=0,
        help="Number of tasks that can be prefetched to the manager. Default is 0.",
    )
    parser.add_argument(
        "--hb_period",
        default=30,
        help="Heartbeat period in seconds. Uses manager default unless set",
    )
    parser.add_argument(
        "--hb_threshold",
        default=120,
        help="Heartbeat threshold in seconds. Uses manager default unless set",
    )
    parser.add_argument(
        "--drain_period",
        default=None,
        help="Drain this pool after specified number of seconds. By default, does not drain.",
    )
    parser.add_argument(
        "--address_probe_timeout",
        default=30,
        help="Timeout to probe for viable address to interchange. Default: 30s",
    )
    parser.add_argument(
        "--poll", default=10, help="Poll period used in milliseconds"
    )
    parser.add_argument(
        "-r",
        "--result_port",
        required=True,
        help="Result port for posting results to the interchange",
    )
    parser.add_argument(
        "--cpu-affinity",
        type=strategyorlist,
        required=True,
        help="Whether/how workers should control CPU affinity.",
    )
    parser.add_argument(
        "--available-accelerators",
        type=str,
        nargs="*",
        default=[],
        help="Names of available accelerators, if not given assumed to be zero accelerators available",
    )
    parser.add_argument(
        "--enable_mpi_mode", action='store_true', help="Enable MPI mode"
    )
    parser.add_argument(
        "--mpi-launcher",
        type=str,
        choices=VALID_LAUNCHERS,
        help="MPI launcher to use iff enable_mpi_mode=true",
    )

    return parser


if __name__ == "__main__":
    parser = get_arg_parser()
    args = parser.parse_args()

    os.makedirs(os.path.join(args.logdir, "block-{}".format(args.block_id), args.uid), exist_ok=True)

    logger = start_file_logger(
        f'{args.logdir}/block-{args.block_id}/{args.uid}/manager.log',
        0,
        level=logging.DEBUG if args.debug is True else logging.INFO
    )
    logger.info(
        f"\n  Python version: {sys.version}"
        f"\n  Debug logging: {args.debug}"
        f"\n  Certificates dir: {args.cert_dir}"
        f"\n  Log dir: {args.logdir}"
        f"\n  Manager ID: {args.uid}"
        f"\n  Block ID: {args.block_id}"
        f"\n  cores_per_worker: {args.cores_per_worker}"
        f"\n  mem_per_worker: {args.mem_per_worker}"
        f"\n  task_port: {args.task_port}"
        f"\n  result_port: {args.result_port}"
        f"\n  addresses: {args.addresses}"
        f"\n  max_workers_per_node: {args.max_workers_per_node}"
        f"\n  poll_period: {args.poll}"
        f"\n  address_probe_timeout: {args.address_probe_timeout}"
        f"\n  Prefetch capacity: {args.prefetch_capacity}"
        f"\n  Heartbeat threshold: {args.hb_threshold}"
        f"\n  Heartbeat period: {args.hb_period}"
        f"\n  Drain period: {args.drain_period}"
        f"\n  CPU affinity: {args.cpu_affinity}"
        f"\n  Accelerators: {' '.join(args.available_accelerators)}"
        f"\n  enable_mpi_mode: {args.enable_mpi_mode}"
        f"\n  mpi_launcher: {args.mpi_launcher}"
    )
    try:
        manager = Manager(task_port=args.task_port,
                          result_port=args.result_port,
                          addresses=args.addresses,
                          address_probe_timeout=int(args.address_probe_timeout),
                          uid=args.uid,
                          block_id=args.block_id,
                          cores_per_worker=float(args.cores_per_worker),
                          mem_per_worker=None if args.mem_per_worker == 'None' else float(args.mem_per_worker),
                          max_workers_per_node=(
                              args.max_workers_per_node if args.max_workers_per_node == float('inf')
                              else int(args.max_workers_per_node)
                          ),
                          prefetch_capacity=int(args.prefetch_capacity),
                          heartbeat_threshold=int(args.hb_threshold),
                          heartbeat_period=int(args.hb_period),
                          drain_period=None if args.drain_period == "None" else int(args.drain_period),
                          poll_period=int(args.poll),
                          cpu_affinity=args.cpu_affinity,
                          enable_mpi_mode=args.enable_mpi_mode,
                          mpi_launcher=args.mpi_launcher,
                          available_accelerators=args.available_accelerators,
                          cert_dir=None if args.cert_dir == "None" else args.cert_dir)
        manager.start()

    except Exception:
        logger.critical("Process worker pool exiting with an exception", exc_info=True)
        raise
    else:
        logger.info("Process worker pool exiting normally")
        print("Process worker pool exiting normally")
