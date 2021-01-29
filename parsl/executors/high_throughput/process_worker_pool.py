#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import platform
import threading
import pickle
import time
import datetime
import queue
import uuid
import zmq
import math
import json
import psutil
import multiprocessing

from parsl.process_loggers import wrap_with_logs

from parsl.version import VERSION as PARSL_VERSION
from parsl.app.errors import RemoteExceptionWrapper
from parsl.executors.high_throughput.errors import WorkerLost
from parsl.executors.high_throughput.probe import probe_addresses
if platform.system() == 'Darwin':
    from parsl.executors.high_throughput.mac_safe_queue import MacSafeQueue as mpQueue
else:
    from multiprocessing import Queue as mpQueue

from parsl.serialize import unpack_apply_message, serialize

HEARTBEAT_CODE = (2 ** 32) - 1


class Manager(object):
    """ Manager manages task execution by the workers

                |         0mq              |    Manager         |   Worker Processes
                |                          |                    |
                | <-----Request N task-----+--Count task reqs   |      Request task<--+
    Interchange | -------------------------+->Receive task batch|          |          |
                |                          |  Distribute tasks--+----> Get(block) &   |
                |                          |                    |      Execute task   |
                |                          |                    |          |          |
                | <------------------------+--Return results----+----  Post result    |
                |                          |                    |          |          |
                |                          |                    |          +----------+
                |                          |                IPC-Qeueues

    """
    def __init__(self,
                 addresses="127.0.0.1",
                 address_probe_timeout=30,
                 task_port="50097",
                 result_port="50098",
                 cores_per_worker=1,
                 mem_per_worker=None,
                 max_workers=float('inf'),
                 prefetch_capacity=0,
                 uid=None,
                 block_id=None,
                 heartbeat_threshold=120,
                 heartbeat_period=30,
                 poll_period=10,
                 cpu_affinity=False):
        """
        Parameters
        ----------
        addresses : str
             comma separated list of addresses for the interchange

        address_probe_timeout : int
             Timeout in seconds for the address probe to detect viable addresses
             to the interchange. Default : 30s

        uid : str
             string unique identifier

        block_id : str
             Block identifier that maps managers to the provider blocks they belong to.

        cores_per_worker : float
             cores to be assigned to each worker. Oversubscription is possible
             by setting cores_per_worker < 1.0. Default=1

        mem_per_worker : float
             GB of memory required per worker. If this option is specified, the node manager
             will check the available memory at startup and limit the number of workers such that
             the there's sufficient memory for each worker. If set to None, memory on node is not
             considered in the determination of workers to be launched on node by the manager.
             Default: None

        max_workers : int
             caps the maximum number of workers that can be launched.
             default: infinity

        prefetch_capacity : int
             Number of tasks that could be prefetched over available worker capacity.
             When there are a few tasks (<100) or when tasks are long running, this option should
             be set to 0 for better load balancing. Default is 0.

        heartbeat_threshold : int
             Seconds since the last message from the interchange after which the
             interchange is assumed to be un-available, and the manager initiates shutdown. Default:120s

             Number of seconds since the last message from the interchange after which the worker
             assumes that the interchange is lost and the manager shuts down. Default:120

        heartbeat_period : int
             Number of seconds after which a heartbeat message is sent to the interchange

        poll_period : int
             Timeout period used by the manager in milliseconds. Default: 10ms

        cpu_affinity : str
             Whether each worker should force its affinity to different CPUs
        """

        logger.info("Manager started")

        try:
            ix_address = probe_addresses(addresses.split(','), task_port, timeout=address_probe_timeout)
            if not ix_address:
                raise Exception("No viable address found")
            else:
                logger.info("Connection to Interchange successful on {}".format(ix_address))
                task_q_url = "tcp://{}:{}".format(ix_address, task_port)
                result_q_url = "tcp://{}:{}".format(ix_address, result_port)
                logger.info("Task url : {}".format(task_q_url))
                logger.info("Result url : {}".format(result_q_url))
        except Exception:
            logger.exception("Caught exception while trying to determine viable address to interchange")
            print("Failed to find a viable address to connect to interchange. Exiting")
            exit(5)

        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.setsockopt(zmq.IDENTITY, uid.encode('utf-8'))
        # Linger is set to 0, so that the manager can exit even when there might be
        # messages in the pipe
        self.task_incoming.setsockopt(zmq.LINGER, 0)
        self.task_incoming.connect(task_q_url)

        self.result_outgoing = self.context.socket(zmq.DEALER)
        self.result_outgoing.setsockopt(zmq.IDENTITY, uid.encode('utf-8'))
        self.result_outgoing.setsockopt(zmq.LINGER, 0)
        self.result_outgoing.connect(result_q_url)
        logger.info("Manager connected")

        self.uid = uid
        self.block_id = block_id

        if os.environ.get('PARSL_CORES'):
            cores_on_node = int(os.environ['PARSL_CORES'])
        else:
            cores_on_node = multiprocessing.cpu_count()

        if os.environ.get('PARSL_MEMORY_GB'):
            available_mem_on_node = float(os.environ['PARSL_MEMORY_GB'])
        else:
            available_mem_on_node = round(psutil.virtual_memory().available / (2**30), 1)

        self.max_workers = max_workers
        self.prefetch_capacity = prefetch_capacity

        mem_slots = max_workers
        # Avoid a divide by 0 error.
        if mem_per_worker and mem_per_worker > 0:
            mem_slots = math.floor(available_mem_on_node / mem_per_worker)

        self.worker_count = min(max_workers,
                                mem_slots,
                                math.floor(cores_on_node / cores_per_worker))
        logger.info("Manager will spawn {} workers".format(self.worker_count))

        self.pending_task_queue = mpQueue()
        self.pending_result_queue = mpQueue()
        self.ready_worker_queue = mpQueue()

        self.max_queue_size = self.prefetch_capacity + self.worker_count

        self.tasks_per_round = 1

        self.heartbeat_period = heartbeat_period
        self.heartbeat_threshold = heartbeat_threshold
        self.poll_period = poll_period
        self.cpu_affinity = cpu_affinity

    def create_reg_message(self):
        """ Creates a registration message to identify the worker to the interchange
        """
        msg = {'parsl_v': PARSL_VERSION,
               'python_v': "{}.{}.{}".format(sys.version_info.major,
                                             sys.version_info.minor,
                                             sys.version_info.micro),
               'worker_count': self.worker_count,
               'uid': self.uid,
               'block_id': self.block_id,
               'prefetch_capacity': self.prefetch_capacity,
               'max_capacity': self.worker_count + self.prefetch_capacity,
               'os': platform.system(),
               'hostname': platform.node(),
               'dir': os.getcwd(),
               'cpu_count': psutil.cpu_count(logical=False),
               'total_memory': psutil.virtual_memory().total,
               'reg_time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        b_msg = json.dumps(msg).encode('utf-8')
        return b_msg

    def heartbeat(self):
        """ Send heartbeat to the incoming task queue
        """
        heartbeat = (HEARTBEAT_CODE).to_bytes(4, "little")
        r = self.task_incoming.send(heartbeat)
        logger.debug("Return from heartbeat: {}".format(r))

    @wrap_with_logs
    def pull_tasks(self, kill_event):
        """ Pull tasks from the incoming tasks 0mq pipe onto the internal
        pending task queue

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """
        logger.info("[TASK PULL THREAD] starting")
        poller = zmq.Poller()
        poller.register(self.task_incoming, zmq.POLLIN)

        # Send a registration message
        msg = self.create_reg_message()
        logger.debug("Sending registration message: {}".format(msg))
        self.task_incoming.send(msg)
        last_beat = time.time()
        last_interchange_contact = time.time()
        task_recv_counter = 0

        poll_timer = self.poll_period

        while not kill_event.is_set():
            ready_worker_count = self.ready_worker_queue.qsize()
            pending_task_count = self.pending_task_queue.qsize()

            logger.debug("[TASK_PULL_THREAD] ready workers:{}, pending tasks:{}".format(ready_worker_count,
                                                                                        pending_task_count))

            if time.time() > last_beat + self.heartbeat_period:
                self.heartbeat()
                last_beat = time.time()

            if pending_task_count < self.max_queue_size and ready_worker_count > 0:
                logger.debug("[TASK_PULL_THREAD] Requesting tasks: {}".format(ready_worker_count))
                msg = ((ready_worker_count).to_bytes(4, "little"))
                self.task_incoming.send(msg)

            socks = dict(poller.poll(timeout=poll_timer))

            if self.task_incoming in socks and socks[self.task_incoming] == zmq.POLLIN:
                poll_timer = 0
                _, pkl_msg = self.task_incoming.recv_multipart()
                tasks = pickle.loads(pkl_msg)
                last_interchange_contact = time.time()

                if tasks == 'STOP':
                    logger.critical("[TASK_PULL_THREAD] Received stop request")
                    kill_event.set()
                    break

                elif tasks == HEARTBEAT_CODE:
                    logger.debug("Got heartbeat from interchange")

                else:
                    task_recv_counter += len(tasks)
                    logger.debug("[TASK_PULL_THREAD] Got tasks: {} of {}".format([t['task_id'] for t in tasks],
                                                                                 task_recv_counter))

                    for task in tasks:
                        self.pending_task_queue.put(task)
                        # logger.debug("[TASK_PULL_THREAD] Ready tasks: {}".format(
                        #    [i['task_id'] for i in self.pending_task_queue]))

            else:
                logger.debug("[TASK_PULL_THREAD] No incoming tasks")
                # Limit poll duration to heartbeat_period
                # heartbeat_period is in s vs poll_timer in ms
                if not poll_timer:
                    poll_timer = self.poll_period
                poll_timer = min(self.heartbeat_period * 1000, poll_timer * 2)

                # Only check if no messages were received.
                if time.time() > last_interchange_contact + self.heartbeat_threshold:
                    logger.critical("[TASK_PULL_THREAD] Missing contact with interchange beyond heartbeat_threshold")
                    kill_event.set()
                    logger.critical("[TASK_PULL_THREAD] Exiting")
                    break

    @wrap_with_logs
    def push_results(self, kill_event):
        """ Listens on the pending_result_queue and sends out results via 0mq

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """

        logger.debug("[RESULT_PUSH_THREAD] Starting thread")

        push_poll_period = max(10, self.poll_period) / 1000    # push_poll_period must be atleast 10 ms
        logger.debug("[RESULT_PUSH_THREAD] push poll period: {}".format(push_poll_period))

        last_beat = time.time()
        items = []

        while not kill_event.is_set():

            try:
                r = self.pending_result_queue.get(block=True, timeout=push_poll_period)
                items.append(r)
            except queue.Empty:
                pass
            except Exception as e:
                logger.exception("[RESULT_PUSH_THREAD] Got an exception: {}".format(e))

            # If we have reached poll_period duration or timer has expired, we send results
            if len(items) >= self.max_queue_size or time.time() > last_beat + push_poll_period:
                last_beat = time.time()
                if items:
                    self.result_outgoing.send_multipart(items)
                    items = []

        logger.critical("[RESULT_PUSH_THREAD] Exiting")

    @wrap_with_logs
    def worker_watchdog(self, kill_event):
        """ Listens on the pending_result_queue and sends out results via 0mq

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """

        logger.debug("[WORKER_WATCHDOG_THREAD] Starting thread")

        while not kill_event.is_set():
            for worker_id, p in self.procs.items():
                if not p.is_alive():
                    logger.info("[WORKER_WATCHDOG_THREAD] Worker {} has died".format(worker_id))
                    try:
                        task = self._tasks_in_progress.pop(worker_id)
                        logger.info("[WORKER_WATCHDOG_THREAD] Worker {} was busy when it died".format(worker_id))
                        try:
                            raise WorkerLost(worker_id, platform.node())
                        except Exception:
                            logger.info("[WORKER_WATCHDOG_THREAD] Putting exception for task {} in the pending result queue".format(task['task_id']))
                            result_package = {'task_id': task['task_id'], 'exception': serialize(RemoteExceptionWrapper(*sys.exc_info()))}
                            pkl_package = pickle.dumps(result_package)
                            self.pending_result_queue.put(pkl_package)
                    except KeyError:
                        logger.info("[WORKER_WATCHDOG_THREAD] Worker {} was not busy when it died".format(worker_id))

                    p = multiprocessing.Process(target=worker, args=(worker_id,
                                                                     self.uid,
                                                                     self.worker_count,
                                                                     self.pending_task_queue,
                                                                     self.pending_result_queue,
                                                                     self.ready_worker_queue,
                                                                     self._tasks_in_progress,
                                                                     self.cpu_affinity
                                                                 ), name="HTEX-Worker-{}".format(worker_id))
                    self.procs[worker_id] = p
                    logger.info("[WORKER_WATCHDOG_THREAD] Worker {} has been restarted".format(worker_id))
                time.sleep(self.poll_period)

        logger.critical("[WORKER_WATCHDOG_THREAD] Exiting")

    def start(self):
        """ Start the worker processes.

        TODO: Move task receiving to a thread
        """
        start = time.time()
        self._kill_event = threading.Event()
        self._tasks_in_progress = multiprocessing.Manager().dict()

        self.procs = {}
        for worker_id in range(self.worker_count):
            p = multiprocessing.Process(target=worker, args=(worker_id,
                                                             self.uid,
                                                             self.worker_count,
                                                             self.pending_task_queue,
                                                             self.pending_result_queue,
                                                             self.ready_worker_queue,
                                                             self._tasks_in_progress,
                                                             self.cpu_affinity
                                                         ), name="HTEX-Worker-{}".format(worker_id))
            p.start()
            self.procs[worker_id] = p

        logger.debug("Manager synced with workers")

        self._task_puller_thread = threading.Thread(target=self.pull_tasks,
                                                    args=(self._kill_event,),
                                                    name="Task-Puller")
        self._result_pusher_thread = threading.Thread(target=self.push_results,
                                                      args=(self._kill_event,),
                                                      name="Result-Pusher")
        self._worker_watchdog_thread = threading.Thread(target=self.worker_watchdog,
                                                        args=(self._kill_event,),
                                                        name="worker-watchdog")
        self._task_puller_thread.start()
        self._result_pusher_thread.start()
        self._worker_watchdog_thread.start()

        logger.info("Loop start")

        # TODO : Add mechanism in this loop to stop the worker pool
        # This might need a multiprocessing event to signal back.
        self._kill_event.wait()
        logger.critical("[MAIN] Received kill event, terminating worker processes")

        self._task_puller_thread.join()
        self._result_pusher_thread.join()
        self._worker_watchdog_thread.join()
        for proc_id in self.procs:
            self.procs[proc_id].terminate()
            logger.critical("Terminating worker {}: is_alive()={}".format(self.procs[proc_id],
                                                                          self.procs[proc_id].is_alive()))
            self.procs[proc_id].join()
            logger.debug("Worker {} joined successfully".format(self.procs[proc_id]))

        self.task_incoming.close()
        self.result_outgoing.close()
        self.context.term()
        delta = time.time() - start
        logger.info("process_worker_pool ran for {} seconds".format(delta))
        return


def execute_task(bufs):
    """Deserialize the buffer and execute the task.

    Returns the result or throws exception.
    """
    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})

    f, args, kwargs = unpack_apply_message(bufs, user_ns, copy=False)

    # We might need to look into callability of the function from itself
    # since we change it's name in the new namespace
    prefix = "parsl_"
    fname = prefix + "f"
    argname = prefix + "args"
    kwargname = prefix + "kwargs"
    resultname = prefix + "result"

    user_ns.update({fname: f,
                    argname: args,
                    kwargname: kwargs,
                    resultname: resultname})

    code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                           argname, kwargname)
    try:
        # logger.debug("[RUNNER] Executing: {0}".format(code))
        exec(code, user_ns, user_ns)

    except Exception as e:
        logger.warning("Caught exception; will raise it: {}".format(e), exc_info=True)
        raise e

    else:
        # logger.debug("[RUNNER] Result: {0}".format(user_ns.get(resultname)))
        return user_ns.get(resultname)


@wrap_with_logs(target="worker_log")
def worker(worker_id, pool_id, pool_size, task_queue, result_queue, worker_queue, tasks_in_progress, cpu_affinity):
    """

    Put request token into queue
    Get task from task_queue
    Pop request from queue
    Put result into result_queue
    """
    start_file_logger('{}/block-{}/{}/worker_{}.log'.format(args.logdir, args.block_id, pool_id, worker_id),
                      worker_id,
                      name="worker_log",
                      level=logging.DEBUG if args.debug else logging.INFO)

    # Store worker ID as an environment variable
    os.environ['PARSL_WORKER_RANK'] = str(worker_id)
    os.environ['PARSL_WORKER_COUNT'] = str(pool_size)
    os.environ['PARSL_WORKER_POOL_ID'] = str(pool_id)

    # Sync worker with master
    logger.info('Worker {} started'.format(worker_id))
    if args.debug:
        logger.debug("Debug logging enabled")

    # If desired, set process affinity
    if cpu_affinity != "none":
        # Count the number of cores per worker
        avail_cores = sorted(os.sched_getaffinity(0))  # Get the available processors
        cores_per_worker = len(avail_cores) // pool_size
        assert cores_per_worker > 0, "Affinity does not work if there are more workers than cores"

        # Determine this worker's cores
        if cpu_affinity == "block":
            my_cores = avail_cores[cores_per_worker * worker_id:cores_per_worker * (worker_id + 1)]
        elif cpu_affinity == "alternating":
            my_cores = avail_cores[worker_id::pool_size]
        else:
            raise ValueError("Affinity strategy {} is not supported".format(cpu_affinity))

        # Set the affinity for this worker
        os.sched_setaffinity(0, my_cores)
        logger.info("Set worker CPU affinity to {}".format(my_cores))

    while True:
        worker_queue.put(worker_id)

        # The worker will receive {'task_id':<tid>, 'buffer':<buf>}
        req = task_queue.get()
        tasks_in_progress[worker_id] = req
        tid = req['task_id']
        logger.info("Received task {}".format(tid))

        try:
            worker_queue.get()
        except queue.Empty:
            logger.warning("Worker ID: {} failed to remove itself from ready_worker_queue".format(worker_id))
            pass

        try:
            result = execute_task(req['buffer'])
            serialized_result = serialize(result, buffer_threshold=1e6)
        except Exception as e:
            logger.info('Caught an exception: {}'.format(e))
            result_package = {'task_id': tid, 'exception': serialize(RemoteExceptionWrapper(*sys.exc_info()))}
        else:
            result_package = {'task_id': tid, 'result': serialized_result}
            # logger.debug("Result: {}".format(result))

        logger.info("Completed task {}".format(tid))
        try:
            pkl_package = pickle.dumps(result_package)
        except Exception:
            logger.exception("Caught exception while trying to pickle the result package")
            pkl_package = pickle.dumps({'task_id': tid,
                                        'exception': serialize(RemoteExceptionWrapper(*sys.exc_info()))
            })

        result_queue.put(pkl_package)
        tasks_in_progress.pop(worker_id)


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
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d Rank:{0} [%(levelname)s]  %(message)s".format(rank)

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    parser.add_argument("-a", "--addresses", default='',
                        help="Comma separated list of addresses at which the interchange could be reached")
    parser.add_argument("-l", "--logdir", default="process_worker_pool_logs",
                        help="Process worker pool log directory")
    parser.add_argument("-u", "--uid", default=str(uuid.uuid4()).split('-')[-1],
                        help="Unique identifier string for Manager")
    parser.add_argument("-b", "--block_id", default=None,
                        help="Block identifier for Manager")
    parser.add_argument("-c", "--cores_per_worker", default="1.0",
                        help="Number of cores assigned to each worker process. Default=1.0")
    parser.add_argument("-m", "--mem_per_worker", default=0,
                        help="GB of memory assigned to each worker process. Default=0, no assignment")
    parser.add_argument("-t", "--task_port", required=True,
                        help="REQUIRED: Task port for receiving tasks from the interchange")
    parser.add_argument("--max_workers", default=float('inf'),
                        help="Caps the maximum workers that can be launched, default:infinity")
    parser.add_argument("-p", "--prefetch_capacity", default=0,
                        help="Number of tasks that can be prefetched to the manager. Default is 0.")
    parser.add_argument("--hb_period", default=30,
                        help="Heartbeat period in seconds. Uses manager default unless set")
    parser.add_argument("--hb_threshold", default=120,
                        help="Heartbeat threshold in seconds. Uses manager default unless set")
    parser.add_argument("--address_probe_timeout", default=30,
                        help="Timeout to probe for viable address to interchange. Default: 30s")
    parser.add_argument("--poll", default=10,
                        help="Poll period used in milliseconds")
    parser.add_argument("-r", "--result_port", required=True,
                        help="REQUIRED: Result port for posting results to the interchange")
    parser.add_argument("--cpu-affinity", type=str, choices=["none", "block", "alternating"],
                        help="Whether/how workers should control CPU affinity.")

    args = parser.parse_args()

    os.makedirs(os.path.join(args.logdir, "block-{}".format(args.block_id), args.uid), exist_ok=True)

    try:
        start_file_logger('{}/block-{}/{}/manager.log'.format(args.logdir, args.block_id, args.uid),
                          0,
                          level=logging.DEBUG if args.debug is True else logging.INFO)

        logger.info("Python version: {}".format(sys.version))
        logger.info("Debug logging: {}".format(args.debug))
        logger.info("Log dir: {}".format(args.logdir))
        logger.info("Manager ID: {}".format(args.uid))
        logger.info("Block ID: {}".format(args.block_id))
        logger.info("cores_per_worker: {}".format(args.cores_per_worker))
        logger.info("mem_per_worker: {}".format(args.mem_per_worker))
        logger.info("task_port: {}".format(args.task_port))
        logger.info("result_port: {}".format(args.result_port))
        logger.info("addresses: {}".format(args.addresses))
        logger.info("max_workers: {}".format(args.max_workers))
        logger.info("poll_period: {}".format(args.poll))
        logger.info("address_probe_timeout: {}".format(args.address_probe_timeout))
        logger.info("Prefetch capacity: {}".format(args.prefetch_capacity))
        logger.info("Heartbeat threshold: {}".format(args.hb_threshold))
        logger.info("Heartbeat period: {}".format(args.hb_period))
        logger.info("CPU affinity: {}".format(args.cpu_affinity))

        manager = Manager(task_port=args.task_port,
                          result_port=args.result_port,
                          addresses=args.addresses,
                          address_probe_timeout=int(args.address_probe_timeout),
                          uid=args.uid,
                          block_id=args.block_id,
                          cores_per_worker=float(args.cores_per_worker),
                          mem_per_worker=None if args.mem_per_worker == 'None' else float(args.mem_per_worker),
                          max_workers=args.max_workers if args.max_workers == float('inf') else int(args.max_workers),
                          prefetch_capacity=int(args.prefetch_capacity),
                          heartbeat_threshold=int(args.hb_threshold),
                          heartbeat_period=int(args.hb_period),
                          poll_period=int(args.poll),
                          cpu_affinity=args.cpu_affinity)
        manager.start()

    except Exception:
        logger.critical("Process worker pool exiting with an exception", exc_info=True)
        raise
    else:
        logger.info("Process worker pool exiting normally")
        print("Process worker pool exiting normally")
