"""LowLatencyExecutor for low latency task/lambda-function execution
"""

from concurrent.futures import Future
import logging
import threading
import queue
from multiprocessing import Process, Queue

from parsl.serialize import pack_apply_message, deserialize
from parsl.executors.low_latency import zmq_pipes
from parsl.executors.low_latency import interchange
from parsl.executors.errors import ScalingFailed, DeserializationError, BadMessage, UnsupportedFeatureError
from parsl.executors.status_handling import StatusHandlingExecutor
from parsl.utils import RepresentationMixin
from parsl.providers import LocalProvider

logger = logging.getLogger(__name__)


class LowLatencyExecutor(StatusHandlingExecutor, RepresentationMixin):
    """
    TODO: docstring for LowLatencyExecutor
    """

    def __init__(self,
                 label='LowLatencyExecutor',
                 provider=LocalProvider(),
                 launch_cmd=None,
                 address="127.0.0.1",
                 worker_port=None,
                 worker_port_range=(54000, 55000),
                 interchange_port_range=(55000, 56000),
                 #  storage_access=None,
                 working_dir=None,
                 worker_debug=False,
                 workers_per_node=1,
                 #  cores_per_worker=1.0,
                 managed=True
                 ):
        logger.debug("Initializing LowLatencyExecutor")

        StatusHandlingExecutor.__init__(self, provider)
        self.label = label
        self.launch_cmd = launch_cmd
        self.provider = provider
        self.worker_debug = worker_debug
        # self.storage_access = storage_access if storage_access is not None else []
        # if len(self.storage_access) > 1:
        # raise ConfigurationError('Multiple storage access schemes are not supported')
        self.working_dir = working_dir
        self.managed = managed
        self.blocks = []
        self.workers_per_node = workers_per_node

        self._task_counter = 0
        self.address = address
        self.worker_port = worker_port
        self.worker_port_range = worker_port_range
        self.interchange_port_range = interchange_port_range
        self.run_dir = '.'

        # TODO: add debugging, logdir, other functionality to workers
        if not launch_cmd:
            self.launch_cmd = """lowlatency_worker.py -n {workers_per_node} --task_url={task_url} --logdir={logdir}"""

    def start(self):
        """Create the Interchange process and connect to it.
        """
        self.outgoing_q = zmq_pipes.TasksOutgoing(
            "127.0.0.1", self.interchange_port_range)
        self.incoming_q = zmq_pipes.ResultsIncoming(
            "127.0.0.1", self.interchange_port_range)

        self.is_alive = True

        self._queue_management_thread = None
        self._start_queue_management_thread()
        self._start_local_queue_process()

        logger.debug("Created management thread: {}"
                     .format(self._queue_management_thread))

        if self.provider:
            # debug_opts = "--debug" if self.worker_debug else ""
            l_cmd = self.launch_cmd.format(  # debug=debug_opts,
                task_url=self.worker_task_url,
                workers_per_node=self.workers_per_node,
                logdir="{}/{}".format(self.run_dir, self.label))
            self.launch_cmd = l_cmd
            logger.debug("Launch command: {}".format(self.launch_cmd))

            self._scaling_enabled = True
            logger.debug(
                "Starting LowLatencyExecutor with provider:\n%s", self.provider)
            if hasattr(self.provider, 'init_blocks'):
                try:
                    for i in range(self.provider.init_blocks):
                        block = self.provider.submit(
                            self.launch_cmd, self.workers_per_node)
                        logger.debug("Launched block {}:{}".format(i, block))
                        if not block:
                            raise(ScalingFailed(self.provider.label,
                                                "Attempts to provision nodes via provider has failed"))
                        self.blocks.extend([block])

                except Exception as e:
                    logger.error("Scaling out failed: {}".format(e))
                    raise e
        else:
            self._scaling_enabled = False
            logger.debug("Starting LowLatencyExecutor with no provider")

    def _start_local_queue_process(self):
        """ TODO: docstring """

        comm_q = Queue(maxsize=10)
        self.queue_proc = Process(target=interchange.starter,
                                  args=(comm_q,),
                                  kwargs={"client_ports": (self.outgoing_q.port,
                                                           self.incoming_q.port),
                                          "worker_port": self.worker_port,
                                          "worker_port_range": self.worker_port_range
                                          # TODO: logdir and logging level
                                          })
        self.queue_proc.start()

        try:
            worker_port = comm_q.get(block=True, timeout=120)
            logger.debug(
                "Got worker port {} from interchange".format(worker_port))
        except queue.Empty:
            logger.error(
                "Interchange has not completed initialization in 120s. Aborting")
            raise Exception("Interchange failed to start")

        self.worker_task_url = "tcp://{}:{}".format(
            self.address, worker_port)

    def _start_queue_management_thread(self):
        """ TODO: docstring """
        if self._queue_management_thread is None:
            logger.debug("Starting queue management thread")
            self._queue_management_thread = threading.Thread(
                target=self._queue_management_worker)
            self._queue_management_thread.daemon = True
            self._queue_management_thread.start()
            logger.debug("Started queue management thread")

        else:
            logger.debug("Management thread already exists, returning")

    def _queue_management_worker(self):
        """ TODO: docstring """
        logger.debug("[MTHREAD] queue management worker starting")

        while not self.bad_state_is_set:
            task_id, buf = self.incoming_q.get()  # TODO: why does this hang?
            msg = deserialize(buf)[0]
            # TODO: handle exceptions
            task_fut = self.tasks[task_id]
            logger.debug("Got response for task id {}".format(task_id))

            if "result" in msg:
                task_fut.set_result(msg["result"])

            elif "exception" in msg:
                # TODO: handle exception
                pass
            elif 'exception' in msg:
                logger.warning("Task: {} has returned with an exception")
                try:
                    s = deserialize(msg['exception'])
                    exception = ValueError("Remote exception description: {}".format(s))
                    task_fut.set_exception(exception)
                except Exception as e:
                    # TODO could be a proper wrapped exception?
                    task_fut.set_exception(
                        DeserializationError("Received exception, but handling also threw an exception: {}".format(e)))

            else:
                raise BadMessage(
                    "Message received is neither result nor exception")

            if not self.is_alive:
                break

        logger.info("[MTHREAD] queue management worker finished")

    def submit(self, func, resource_specification, *args, **kwargs):
        """ TODO: docstring """
        if resource_specification:
            logger.error("Ignoring the resource specification. "
                         "Parsl resource specification is not supported in LowLatency Executor. "
                         "Please check WorkQueueExecutor if resource specification is needed.")
            raise UnsupportedFeatureError('resource specification', 'LowLatency Executor', 'WorkQueue Executor')

        if self.bad_state_is_set:
            raise self.executor_exception

        self._task_counter += 1
        task_id = self._task_counter

        logger.debug(
            "Pushing function {} to queue with args {}".format(func, args))

        self.tasks[task_id] = Future()

        fn_buf = pack_apply_message(func, args, kwargs,
                                    buffer_threshold=1024 * 1024)

        # Post task to the the outgoing queue
        self.outgoing_q.put(task_id, fn_buf)

        # Return the future
        return self.tasks[task_id]

    @property
    def scaling_enabled(self):
        return self._scaling_enabled

    def scale_out(self, blocks=1):
        """Scales out the number of active workers by the number of blocks specified.

        Parameters
        ----------

        blocks : int
             # of blocks to scale out. Default=1
        """
        r = []
        for i in range(blocks):
            if self.provider:
                try:
                    block = self.provider.submit(
                        self.launch_cmd, self.workers_per_node)
                    logger.debug("Launched block {}:{}".format(i, block))
                    # TODO: use exceptions for this
                    if not block:
                        self._fail_job_async(None, "Failed to launch block")
                    self.blocks.extend([block])
                except Exception as ex:
                    self._fail_job_async(None, "Failed to launch block: {}".format(ex))
            else:
                logger.error("No execution provider available")
                r = None
        return r

    def scale_in(self, blocks):
        """Scale in the number of active blocks by specified amount.

        The scale in method here is very rude. It doesn't give the workers
        the opportunity to finish current tasks or cleanup. This is tracked
        in issue #530
        """
        to_kill = self.blocks[:blocks]
        if self.provider:
            r = self.provider.cancel(to_kill)
        return self._filter_scale_in_ids(to_kill, r)

    def _get_job_ids(self):
        return self.blocks

    def shutdown(self, hub=True, targets='all', block=False):
        """Shutdown the executor, including all workers and controllers.

        This is not implemented.

        Kwargs:
            - hub (Bool): Whether the hub should be shutdown, Default:True,
            - targets (list of ints| 'all'): List of block id's to kill, Default:'all'
            - block (Bool): To block for confirmations or not
        """

        logger.warning("Attempting LowLatencyExecutor shutdown")
        # self.outgoing_q.close()
        # self.incoming_q.close()
        self.queue_proc.terminate()
        logger.warning("Finished LowLatencyExecutor shutdown attempt")
        return True
