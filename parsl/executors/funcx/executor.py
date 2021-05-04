import threading
import queue
import time
import logging
import random
import os
import inspect
from concurrent.futures import Future
from parsl.utils import RepresentationMixin
from parsl.executors.status_handling import NoStatusHandlingExecutor
from parsl.executors.errors import (
    BadMessage, ScalingFailed,
    DeserializationError, SerializationError,
    UnsupportedFeatureError
)
from parsl.app.errors import RemoteExceptionWrapper

from funcx.sdk.client import FuncXClient

logger = logging.getLogger(__name__)


class FuncXExecutor(NoStatusHandlingExecutor, RepresentationMixin):

    def __init__(self,
                 label="FuncXExecutor",
                 # provider=None,
                 endpoints=None,
                 workdir='FXEX',
                 batch_interval=1,
                 batch_size=20,
                 poll_interval=5,
                 managed=True):

        logger.info("Initializing FuncXExecutor")
        self.label = label
        self.endpoints = endpoints
        self.batch_interval = batch_interval
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.workdir = os.path.abspath(workdir)
        self.managed = managed

        self._scaling_enabled = False

        self._task_counter = 0
        self._tasks = {}

    def start(self):
        """ Called when DFK starts the executor when the config is loaded
        1. Make workdir
        2. Start task_status_poller
        3. Create a funcx SDK client
        """
        os.makedirs(self.workdir, exist_ok=True)
        self.task_outgoing = queue.Queue()
        # Create a funcx SDK client
        self.fxc = FuncXClient()

        # Create a dict to record the function uuids for apps
        self.functions = {}
        # Record the mapping between funcX function uuids and internal task ids
        self.task_uuids = {}

        self._kill_event = threading.Event()
        # Start the task submission thread
        self.task_submit_thread = threading.Thread(target=self.task_submit_thread,
                                                   args=(self._kill_event,))
        self.task_submit_thread.daemon = True
        self.task_submit_thread.start()
        logger.info("Started task submit thread")

        # Start the task status poller thread
        self.task_poller_thread = threading.Thread(target=self.task_status_poller,
                                                   args=(self._kill_event,))
        self.task_poller_thread.daemon = True
        self.task_poller_thread.start()
        logger.info("Started task status poller thread")

        return []

    def submit(self, func, resource_specification, *args, **kwargs):
        """Submits work to the the task_outgoing queue.
        The task_outgoing queue is an external process listens on this
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
                         "Parsl resource specification is not supported in FuncXExecutor. "
                         "Please check WorkQueueExecutor if resource specification is needed.")
            raise UnsupportedFeatureError('resource specification', 'FuncXExecutor', 'WorkQueue Executor')

        self._task_counter += 1
        task_id = self._task_counter

        logger.info("Submitting function {} with args {} and kwargs {}".format(func.__dict__, args, kwargs))
        logger.info("Source code: {}".format(inspect.getsource(func)))

        # The code 

        if func not in self.functions:
            try:
                func_uuid = self.fxc.register_function(func,
                                                       description="Parsl app {}".format(func.__name__))
                logger.info("Registered Parsl app {} as funcX function uuid {}".format(func.__name__,
                                                                                       func_uuid))
            except Exception:
                logger.error("Error in registering Parsl app {}".format(func.__name__))
                raise Exception("Error in registering Parsl app {}".format(func.__name__))
            else:
                self.functions[func] = func_uuid

        func_uuid = self.functions[func]

        # handle people sending blobs gracefully
        args_to_print = args
        if logger.getEffectiveLevel() >= logging.DEBUG:
            args_to_print = tuple([arg if len(repr(arg)) < 100 else (repr(arg)[:100] + '...') for arg in args])
        logger.debug("Pushing function {} to queue with args {}".format(func, args_to_print))

        self.tasks[task_id] = Future()

        msg = {"task_id": task_id,
               "func_uuid": func_uuid,
               "args": args,
               "kwargs": kwargs}

        # Post task to the the outgoing queue
        self.task_outgoing.put(msg)

        # Return the future
        return self.tasks[task_id]

    def task_submit_thread(self, kill_event):
        """Task submission thread that fetch tasks from task_outgoing queue,
        batch function requests, and submit functions to funcX"""
        while not kill_event.is_set():
            messages = self._get_tasks_in_batch()
            if messages:
                logger.info("[TASK_SUBMIT_THREAD] Submitting {} tasks to funcX".format(len(messages)))
            self._schedule_tasks(messages)
        logger.info("[TASK_SUBMIT_THREAD] Exiting")

    def _schedule_tasks(self, messages):
        """Schedule a batch of tasks to different funcx endpoints
        This is a naive task scheduler, which submit a task to a random endpoint
        """
        if messages:
            batch = self.fxc.create_batch()
            for msg in messages:
                # random endpoint selection for v1
                endpoint = random.choice(self.endpoints)
                func_uuid, args, kwargs = msg['func_uuid'], msg['args'], msg['kwargs']
                batch.add(*args, *kwargs,
                          endpoint_id=endpoint,
                          function_id=func_uuid)
                logger.debug("[TASK_SUBMIT_THREAD] Adding msg {} to funcX batch".format(msg))
            try:
                batch_tasks = self.fxc.batch_run(batch)
            except Exception:
                logger.error("[TASK_SUBMIT_THREAD] Error submitting {} tasks to funcX".format(len(messages)))
            else:
                for i, msg in enumerate(messages):
                    self.task_uuids[batch_tasks[i]] = msg['task_id']

    def _get_tasks_in_batch(self):
        """Get tasks from task_outgoing queue in batch, either by interval or by batch size"""
        messages = []
        start = time.time()
        while True:
            if time.time() - start >= self.batch_size or len(messages) >= self.batch_interval:
                break
            try:
                x = self.task_outgoing.get(timeout=0.1)
            except queue.Empty:
                break
            else:
                messages.append(x)
        return messages

    def task_status_poller(self, kill_event):
        """Task status poller thread that keeps polling the status of existing tasks"""
        while not kill_event.is_set():
            if self.tasks and self.task_uuids:
                to_poll_tasks = list(self.task_uuids.keys())
                logger.debug("[TASK_POLLER_THREAD] To poll tasks {}".format(to_poll_tasks))
                try:
                    # TODO: using prod funcx now, need to update to get_batch_result for results
                    batch_results = self.fxc.get_batch_status(to_poll_tasks)
                except Exception:
                    logger.error("[TASK_POLLER_THREAD] Exception in poll tasks in batch")
                else:
                    logger.debug("[TASK_POLLER_THREAD] Got batch results {}".format(batch_results))
                    for tid in batch_results:
                        msg = batch_results[tid]
                        if not msg['pending']:
                            internal_task_id = self.task_uuids.pop(tid)
                            task_fut = self.tasks.pop(internal_task_id)
                            logger.debug("[TASK_POLLER_THREAD] Processing message " 
                                         "{} for task {}".format(msg, internal_task_id))

                            if 'result' in msg:
                                result = msg['result']
                                task_fut.set_result(result)

                            elif 'exception' in msg:
                                try:
                                    s = msg['exception']
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

            time.sleep(self.poll_interval)

        logger.info("[TASK_POLLER_THREAD] Exiting")

    def shutdown(self):
        self._kill_event.set()
        logger.info("FuncXExecutor shutdown")
        return True

    def scale_in(self):
        pass

    def scale_out(self):
        pass

    @property
    def scaling_enabled(self):
        return self._scaling_enabled
