import threading
import logging
from collections import deque
from concurrent.futures import Future

import os
import pickle

from ipyparallel.serialize import pack_apply_message

from parsl.executors.errors import *
from parsl.executors.base import ParslExecutor

from work_queue import *

logger = logging.getLogger(__name__)


def WorkQueueThread(tasks={},
                    task_queue=deque,
                    queue_lock=threading.Lock(),
                    tasks_lock=threading.Lock(),
                    port=50000,
                    launch_cmd=None,
                    data_dir=".",
                    log_dir=None,
                    full=False,
                    password=None,
                    password_file=None,
                    project_name=None,
                    env=None):

    logger.debug("Starting WorkQueue Master Thread")

    wq_to_parsl = {}
    if log_dir is not None:
        wq_debug_log = os.path.join(log_dir, "debug")
        cctools_debug_flags_set("all")
        cctools_debug_config_file(wq_debug_log)

    logger.debug("Creating Workqueue Object")
    try:
        q = WorkQueue(port)
    except Exception as e:
        logger.error("Unable to create Workqueue object: {}", format(e))
        raise e

    if project_name:
        q.specify_name(project_name)

    if password:
        q.specify_password(password)
    elif password_file:
        q.specify_password_file(password_file)

    # Only write Logs when the log_dir is specified, which is most likely always will be
    if log_dir is not None:
        wq_master_log = os.path.join(log_dir, "master_log")
        wq_trans_log = os.path.join(log_dir, "transaction_log")
        if not full:
            wq_resource_log = os.path.join(log_dir, "resource_logs")
            q.enable_monitoring_full(dirname=wq_resource_log)

        q.specify_log(wq_master_log)
        q.specify_transactions_log(wq_trans_log)

    continue_running = True
    while(continue_running):
        # Monitor the Task Queue

        place_holder_queue = []

        queue_lock.acquire()
        queue_len = len(task_queue)
        while queue_len > 0:
            place_holder_queue.append(task_queue.pop())
            queue_len -= 1
        queue_lock.release()

        # Submit Tasks
        for item in place_holder_queue:
            parsl_id = item["task_id"]
            if parsl_id < 0:
                continue_running = False
                break
            function_data_loc = item["data_loc"]
            function_result_loc = item["result_loc"]
            function_result_loc_remote = function_result_loc.split("/")[-1]
            function_data_loc_remote = function_data_loc.split("/")[-1]

            # TODO Make this general
            full_script_name = "/afs/crc.nd.edu/user/a/alitteke/parsl/parsl/executors/workqueue/workqueue_worker.py"

            script_name = full_script_name.split("/")[-1]
            sandbox_func_data = "$WORK_QUEUE_SANDBOX/" + function_data_loc_remote
            sandbox_func_result = "$WORK_QUEUE_SANDBOX/" + function_result_loc_remote
            command_str = launch_cmd.format(input_file=sandbox_func_data,
                                            output_file=sandbox_func_result)
            command_str = launch_cmd.format(input_file=function_data_loc_remote,
                                            output_file=function_result_loc_remote)
            logger.debug("Sending task {} with command: {}".format(parsl_id, command_str))
            try:
                t = Task(command_str)
            except Exception as e:
                logger.error("Unable to create task: {}".format(e))
                raise e
            if env is not None:
                for var in env:
                    t.specify_environment_variable(var, env[var])

            t.specify_file(full_script_name, script_name, WORK_QUEUE_INPUT, cache=True)
            t.specify_file(function_result_loc, function_result_loc_remote, WORK_QUEUE_OUTPUT, cache=False)
            t.specify_file(function_data_loc, function_data_loc_remote, WORK_QUEUE_INPUT, cache=False)

            logger.debug("Submitting task {} to workqueue".format(parsl_id))
            try:
                wq_id = q.submit(t)
            except Exception as e:
                logger.error("Unable to create task: {}".format(e))
                raise e

            logger.debug("Task {} submitted workqueue with id {}".format(parsl_id, wq_id))
            wq_to_parsl[wq_id] = parsl_id

        if continue_running is False:
            logger.debug("Exiting WorkQueue Master Thread event loop")
            break

        # Wait for Tasks
        task_found = True
        while task_found:
            t = q.wait(5)
            if t is None:
                task_found = False
            else:
                # print(t.id)
                wq_tid = t.id
                parsl_tid = wq_to_parsl[wq_tid]
                logger.debug("Completed workqueue task {}, parsl task {}".format(wq_id, parsl_tid))
                status = t.return_status
                # TODO output sdtout and stderr to files as well as submit command
                if status != 0:
                    logger.debug("Workqueue task {} failed with status {}".format(wq_id, status))
                    print(t.output)
                    # Should probably do something smarter for this later
                    del wq_to_parsl[wq_tid]
                    continue
                result_loc = os.path.join(data_dir, "task_" + str(parsl_tid) + "_function_result")
                logger.debug("Looking for result in {}".format(result_loc))
                f = open(result_loc, "rb")
                result = pickle.load(f)
                f.close()

                tasks_lock.acquire()
                future = tasks[parsl_tid]
                tasks_lock.release()

                logger.debug("Updating Future for Parsl Task {}".format(parsl_tid))
                future.set_result(result)
                del wq_to_parsl[wq_tid]

    for wq_task in wq_to_parsl:
        logger.debug("Cancelling Workqueue Task {}".format(wq_task))
        q.cancel_by_taskid(wq_task)

    logger.debug("Exiting WorkQueue Master Thread")
    return


class WorkQueueExecutor(ParslExecutor):
    """Define the strict interface for all Executor classes.

    This is a metaclass that only enforces concrete implementations of
    functionality by the child classes.

    In addition to the listed methods, a ParslExecutor instance must always
    have a member field:

       label: str - a human readable label for the executor, unique
              with respect to other executors.

    """

    def __init__(self,
                 label="wq",
                 working_dir=None,
                 managed=True,
                 project_name=None,
                 project_password=None,
                 project_password_file=None,
                 port=WORK_QUEUE_DEFAULT_PORT,
                 env=None,
                 init_command=""):

        self.label = label
        self.managed = managed
        self.task_queue = deque()
        self.tasks = {}
        self.port = port
        self.task_counter = 0
        self.scaling_enabled = False
        self.project_name = project_name
        self.project_password = project_password
        self.project_password_file = project_password_file
        self.env = env
        self.init_command = init_command

        if self.project_password is not None and self.project_password_file is not None:
            logger.debug("Password File and Password text specified for WorkQueue Executor, only Password Text will be used")
            self.project_password_file = None
        if self.project_password_file is not None:
            if os.path.exists(self.project_password_file) is False:
                logger.debug("Password File does not exist, no file used")
                self.project_password_file = None

        self.launch_cmd = ("python3 workqueue_worker.py {input_file} {output_file}")
        if self.init_command != "":
            self.launch_cmd = self.init_command + "; " + self.launch_cmd

    def start(self):
        self.queue_lock = threading.Lock()
        self.tasks_lock = threading.Lock()

        self.function_data_dir = os.path.join(self.run_dir, "function_data")
        self.wq_log_dir = os.path.join(self.run_dir, self.label)

        os.mkdir(self.function_data_dir)
        os.mkdir(self.wq_log_dir)

        logger.debug("Starting WorkQueueExectutor")

        thread_kwargs = {"port": self.port,
                         "tasks": self.tasks,
                         "task_queue": self.task_queue,
                         "queue_lock": self.queue_lock,
                         "tasks_lock": self.tasks_lock,
                         "launch_cmd": self.launch_cmd,
                         "data_dir": self.function_data_dir,
                         "log_dir": self.wq_log_dir,
                         "password_file": self.project_password_file,
                         "password": self.project_password,
                         "project_name": self.project_name,
                         "env": self.env}
        self.master_thread = threading.Thread(target=WorkQueueThread,
                                              name="master_thread",
                                              kwargs=thread_kwargs)
        self.master_thread.daemon = True
        self.master_thread.start()

    def submit(self, func, *args, **kwargs):
        """Submit.

        We haven't yet decided on what the args to this can be,
        whether it should just be func, args, kwargs or be the partially evaluated
        fn
        """
        self.task_counter += 1
        task_id = self.task_counter

        fu = Future()
        self.tasks_lock.acquire()
        self.tasks[task_id] = fu
        self.tasks_lock.release()

        logger.debug("Creating task {} for function {} with args {}".format(task_id, func, args))

        # Pickle the result into object to pass into message buffer
        # TODO Try/Except Block
        function_data_file = os.path.join(self.function_data_dir, "task_" + str(task_id) + "_function_data")
        function_result_file = os.path.join(self.function_data_dir, "task_" + str(task_id) + "_function_result")

        logger.debug("Creating Task {} with executable at: {}".format(task_id, function_data_file))
        logger.debug("Creating Tasks {} with result to be found at: {}".format(task_id, function_result_file))

        f = open(function_data_file, "wb")
        fn_buf = pack_apply_message(func, args, kwargs,
                                    buffer_threshold=1024 * 1024,
                                    item_threshold=1024)
        pickle.dump(fn_buf, f)
        f.close

        logger.debug("Placing task {} on message queue".format(task_id))
        msg = {"task_id": task_id,
               "data_loc": function_data_file,
               "result_loc": function_result_file}

        self.queue_lock.acquire()
        self.task_queue.appendleft(msg)
        self.queue_lock.release()

        return fu

    def scale_out(self, *args, **kwargs):
        """Scale out method.

        We should have the scale out method simply take resource object
        which will have the scaling methods, scale_out itself should be a coroutine, since
        scaling tasks can be slow.
        """
        pass

    def scale_in(self, count):
        """Scale in method.

        Cause the executor to reduce the number of blocks by count.

        We should have the scale in method simply take resource object
        which will have the scaling methods, scale_in itself should be a coroutine, since
        scaling tasks can be slow.
        """
        pass

    def shutdown(self, *args, **kwargs):
        """Shutdown the executor.

        This includes all attached resources such as workers and controllers.
        """
        # print("shutdown")
        logger.debug("Placing sentinel task on message queue")
        msg = {"task_id": -1,
               "data_loc": None,
               "result_loc": None}

        self.queue_lock.acquire()
        self.task_queue.append(msg)
        self.queue_lock.release()

        self.master_thread.join()

        return True

    def scaling_enabled(self):
        """Specify if scaling is enabled.

        The callers of ParslExecutors need to differentiate between Executors
        and Executors wrapped in a resource provider
        """
        return False

    def run_dir(self):
        """Path to the run directory.
        """
        return self._run_dir

    def run_dir(self, value):
        self._run_dir = value
