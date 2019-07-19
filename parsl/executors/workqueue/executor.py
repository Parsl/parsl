import threading
import multiprocessing
import logging
from concurrent.futures import Future

import os
import pickle
import queue

from ipyparallel.serialize import pack_apply_message, deserialize_object

from parsl.app.errors import AppFailure
from parsl.app.errors import RemoteExceptionWrapper
from parsl.executors.errors import ExecutorError
from parsl.executors.base import ParslExecutor
from parsl.data_provider.files import File
from parsl.executors.workqueue import workqueue_worker

WORK_QUEUE_DEFAULT_PORT = -1
WORK_QUEUE_RESULT_SUCCESS = 0

from work_queue import WorkQueue
from work_queue import Task
from work_queue import WORK_QUEUE_DEFAULT_PORT
from work_queue import WORK_QUEUE_INPUT
from work_queue import WORK_QUEUE_OUTPUT, 

logger = logging.getLogger(__name__)


def WorkQueueSubmitThread(task_queue=multiprocessing.Queue(),
                          queue_lock=threading.Lock(),
                          launch_cmd=None,
                          env=None,
                          collector_queue=multiprocessing.Queue(),
                          see_worker_output=False,
                          data_dir=".",
                          full=False,
                          cancel_value=multiprocessing.Value('i', 1),
                          port=WORK_QUEUE_DEFAULT_PORT,
                          wq_log_dir=None,
                          project_password=None,
                          project_password_file=None,
                          project_name=None):

    logger.debug("Starting WorkQueue Submit/Wait Process")

    orig_ppid = os.getppid()

    wq_tasks = set()

    continue_running = True

    if wq_log_dir is not None:
        wq_debug_log = os.path.join(wq_log_dir, "debug")
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

    if project_password:
        q.specify_password(project_password)
    elif project_password_file:
        q.specify_password_file(project_password_file)

    # Only write Logs when the log_dir is specified, which is most likely always will be
    if wq_log_dir is not None:
        wq_master_log = os.path.join(wq_log_dir, "master_log")
        wq_trans_log = os.path.join(wq_log_dir, "transaction_log")
        if full:
            wq_resource_log = os.path.join(wq_log_dir, "resource_logs")
            q.enable_monitoring_full(dirname=wq_resource_log)

        q.specify_log(wq_master_log)
        q.specify_transactions_log(wq_trans_log)

    while(continue_running):
        # Monitor the Task Queue
        ppid = os.getppid()
        if ppid != orig_ppid:
            continue_running = False
            continue

        # Submit Tasks
        while task_queue.qsize() > 0:
            if cancel_value.value == 0:
                continue_running = False
                break

            try:
                # item = task_queue.get_nowait()
                item = task_queue.get(timeout=1)
                logger.debug("Removing task from queue")
            except queue.Empty:
                continue
            parsl_id = item["task_id"]

            function_data_loc = item["data_loc"]
            function_result_loc = item["result_loc"]
            function_result_loc_remote = function_result_loc.split("/")[-1]
            function_data_loc_remote = function_data_loc.split("/")[-1]

            input_files = item["input_files"]
            output_files = item["output_files"]
            std_files = item["std_files"]

            full_script_name = workqueue_worker.__file__
            script_name = full_script_name.split("/")[-1]

            remapping_string = ""

            std_string = ""
            logger.debug("looking at input")
            for item in input_files:
                if item[3] == "std":
                    std_string += "mv " + item[1] + " " + item[0] + "; "
                else:
                    remapping_string += item[0] + ":" + item[1] + ","
            logger.debug(remapping_string)

            logger.debug("looking at output")
            for item in output_files:
                remapping_string += item[0] + ":" + item[1] + ","
            logger.debug(remapping_string)

            if len(input_files) + len(output_files) > 0:
                remapping_string = "-r " + remapping_string
                remapping_string = remapping_string[:-1]

            logger.debug(launch_cmd)
            command_str = launch_cmd.format(input_file=function_data_loc_remote,
                                            output_file=function_result_loc_remote,
                                            remapping_string=remapping_string)

            logger.debug(command_str)
            command_str = std_string + command_str
            logger.debug(command_str)

            logger.debug("Sending task {} with command: {}".format(parsl_id, command_str))
            try:
                t = Task(command_str)
            except Exception as e:
                logger.error("Unable to create task: {}".format(e))
                continue
            if env is not None:
                for var in env:
                    t.specify_environment_variable(var, self.env[var])

            t.specify_file(full_script_name, script_name, WORK_QUEUE_INPUT, cache=True)
            t.specify_file(function_result_loc, function_result_loc_remote, WORK_QUEUE_OUTPUT, cache=False)
            t.specify_file(function_data_loc, function_data_loc_remote, WORK_QUEUE_INPUT, cache=False)
            t.specify_tag(str(parsl_id))

            for item in input_files:
                t.specify_file(item[0], item[1], WORK_QUEUE_INPUT, cache=item[2])

            for item in output_files:
                t.specify_file(item[0], item[1], WORK_QUEUE_OUTPUT, cache=item[2])

            for item in std_files:
                t.specify_file(item[0], item[1], WORK_QUEUE_OUTPUT, cache=item[2])

            logger.debug("Submitting task {} to workqueue".format(parsl_id))
            try:
                wq_id = q.submit(t)
                wq_tasks.add(wq_id)
            except Exception as e:
                logger.error("Unable to create task: {}".format(e))

                msg = {"tid": parsl_tid,
                       "result_recieved": False,
                       "reason": "Workqueue Task Start Failure",
                       "status": 1}

                collector_queue.put_nowait(msg)
                continue

            logger.debug("Task {} submitted workqueue with id {}".format(parsl_id, wq_id))

        if cancel_value.value == 0:
            continue_running = False

        # Wait for Tasks
        task_found = True
        # If the queue is not empty wait on the workqueue queue for a task
        if not q.empty() and continue_running:
            while task_found is True:
                if cancel_value.value == 0:
                    continue_running = False
                    task_found = False
                    continue
                t = q.wait(1)
                if t is None:
                    task_found = False
                    continue
                else:
                    parsl_tid = t.tag
                    logger.debug("Completed workqueue task {}, parsl task {}".format(t.id, parsl_tid))
                    status = t.return_status
                    task_result = t.result
                    msg = None

                    if status != 0 or (task_result != WORK_QUEUE_RESULT_SUCCESS and task_result != WORK_QUEUE_RESULT_OUTPUT_MISSING):
                        if task_result == WORK_QUEUE_RESULT_SUCCESS:
                            logger.debug("Workqueue task {} failed with status {}".format(t.id, status))

                            reason = "Wrapper Script Failure: "
                            if status == 1:
                                reason += "command line parsing"
                            if status == 2:
                                reason += "problem loading function data"
                            if status == 3:
                                reason += "problem remapping file names"
                            if status == 4:
                                reason += "problem writing out function result"

                            reason += "\nTrace:\n" + t.output

                            logger.debug("Workqueue runner script failed for task {} because {}\n".format(parsl_tid, reason))

                        else:
                            reason = "Workqueue system failure\n"

                        msg = {"tid": parsl_tid,
                               "result_recieved": False,
                               "reason": reason,
                               "status": status}

                        collector_queue.put_nowait(msg)

                    else:

                        if see_worker_output:
                            print(t.output)

                        result_loc = os.path.join(data_dir, "task_" + str(parsl_tid) + "_function_result")
                        logger.debug("Looking for result in {}".format(result_loc))
                        f = open(result_loc, "rb")
                        result = pickle.load(f)
                        f.close()

                        msg = {"tid": parsl_tid,
                               "result_recieved": True,
                               "result": result}
                        wq_tasks.remove(t.id)

                    collector_queue.put_nowait(msg)

        if continue_running is False:
            logger.debug("Exiting WorkQueue Master Thread event loop")
            break

    for wq_task in wq_tasks:
        logger.debug("Cancelling Workqueue Task {}".format(wq_task))
        q.cancel_by_taskid(wq_task)

    logger.debug("Exiting WorkQueue Monitoring Process")
    return 0


def WorkQueueCollectorThread(collector_queue=multiprocessing.Queue(),
                             tasks={},
                             tasks_lock=threading.Lock(),
                             cancel_value=multiprocessing.Value('i', 1),
                             submit_process=None,
                             executor=None):

    logger.debug("Starting Collector Thread")

    continue_running = True
    while continue_running:
        if cancel_value.value == 0:
            continue_running = False
            continue

        if not submit_process.is_alive() and cancel_value.value != 0:
            raise ExecutorError(executor, "Workqueue Submit Process is not alive")

        try:
            item = collector_queue.get(timeout=1)
        except queue.Empty:
            continue

        parsl_tid = item["tid"]
        recieved = item["result_recieved"]

        tasks_lock.acquire()
        future = tasks[parsl_tid]
        tasks_lock.release()

        if recieved is False:
            reason = item["reason"]
            status = item["status"]
            future.set_exception(AppFailure(reason, status))
        else:
            result = item["result"]
            future_update, _ = deserialize_object(result["result"])
            logger.debug("Updating Future for Parsl Task {}".format(parsl_tid))
            if result["failure"] is False:
                future.set_result(future_update)
            else:
                future.set_exception(RemoteExceptionWrapper(*future_update))

    logger.debug("Exiting Collector Thread")
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
                 working_dir=".",
                 managed=True,
                 project_name=None,
                 project_password=None,
                 project_password_file=None,
                 port=WORK_QUEUE_DEFAULT_PORT,
                 env=None,
                 shared_fs=False,
                 init_command="",
                 see_worker_output=False):

        self.label = label
        self.managed = managed
        self.task_queue = multiprocessing.Queue()
        self.collector_queue = multiprocessing.Queue()
        self.tasks = {}
        self.port = port
        self.task_counter = -1
        self.scaling_enabled = False
        self.project_name = project_name
        self.project_password = project_password
        self.project_password_file = project_password_file
        self.env = env
        self.init_command = init_command
        self.shared_fs = shared_fs
        self.working_dir = working_dir
        self.used_names = {}
        self.shared_files = set()
        self.registered_files = set()
        self.worker_output = see_worker_output
        self.full = True
        self.cancel_value = multiprocessing.Value('i', 1)

        if self.project_password is not None and self.project_password_file is not None:
            logger.debug("Password File and Password text specified for WorkQueue Executor, only Password Text will be used")
            self.project_password_file = None
        if self.project_password_file is not None:
            if os.path.exists(self.project_password_file) is False:
                logger.debug("Password File does not exist, no file used")
                self.project_password_file = None

        self.launch_cmd = ("python3 workqueue_worker.py -i {input_file} -o {output_file} {remapping_string}")
        if self.shared_fs is True:
            self.launch_cmd += " --shared-fs"
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

        submit_process_kwargs = {"task_queue": self.task_queue,
                                 "queue_lock": self.queue_lock,
                                 "launch_cmd": self.launch_cmd,
                                 "data_dir": self.function_data_dir,
                                 "collector_queue": self.collector_queue,
                                 "see_worker_output": self.worker_output,
                                 "full": self.full,
                                 "cancel_value": self.cancel_value,
                                 "port": self.port,
                                 "wq_log_dir": self.wq_log_dir,
                                 "project_password": self.project_password,
                                 "project_password_file": self.project_password_file,
                                 "project_name": self.project_name}

        self.submit_process = multiprocessing.Process(target=WorkQueueSubmitThread,
                                                      name="submit_thread",
                                                      kwargs=submit_process_kwargs)

        collector_thread_kwargs = {"collector_queue": self.collector_queue,
                                   "tasks": self.tasks,
                                   "tasks_lock": self.tasks_lock,
                                   "cancel_value": self.cancel_value,
                                   "submit_process": self.submit_process,
                                   "executor": self}
        self.collector_thread = threading.Thread(target=WorkQueueCollectorThread,
                                                 name="wait_thread",
                                                 kwargs=collector_thread_kwargs)
        self.collector_thread.daemon = True

        self.submit_process.start()
        self.collector_thread.start()

    def create_name_tuple(self, parsl_file_obj, in_or_out):
        new_name = parsl_file_obj.filepath
        if parsl_file_obj.filepath not in self.used_names:
            if self.shared_fs is False:
                new_name = self.create_new_name(os.path.basename(parsl_file_obj.filepath))
                self.used_names[parsl_file_obj.filepath] = new_name
        else:
            new_name = self.used_names[parsl_file_obj.filepath]
        file_is_shared = False
        if parsl_file_obj in self.registered_files:
            file_is_shared = True
            if parsl_file_obj not in self.shared_files:
                self.shared_files.add(parsl_file_obj)
        else:
            self.registered_files.add(parsl_file_obj)
        return (parsl_file_obj.filepath, new_name, file_is_shared, in_or_out)

    def create_new_name(self, file_name):
        new_name = file_name
        index = 0
        while new_name in self.used_names:
            new_name = file_name + "-" + str(index)
            index += 1
        return new_name

    def submit(self, func, *args, **kwargs):
        """Submit.

        We haven't yet decided on what the args to this can be,
        whether it should just be func, args, kwargs or be the partially evaluated
        fn
        """
        self.task_counter += 1
        task_id = self.task_counter

        input_files = []
        output_files = []
        std_files = []

        func_inputs = kwargs.get("inputs", [])
        for inp in func_inputs:
            if isinstance(inp, File):
                input_files.append(self.create_name_tuple(inp, "in"))

        for kwarg, inp in kwargs.items():
            if kwarg == "stdout" or kwarg == "stderr":
                if (isinstance(inp, tuple) and len(inp) > 1 and isinstance(inp[0], str) and isinstance(inp[1], str)) or isinstance(inp, str):
                    if isinstance(inp, tuple):
                        inp = inp[0]
                    print(os.path.split(inp))
                    if not os.path.exists(os.path.join(".", os.path.split(inp)[0])):
                        continue
                    if inp in self.registered_files:
                        input_files.append((inp, os.path.basename(inp) + "-1", False, "std"))
                        output_files.append((inp, os.path.basename(inp), False, "std"))
                    else:
                        output_files.append((inp, os.path.basename(inp), False, "std"))
                        self.registered_files.add(inp)
            elif isinstance(inp, File):
                input_files.append(self.create_name_tuple(inp, "in"))

        for inp in args:
            if isinstance(inp, File):
                input_files.append(self.create_name_tuple(inp, "in"))

        func_outputs = kwargs.get("outputs", [])
        for output in func_outputs:
            if isinstance(output, File):
                output_files.append(self.create_name_tuple(output, "out"))

        if not self.submit_process.is_alive():
            raise ExecutorError(self, "Workqueue Submit Process is not alive")

        fu = Future()
        self.tasks_lock.acquire()
        self.tasks[str(task_id)] = fu
        self.tasks_lock.release()

        logger.debug("Creating task {} for function {} with args {}".format(task_id, func, args))

        # Pickle the result into object to pass into message buffer
        # TODO Try/Except Block
        function_data_file = os.path.join(self.function_data_dir, "task_" + str(task_id) + "_function_data")
        function_result_file = os.path.join(self.function_data_dir, "task_" + str(task_id) + "_function_result")

        logger.debug("Creating Task {} with executable at: {}".format(task_id, function_data_file))
        logger.debug("Creating Task {} with result to be found at: {}".format(task_id, function_result_file))

        f = open(function_data_file, "wb")
        fn_buf = pack_apply_message(func, args, kwargs,
                                    buffer_threshold=1024 * 1024,
                                    item_threshold=1024)
        pickle.dump(fn_buf, f)
        f.close()

        logger.debug("Placing task {} on message queue".format(task_id))
        msg = {"task_id": task_id,
               "data_loc": function_data_file,
               "result_loc": function_result_file,
               "input_files": input_files,
               "output_files": output_files,
               "std_files": std_files}

        self.task_queue.put_nowait(msg)

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
        logger.debug("Setting value to cancel")
        self.cancel_value.value = 0

        self.submit_process.join()
        self.collector_thread.join()

        return True

    def scaling_enabled(self):
        """Specify if scaling is enabled.

        The callers of ParslExecutors need to differentiate between Executors
        and Executors wrapped in a resource provider
        """
        return False

    def run_dir(self, value=None):
        """Path to the run directory.
        """
        if value is not None:
            self._run_dir = value
        return self._run_dir
