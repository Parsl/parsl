"""WorkQueueExecutor utilizes the Work Queue distributed framework developed by the
Cooperative Computing Lab (CCL) at Notre Dame to provide a fault-tolerant,
high-throughput system for delegating Parsl tasks to thousands of remote machines
"""

import threading
import multiprocessing
import logging
from concurrent.futures import Future

import os
import pickle
import queue
import inspect
from ipyparallel.serialize import pack_apply_message

from parsl.app.errors import AppFailure
from parsl.app.errors import RemoteExceptionWrapper
from parsl.executors.errors import ExecutorError
from parsl.executors.base import ParslExecutor
from parsl.data_provider.files import File
from parsl.providers.error import OptionalModuleMissing
from parsl.executors.workqueue import workqueue_worker

try:
    from work_queue import WorkQueue
    from work_queue import Task
    from work_queue import WORK_QUEUE_DEFAULT_PORT
    from work_queue import WORK_QUEUE_INPUT
    from work_queue import WORK_QUEUE_OUTPUT
    from work_queue import WORK_QUEUE_RESULT_SUCCESS
    from work_queue import WORK_QUEUE_RESULT_OUTPUT_MISSING
    from work_queue import cctools_debug_flags_set
    from work_queue import cctools_debug_config_file
except ImportError:
    _work_queue_enabled = False
    WORK_QUEUE_DEFAULT_PORT = 0
else:
    _work_queue_enabled = True

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
    """Thread to handle Parsl app submissions to the Work Queue objects.
    Takes in Parsl functions submitted using submit(), and creates a
    Work Queue task with the appropriate specifications, which is then
    submitted to Work Queue. After tasks are completed, processes the
    exit status and exit code of the task, and sends results to the
    Work Queue collector thread.
    """
    logger.debug("Starting WorkQueue Submit/Wait Process")

    # Enable debugging flags and create logging file
    if wq_log_dir is not None:
        logger.debug("Setting debugging flags and creating logging file")
        wq_debug_log = os.path.join(wq_log_dir, "debug_log")
        cctools_debug_flags_set("all")
        cctools_debug_config_file(wq_debug_log)

    # Create WorkQueue queue object
    logger.debug("Creating WorkQueue Object")
    try:
        logger.debug("Listening on port {}".format(port))
        q = WorkQueue(port)
    except Exception as e:
        logger.error("Unable to create WorkQueue object: {}".format(e))
        raise e

    # Specify WorkQueue queue attributes
    if project_name:
        q.specify_name(project_name)
    if project_password:
        q.specify_password(project_password)
    elif project_password_file:
        q.specify_password_file(project_password_file)

    # Only write logs when the wq_log_dir is specified, which it most likely will be
    if wq_log_dir is not None:
        wq_master_log = os.path.join(wq_log_dir, "master_log")
        wq_trans_log = os.path.join(wq_log_dir, "transaction_log")
        if full:
            wq_resource_log = os.path.join(wq_log_dir, "resource_logs")
            q.enable_monitoring_full(dirname=wq_resource_log)
        q.specify_log(wq_master_log)
        q.specify_transactions_log(wq_trans_log)

    wq_tasks = set()
    orig_ppid = os.getppid()
    continue_running = True
    while(continue_running):
        # Monitor the task queue
        ppid = os.getppid()
        if ppid != orig_ppid:
            logger.debug("new Process")
            continue_running = False
            continue

        # Submit tasks
        while task_queue.qsize() > 0:
            if cancel_value.value == 0:
                logger.debug("cancel value set to cancel")
                continue_running = False
                break

            # Obtain task from task_queue
            try:
                item = task_queue.get(timeout=1)
                logger.debug("Removing task from queue")
            except queue.Empty:
                continue
            parsl_id = item["task_id"]

            # Extract information about the task
            function_data_loc = item["data_loc"]
            function_data_loc_remote = function_data_loc.split("/")[-1]
            function_result_loc = item["result_loc"]
            function_result_loc_remote = function_result_loc.split("/")[-1]
            input_files = item["input_files"]
            output_files = item["output_files"]
            std_files = item["std_files"]

            full_script_name = workqueue_worker.__file__
            script_name = full_script_name.split("/")[-1]

            remapping_string = ""
            std_string = ""

            # Parse input file information
            logger.debug("Looking at input")
            for item in input_files:
                if item[3] == "std":
                    std_string += "mv " + item[1] + " " + item[0] + "; "
                else:
                    remapping_string += item[0] + ":" + item[1] + ","
            logger.debug(remapping_string)

            # Parse output file information
            logger.debug("Looking at output")
            for item in output_files:
                remapping_string += item[0] + ":" + item[1] + ","
            logger.debug(remapping_string)

            if len(input_files) + len(output_files) > 0:
                remapping_string = "-r " + remapping_string
                remapping_string = remapping_string[:-1]

            # Create command string
            logger.debug(launch_cmd)
            command_str = launch_cmd.format(input_file=function_data_loc_remote,
                                            output_file=function_result_loc_remote,
                                            remapping_string=remapping_string)
            command_str = std_string + command_str
            logger.debug(command_str)

            # Create WorkQueue task for the command
            logger.debug("Sending task {} with command: {}".format(parsl_id, command_str))
            try:
                t = Task(command_str)
            except Exception as e:
                logger.error("Unable to create task: {}".format(e))
                continue

            # Specify environment variables for the task
            if env is not None:
                for var in env:
                    t.specify_environment_variable(var, env[var])

            # Specify script, and data/result files for task
            t.specify_file(full_script_name, script_name, WORK_QUEUE_INPUT, cache=True)
            t.specify_file(function_data_loc, function_data_loc_remote, WORK_QUEUE_INPUT, cache=False)
            t.specify_file(function_result_loc, function_result_loc_remote, WORK_QUEUE_OUTPUT, cache=False)
            t.specify_tag(str(parsl_id))
            logger.debug("Parsl ID: {}".format(t.id))

            # Specify all input/output files for task
            for item in input_files:
                t.specify_file(item[0], item[1], WORK_QUEUE_INPUT, cache=item[2])
            for item in output_files:
                t.specify_file(item[0], item[1], WORK_QUEUE_OUTPUT, cache=item[2])
            for item in std_files:
                t.specify_file(item[0], item[1], WORK_QUEUE_OUTPUT, cache=item[2])

            # Submit the task to the WorkQueue object
            logger.debug("Submitting task {} to WorkQueue".format(parsl_id))
            try:
                wq_id = q.submit(t)
                wq_tasks.add(wq_id)
            except Exception as e:
                logger.error("Unable to create task: {}".format(e))

                msg = {"tid": parsl_id,
                       "result_received": False,
                       "reason": "Workqueue Task Start Failure",
                       "status": 1}

                collector_queue.put_nowait(msg)
                continue

            logger.debug("Task {} submitted to WorkQueue with id {}".format(parsl_id, wq_id))

        if cancel_value.value == 0:
            continue_running = False

        # If the queue is not empty wait on the WorkQueue queue for a task
        task_found = True
        if not q.empty() and continue_running:
            while task_found is True:
                if cancel_value.value == 0:
                    continue_running = False
                    task_found = False
                    continue

                # Obtain the task from the queue
                t = q.wait(1)
                if t is None:
                    task_found = False
                    continue
                else:
                    parsl_tid = t.tag
                    logger.debug("Completed WorkQueue task {}, parsl task {}".format(t.id, parsl_tid))
                    status = t.return_status
                    task_result = t.result
                    msg = None

                    # Task failure
                    if status != 0 or (task_result != WORK_QUEUE_RESULT_SUCCESS and task_result != WORK_QUEUE_RESULT_OUTPUT_MISSING):
                        logger.debug("Wrapper Script status: {}\nWorkQueue Status: {}".format(status, task_result))
                        # Wrapper script failure
                        if status != 0:
                            logger.debug("WorkQueue task {} failed with status {}".format(t.id, status))
                            reason = "Wrapper Script Failure: "
                            if status == 1:
                                reason += "problem parsing command line options"
                            elif status == 2:
                                reason += "problem loading function data"
                            elif status == 3:
                                reason += "problem remapping file names"
                            elif status == 4:
                                reason += "problem writing out function result"
                            else:
                                reason += "unable to process wrapper script failure with status = {}".format(status)
                            reason += "\nTrace:\n" + str(t.output)
                            logger.debug("WorkQueue runner script failed for task {} because {}\n".format(parsl_tid, reason))
                        # WorkQueue system failure
                        else:
                            reason = "WorkQueue System Failure: "
                            if task_result == 1:
                                reason += "missing input file"
                            elif task_result == 2:
                                reason += "unable to generate output file"
                            elif task_result == 4:
                                reason += "stdout has been truncated"
                            elif task_result == 1 << 3:
                                reason += "task terminated with a signal"
                            elif task_result == 2 << 3:
                                reason += "task used more resources than requested"
                            elif task_result == 3 << 3:
                                reason += "task ran past the specified end time"
                            elif task_result == 4 << 3:
                                reason += "result could not be classified"
                            elif task_result == 5 << 3:
                                reason += "task failed, but not a task error"
                            elif task_result == 6 << 3:
                                reason += "unable to complete after specified number of retries"
                            elif task_result == 7 << 3:
                                reason += "task ran for more than the specified time"
                            elif task_result == 8 << 3:
                                reason += "task needed more space to complete task"
                            else:
                                reason += "unable to process Work Queue system failure"

                        msg = {"tid": parsl_tid,
                               "result_received": False,
                               "reason": reason,
                               "status": status}

                        collector_queue.put_nowait(msg)

                    # Task Success
                    else:
                        # Print the output from the task
                        if see_worker_output:
                            print(t.output)

                        # Load result into result file
                        result_loc = os.path.join(data_dir, "task_" + str(parsl_tid) + "_function_result")
                        logger.debug("Looking for result in {}".format(result_loc))
                        f = open(result_loc, "rb")
                        result = pickle.load(f)
                        f.close()

                        msg = {"tid": parsl_tid,
                               "result_received": True,
                               "result": result}
                        wq_tasks.remove(t.id)

                    collector_queue.put_nowait(msg)

        if continue_running is False:
            logger.debug("Exiting WorkQueue Master Thread event loop")
            break

    # Remove all WorkQueue tasks that remain in the queue object
    for wq_task in wq_tasks:
        logger.debug("Cancelling WorkQueue Task {}".format(wq_task))
        q.cancel_by_taskid(wq_task)

    logger.debug("Exiting WorkQueue Monitoring Process")
    return 0


def WorkQueueCollectorThread(collector_queue=multiprocessing.Queue(),
                             tasks={},
                             tasks_lock=threading.Lock(),
                             cancel_value=multiprocessing.Value('i', 1),
                             submit_process=None,
                             executor=None):
    """Processes completed Parsl tasks. If an error arose while the Parsl task
    was executed, raises the exception on the local machine.
    """

    logger.debug("Starting Collector Thread")

    continue_running = True
    while continue_running:
        if cancel_value.value == 0:
            continue_running = False
            continue

        # The WorkQueue process that creates task has died
        if not submit_process.is_alive() and cancel_value.value != 0:
            raise ExecutorError(executor, "Workqueue Submit Process is not alive")

        # Get the result message from the collector_queue
        try:
            item = collector_queue.get(timeout=1)
        except queue.Empty:
            continue

        parsl_tid = item["tid"]
        received = item["result_received"]

        # Obtain the future from the tasks dictionary
        tasks_lock.acquire()
        future = tasks[parsl_tid]
        tasks_lock.release()

        # Failed task
        if received is False:
            reason = item["reason"]
            status = item["status"]
            future.set_exception(AppFailure(reason, status))
        # Successful task
        else:
            result = item["result"]
            future_update = result["result"]
            logger.debug("Updating Future for Parsl Task {}".format(parsl_tid))
            if result["failure"] is False:
                future.set_result(future_update)
            else:
                future_fail = pickle.loads(future_update)
                exc = RemoteExceptionWrapper(*future_fail)
                try:
                    exc.reraise()
                except Exception as e:
                    future.set_exception(e)

    logger.debug("Exiting Collector Thread")
    return


class WorkQueueExecutor(ParslExecutor):
    """Executor to use Work Queue batch system

    The WorkQueueExecutor system utilizes the Work Queue framework to
    efficiently delegate Parsl apps to remote machines in clusters and
    grids using a fault-tolerant system. Users can run the
    work_queue_worker program on remote machines to connect to the
    WorkQueueExecutor, and Parsl apps will then be sent out to these
    machines for execution and retrieval.


        Parameters
        ----------

        label: str
            A human readable label for the executor, unique
            with respect to other Work Queue master programs

        working_dir: str
            Location for Parsl to perform app delegation to the Work
            Queue system

        managed: bool
            If this executor is managed by the DFK or externally handled

        project_name: str
            Work Queue process name

        project_password: str
            Optional password for the Work Queue project

        project_password_file: str
            Optional password file for the work queue project

        port: int
            TCP port on Parsl submission machine for Work Queue workers
            to connect to. Workers will specify this port number when
            trying to connect to Parsl

        env: dict{str}
            Dictionary that contains the environmental variables that
            need to be set on the Work Queue worker machine

        shared_fs: bool
            Define if working in a shared file system or not. If Parsl
            and the Work Queue workers are on a shared file system, Work
            Queue does not need to transfer and rename files for execution

        source: bool
            Choose whether to transfer parsl app information as
            source code. (Note: this increases throughput for
            @python_apps, but the implementation does not include
            functionality for @bash_apps, and thus source=False
            must be used for programs utilizing @bash_apps.)

        init_command: str
            Command to run before constructed Work Queue command

        see_worker_output: bool
            Prints worker standard output if true


    """

    def __init__(self,
                 label="WorkQueueExecutor",
                 working_dir=".",
                 managed=True,
                 project_name=None,
                 project_password=None,
                 project_password_file=None,
                 port=WORK_QUEUE_DEFAULT_PORT,
                 env=None,
                 shared_fs=False,
                 source=False,
                 init_command="",
                 full_debug=True,
                 see_worker_output=False):
        if not _work_queue_enabled:
            raise OptionalModuleMissing(['work_queue'], "WorkQueueExecutor requires the work_queue module.")

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
        self.full = full_debug
        self.source = source
        self.cancel_value = multiprocessing.Value('i', 1)

        # Resolve ambiguity when password and password_file are both specified
        if self.project_password is not None and self.project_password_file is not None:
            logger.warning("Password File and Password text specified for WorkQueue Executor, only Password Text will be used")
            self.project_password_file = None
        if self.project_password_file is not None:
            if os.path.exists(self.project_password_file) is False:
                logger.debug("Password File does not exist, no file used")
                self.project_password_file = None

        # Build foundations of the launch command
        self.launch_cmd = ("python3 workqueue_worker.py -i {input_file} -o {output_file} {remapping_string}")
        if self.shared_fs is True:
            self.launch_cmd += " --shared-fs"
        if self.source is True:
            self.launch_cmd += " --source"
        if self.init_command != "":
            self.launch_cmd = self.init_command + "; " + self.launch_cmd

    def start(self):
        """Create submit process and collector thread to create, send, and
        retrieve Parsl tasks within the Work Queue system.
        """
        self.queue_lock = threading.Lock()
        self.tasks_lock = threading.Lock()

        # Create directories for data and results
        self.function_data_dir = os.path.join(self.run_dir, "function_data")
        self.wq_log_dir = os.path.join(self.run_dir, self.label)
        logger.debug("function data directory: {}\nlog directory: {}".format(self.function_data_dir, self.wq_log_dir))
        os.mkdir(self.function_data_dir)
        os.mkdir(self.wq_log_dir)

        logger.debug("Starting WorkQueueExectutor")

        # Create a Process to perform WorkQueue submissions
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

        # Create a process to analyze WorkQueue task completions
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

        # Begin both processes
        self.submit_process.start()
        self.collector_thread.start()

    def create_name_tuple(self, parsl_file_obj, in_or_out):
        """Returns a tuple containing information about an input or output file
        to a Parsl app. Utilized to specify input and output files for a specific
        Work Queue task within the system.

        Parameters
        ---------

        parsl_file_obj : str
            Name of file specified as input or output file to the Parsl app

        in_or_out : str
            Specifies whether the file is an input or output file to the Parsl
            app
        """
        # Determine new_name
        new_name = parsl_file_obj.filepath
        if parsl_file_obj.filepath not in self.used_names:
            if self.shared_fs is False:
                new_name = self.create_new_name(os.path.basename(parsl_file_obj.filepath))
                self.used_names[parsl_file_obj.filepath] = new_name
        else:
            new_name = self.used_names[parsl_file_obj.filepath]
        # Determine file_is_shared
        file_is_shared = False
        if parsl_file_obj in self.registered_files:
            file_is_shared = True
            if parsl_file_obj not in self.shared_files:
                self.shared_files.add(parsl_file_obj)
        else:
            self.registered_files.add(parsl_file_obj)
        return (parsl_file_obj.filepath, new_name, file_is_shared, in_or_out)

    def create_new_name(self, file_name):
        """Returns a unique file name for an input file name. If the file name
        is already unique with respect to the Parsl process, then it returns
        the original file name

        Parameters
        ----------

        file_name : str
            Name of file that needs to be unique
        """
        new_name = file_name
        index = 0
        while new_name in self.used_names:
            new_name = file_name + "-" + str(index)
            index += 1
        return new_name

    def submit(self, func, *args, **kwargs):
        """Processes the Parsl app by its arguments and submits the function
        information to the task queue, to be executed using the Work Queue
        system. The args and kwargs are processed for input and output files to
        the Parsl app, so that the files are appropriately specified for the Work
        Queue task.

        Parameters
        ----------

        func : function
            Parsl app to be submitted to the Work Queue system
        args : list
            Arguments to the Parsl app
        kwargs : dict
            Keyword arguments to the Parsl app
        """
        self.task_counter += 1
        task_id = self.task_counter

        input_files = []
        output_files = []
        std_files = []

        # Add input files from the "inputs" keyword argument
        func_inputs = kwargs.get("inputs", [])
        for inp in func_inputs:
            if isinstance(inp, File):
                input_files.append(self.create_name_tuple(inp, "in"))

        for kwarg, inp in kwargs.items():
            # Add appropriate input and output files from "stdout" and "stderr" keyword arguments
            if kwarg == "stdout" or kwarg == "stderr":
                if (isinstance(inp, tuple) and len(inp) > 1 and isinstance(inp[0], str) and isinstance(inp[1], str)) or isinstance(inp, str):
                    if isinstance(inp, tuple):
                        inp = inp[0]
                    if not os.path.exists(os.path.join(".", os.path.split(inp)[0])):
                        continue
                    # Create "std" files instead of input or output files
                    if inp in self.registered_files:
                        input_files.append((inp, os.path.basename(inp) + "-1", False, "std"))
                        output_files.append((inp, os.path.basename(inp), False, "std"))
                    else:
                        output_files.append((inp, os.path.basename(inp), False, "std"))
                        self.registered_files.add(inp)
            # Add to input file if passed-in argument is a File object
            elif isinstance(inp, File):
                input_files.append(self.create_name_tuple(inp, "in"))

        # Add to input file if passed-in argument is a File object
        for inp in args:
            if isinstance(inp, File):
                input_files.append(self.create_name_tuple(inp, "in"))

        # Add output files from the "outputs" keyword argument
        func_outputs = kwargs.get("outputs", [])
        for output in func_outputs:
            if isinstance(output, File):
                output_files.append(self.create_name_tuple(output, "out"))

        if not self.submit_process.is_alive():
            raise ExecutorError(self, "Workqueue Submit Process is not alive")

        # Create a Future object and have it be mapped from the task ID in the tasks dictionary
        fu = Future()
        self.tasks_lock.acquire()
        self.tasks[str(task_id)] = fu
        self.tasks_lock.release()

        logger.debug("Creating task {} for function {} with args {}".format(task_id, func, args))

        # Pickle the result into object to pass into message buffer
        function_data_file = os.path.join(self.function_data_dir, "task_" + str(task_id) + "_function_data")
        function_result_file = os.path.join(self.function_data_dir, "task_" + str(task_id) + "_function_result")

        logger.debug("Creating Task {} with executable at: {}".format(task_id, function_data_file))
        logger.debug("Creating Task {} with result to be found at: {}".format(task_id, function_result_file))

        # Obtain function information and put into dictionary
        if self.source:
            # Obtain function information and put into dictionary
            source_code = inspect.getsource(func)
            name = func.__name__
            function_info = {"source code": source_code,
                             "name": name,
                             "args": args,
                             "kwargs": kwargs}

            # Pack the function data into file
            f = open(function_data_file, "wb")
            pickle.dump(function_info, f)
            f.close()
        else:
            # Serialize function information
            function_info = pack_apply_message(func, args, kwargs,
                                               buffer_threshold=1024 * 1024,
                                               item_threshold=1024)

            # Pack the function data into file
            f = open(function_data_file, "wb")
            pickle.dump(function_info, f)
            f.close()

        # Create message to put into the message queue
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
        """Scale out method. Not implemented.
        """
        pass

    def scale_in(self, count):
        """Scale in method. Not implemented.
        """
        pass

    def shutdown(self, *args, **kwargs):
        """Shutdown the executor. Sets flag to cancel the submit process and
        collector thread, which shuts down the Work Queue system submission.
        """
        # Set shared variable to 0 to signal shutdown
        logger.debug("Setting value to cancel")
        self.cancel_value.value = 0

        self.submit_process.join()
        self.collector_thread.join()

        return True

    def scaling_enabled(self):
        """Specify if scaling is enabled. Not enabled in Work Queue.
        """
        return False

    def run_dir(self, value=None):
        """Path to the run directory.
        """
        if value is not None:
            self._run_dir = value
        return self._run_dir
