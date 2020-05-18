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
import itertools
from ipyparallel.serialize import pack_apply_message

import parsl.app.errors as perror
import parsl.utils as putils
from parsl.executors.errors import ExecutorError
from parsl.data_provider.files import File
from parsl.executors.status_handling import NoStatusHandlingExecutor
from parsl.providers.error import OptionalModuleMissing
from parsl.executors.workqueue import exec_parsl_function

from collections import namedtuple

try:
    import work_queue as wq
    from work_queue import WorkQueue
    from work_queue import Task
    from work_queue import WORK_QUEUE_DEFAULT_PORT
    from work_queue import WORK_QUEUE_ALLOCATION_MODE_MAX_THROUGHPUT
    from work_queue import cctools_debug_flags_set
    from work_queue import cctools_debug_config_file
except ImportError:
    _work_queue_enabled = False
    WORK_QUEUE_DEFAULT_PORT = 0
else:
    _work_queue_enabled = True

logger = logging.getLogger(__name__)


# Support structure to communicate parsl tasks to the work queue submit thread.
ParslTaskToWq = namedtuple('ParslTaskToWq', 'id category map_file function_file result_file input_files output_files')

# Support structure to communicate final status of work queue tasks to parsl
# result is only valid if result_received is True
# reason and status are only valid if result_received is False
WqTaskToParsl = namedtuple('WqTaskToParsl', 'id result_received result reason status')

# Support structure to report parsl filenames to work queue.
# parsl_name is the local_name or filepath attribute of a parsl file object.
# stage tells whether the file should be copied by work queue to the workers.
# cache tells whether the file should be cached at workers. Only valid if stage == True
ParslFileToWq = namedtuple('ParslFileToWq', 'parsl_name stage cache')


class WorkqueueTaskFailure(perror.AppException):
    """A failure executing a task in workqueue

    Contains:
    reason(string)
    status(int)
    """

    def __init__(self, reason, status):
        self.reason = reason
        self.status = status


def WorkQueueSubmitThread(task_queue=multiprocessing.Queue(),
                          queue_lock=threading.Lock(),
                          launch_cmd=None,
                          env=None,
                          collector_queue=multiprocessing.Queue(),
                          see_worker_output=False,
                          data_dir=".",
                          full=False,
                          shared_fs=False,
                          autolabel=False,
                          autolabel_window=None,
                          autocategory=False,
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
    if autolabel:
        q.enable_monitoring()
        if autolabel_window is not None:
            q.tune('category-steady-n-tasks', autolabel_window)

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

    result_file_of_task_id = {}  # Mapping taskid -> result file for active tasks.

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
                task = task_queue.get(timeout=1)
                logger.debug("Removing task from queue")
            except queue.Empty:
                continue

            # Create command string
            logger.debug(launch_cmd)
            command_str = launch_cmd.format(mapping=os.path.basename(task.map_file),
                                            function=os.path.basename(task.function_file),
                                            result=os.path.basename(task.result_file))
            logger.debug(command_str)

            # Create WorkQueue task for the command
            logger.debug("Sending task {} with command: {}".format(task.id, command_str))
            try:
                t = Task(command_str)
            except Exception as e:
                logger.error("Unable to create task: {}".format(e))
                collector_queue.put_nowait(WqTaskToParsl(id=task.id,
                                                         result_received=False,
                                                         result=None,
                                                         reason="task could not be created by work queue",
                                                         status=-1))
                continue

            t.specify_category(task.category)
            if autolabel:
                q.specify_category_mode(task.category, WORK_QUEUE_ALLOCATION_MODE_MAX_THROUGHPUT)

            # Specify environment variables for the task
            if env is not None:
                for var in env:
                    t.specify_environment_variable(var, env[var])

            # Specify script, and data/result files for task
            t.specify_input_file(exec_parsl_function.__file__, cache=True)
            t.specify_input_file(task.function_file, cache=False)
            t.specify_input_file(task.map_file, cache=False)
            t.specify_output_file(task.result_file, cache=False)
            t.specify_tag(str(task.id))
            result_file_of_task_id[str(task.id)] = task.result_file

            logger.debug("Parsl ID: {}".format(task.id))

            # Specify input/output files that need to be staged.
            # Absolute paths are assumed to be in shared filesystem, and thus
            # not staged by work queue.
            if not shared_fs:
                for spec in task.input_files:
                    if spec.stage:
                        t.specify_input_file(spec.parsl_name, spec.parsl_name, cache=spec.cache)
                for spec in task.output_files:
                    if spec.stage:
                        t.specify_output_file(spec.parsl_name, spec.parsl_name, cache=spec.cache)

            # Submit the task to the WorkQueue object
            logger.debug("Submitting task {} to WorkQueue".format(task.id))
            try:
                wq_id = q.submit(t)
                wq_tasks.add(wq_id)
            except Exception as e:
                logger.error("Unable to create task: {}".format(e))
                collector_queue.put_nowait(WqTaskToParsl(id=task.id,
                                                         result_received=False,
                                                         result=None,
                                                         reason="task could not be submited to work queue",
                                                         status=-1))
                continue

            logger.debug("Task {} submitted to WorkQueue with id {}".format(task.id, wq_id))

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
                # When a task is found:
                wq_tasks.remove(t.id)
                parsl_id = t.tag
                logger.debug("Completed WorkQueue task {}, parsl task {}".format(t.id, t.tag))
                result_file = result_file_of_task_id.pop(t.tag)

                # A tasks completes 'succesfully' if it has result file,
                # and it can be loaded. This may mean that the 'success' is
                # an exception.
                logger.debug("Looking for result in {}".format(result_file))
                try:
                    with open(result_file, "rb") as f_in:
                        result = pickle.load(f_in)
                    logger.debug("Found result in {}".format(result_file))
                    collector_queue.put_nowait(WqTaskToParsl(id=parsl_id,
                                                             result_received=True,
                                                             result=result,
                                                             reason=None,
                                                             status=t.return_status))
                # If a result file could not be generated, explain the
                # failure according to work queue error codes. We generate
                # an exception and wrap it with RemoteExceptionWrapper, to
                # match the positive case.
                except Exception as e:
                    reason = _explain_work_queue_result(t)
                    logger.debug("Did not find result in {}".format(result_file))
                    logger.debug("Wrapper Script status: {}\nWorkQueue Status: {}"
                                 .format(t.return_status, t.result))
                    logger.debug("Task with id parsl {} / wq {} failed because:\n{}"
                                 .format(parsl_id, t.id, reason))
                    collector_queue.put_nowait(WqTaskToParsl(id=parsl_id,
                                                             result_received=False,
                                                             result=e,
                                                             reason=reason,
                                                             status=t.return_status))

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
            task_report = collector_queue.get(timeout=1)
        except queue.Empty:
            continue

        # Obtain the future from the tasks dictionary
        tasks_lock.acquire()
        future = tasks[task_report.id]
        tasks_lock.release()

        logger.debug("Updating Future for Parsl Task {}".format(task_report.id))
        if task_report.result_received:
            future.set_result(task_report.result)
        else:
            # If there are no results, then the task failed according to one of
            # work queue modes, such as resource exhaustion.
            future.set_exception(WorkqueueTaskFailure(task_report.reason, task_report.result))

    logger.debug("Exiting Collector Thread")
    return


class WorkQueueExecutor(NoStatusHandlingExecutor):
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
            with respect to other Work Queue master programs.
            Default is "WorkQueueExecutor".

        working_dir: str
            Location for Parsl to perform app delegation to the Work
            Queue system. Defaults to current directory.

        managed: bool
            Whether this executor is managed by the DFK or externally handled.
            Default is True (managed by DFK).

        project_name: str
            If given, Work Queue master process name. Default is None.

        project_password: str
            Optional password for the Work Queue project. Default is None.

        project_password_file: str
            Optional password file for the work queue project. Default is None.

        port: int
            TCP port on Parsl submission machine for Work Queue workers
            to connect to. Workers will specify this port number when
            trying to connect to Parsl. Default is 9123.

        env: dict{str}
            Dictionary that contains the environmental variables that
            need to be set on the Work Queue worker machine.

        shared_fs: bool
            Define if working in a shared file system or not. If Parsl
            and the Work Queue workers are on a shared file system, Work
            Queue does not need to transfer and rename files for execution.
            Default is False.

        use_cache: bool
            Whether workers should cache files that are common to tasks.
            Warning: Two files are considered the same if they have the same
            filepath name. Use with care when reusing the executor instance
            across multiple parsl workflows. Default is False.

        source: bool
            Choose whether to transfer parsl app information as
            source code. (Note: this increases throughput for
            @python_apps, but the implementation does not include
            functionality for @bash_apps, and thus source=False
            must be used for programs utilizing @bash_apps.)
            Default is False.

        autolabel: bool
            Use the Resource Monitor to automatically determine resource
            labels based on observed task behavior.

        autolabel_window: int
            Set the number of tasks considered for autolabeling. Work Queue
            will wait for a series of N tasks with steady resource
            requirements before making a decision on labels. Increasing
            this parameter will reduce the number of failed tasks due to
            resource exhaustion when autolabeling, at the cost of increased
            resources spent collecting stats.

        autocategory: bool
            Place each app in its own category by default. If all
            invocations of an app have similar performance characteristics,
            this will provide a reasonable set of categories automatically.

        init_command: str
            Command line to run before executing a task in a worker.
            Default is ''.

        see_worker_output: bool
            Prints worker standard output when a task finishes.
            Default is False.
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
                 storage_access=None,
                 use_cache=False,
                 source=False,
                 autolabel=False,
                 autolabel_window=1,
                 autocategory=False,
                 init_command="",
                 full_debug=True,
                 see_worker_output=False):
        NoStatusHandlingExecutor.__init__(self)
        if not _work_queue_enabled:
            raise OptionalModuleMissing(['work_queue'], "WorkQueueExecutor requires the work_queue module.")

        self.label = label
        self.managed = managed
        self.task_queue = multiprocessing.Queue()
        self.collector_queue = multiprocessing.Queue()
        self.port = port
        self.task_counter = -1
        self.scaling_enabled = False
        self.project_name = project_name
        self.project_password = project_password
        self.project_password_file = project_password_file
        self.env = env
        self.init_command = init_command
        self.shared_fs = shared_fs
        self.storage_access = storage_access
        self.use_cache = use_cache
        self.working_dir = working_dir
        self.used_names = {}
        self.registered_files = set()
        self.worker_output = see_worker_output
        self.full = full_debug
        self.source = source
        self.autolabel = autolabel
        self.autolabel_window = autolabel_window
        self.autocategory = autocategory
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
        self.launch_cmd = ("python3 exec_parsl_function.py {mapping} {function} {result}")
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
                                 "shared_fs": self.shared_fs,
                                 "autolabel": self.autolabel,
                                 "autolabel_window": self.autolabel_window,
                                 "autocategory": self.autocategory,
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

    def _path_in_task(self, task_id, *path_components):
        """Returns a filename specific to a task.
        It is used for the following filename's:
            (not given): The subdirectory per task that contains function, result, etc.
            'function': Pickled file that contains the function to be executed.
            'result': Pickled file that (will) contain the result of the function.
            'map': Pickled file with a dict between local parsl names, and remote work queue names.
        """
        task_dir = "{:04d}".format(task_id)
        return os.path.join(self.function_data_dir, task_dir, *path_components)

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

        # Create a per task directory for the function, result, map, and result files
        os.mkdir(self._path_in_task(task_id))

        input_files = []
        output_files = []

        # Determine the input and output files that will exist at the workes:
        input_files += [self._register_file(f) for f in kwargs.get("inputs", []) if isinstance(f, File)]
        output_files += [self._register_file(f) for f in kwargs.get("outputs", []) if isinstance(f, File)]

        # Also consider any *arg that looks like a file as an input:
        input_files += [self._register_file(f) for f in args if isinstance(f, File)]

        for kwarg, maybe_file in kwargs.items():
            # Add appropriate input and output files from "stdout" and "stderr" keyword arguments
            if kwarg == "stdout" or kwarg == "stderr":
                if maybe_file:
                    output_files.append(self._std_output_to_wq(kwarg, maybe_file))
            # For any other keyword that looks like a file, assume it is an input file
            elif isinstance(maybe_file, File):
                input_files.append(self._register_file(maybe_file))

        # Create a Future object and have it be mapped from the task ID in the tasks dictionary
        fu = Future()
        self.tasks_lock.acquire()
        self.tasks[str(task_id)] = fu
        self.tasks_lock.release()

        logger.debug("Creating task {} for function {} with args {}".format(task_id, func, args))

        # Pickle the result into object to pass into message buffer
        function_file = self._path_in_task(task_id, "function")
        result_file = self._path_in_task(task_id, "result")
        map_file = self._path_in_task(task_id, "map")

        logger.debug("Creating Task {} with function at: {}".format(task_id, function_file))
        logger.debug("Creating Task {} with result to be found at: {}".format(task_id, result_file))

        self._serialize_function(function_file, func, args, kwargs)

        logger.debug("Constructing map for local filenames at worker for task {}".format(task_id))
        self._construct_map_file(map_file, input_files, output_files)

        if not self.submit_process.is_alive():
            raise ExecutorError(self, "Workqueue Submit Process is not alive")

        # Create message to put into the message queue
        logger.debug("Placing task {} on message queue".format(task_id))
        category = func.__qualname__ if self.autocategory else 'parsl-default'
        self.task_queue.put_nowait(ParslTaskToWq(task_id, category, map_file, function_file, result_file, input_files, output_files))

        return fu

    def _serialize_function(self, fn_path, parsl_fn, parsl_fn_args, parsl_fn_kwargs):
        """Takes the function application parsl_fn(*parsl_fn_args, **parsl_fn_kwargs)
        and serializes it to the file fn_path."""

        # Either build a dictionary with the source of the function, or pickle
        # the function directly:
        if self.source:
            function_info = {"source code": inspect.getsource(parsl_fn),
                             "name": parsl_fn.__name__,
                             "args": parsl_fn_args,
                             "kwargs": parsl_fn_kwargs}
        else:
            function_info = {"byte code": pack_apply_message(parsl_fn, parsl_fn_args, parsl_fn_kwargs,
                                                             buffer_threshold=1024 * 1024,
                                                             item_threshold=1024)}
            with open(fn_path, "wb") as f_out:
                pickle.dump(function_info, f_out)

    def _construct_map_file(self, map_file, input_files, output_files):
        """ Map local filepath of parsl files to the filenames at the execution worker.
        If using a shared filesystem, the filepath is mapped to its absolute filename.
        Otherwise, to its original relative filename. In this later case, work queue
        recreates any directory hierarchy needed."""
        file_translation_map = {}
        for spec in itertools.chain(input_files, output_files):
            local_name = spec[0]
            if self.shared_fs:
                remote_name = os.path.abspath(local_name)
            else:
                remote_name = local_name
            file_translation_map[local_name] = remote_name
        with open(map_file, "wb") as f_out:
            pickle.dump(file_translation_map, f_out)

    def _register_file(self, parsl_file):
        """Generates a tuple (parsl_file.filepath, stage, cache) to give to
        work queue. cache is always False if self.use_cache is False.
        Otherwise, it is set to True if parsl_file is used more than once.
        stage is True if the file needs to be copied by work queue. (i.e., not
        a URL or an absolute path)

        It has the side-effect of adding parsl_file to a list of registered
        files.

        Note: The first time a file is used cache is set to False. Since
        tasks are generated dynamically, without other information this is
        the best we can do."""
        to_cache = False
        if self.use_cache:
            to_cache = parsl_file in self.registered_files

        to_stage = False
        if parsl_file.scheme == 'file' or (parsl_file.local_path and os.path.exists(parsl_file.local_path)):
            to_stage = not os.path.isabs(parsl_file.filepath)

        self.registered_files.add(parsl_file)

        return ParslFileToWq(parsl_file.filepath, to_stage, to_cache)

    def _std_output_to_wq(self, fdname, stdfspec):
        """Find the name of the file that will contain stdout or stderr and
        return a ParslFileToWq with it. These files are never cached"""
        fname, mode = putils.get_std_fname_mode(fdname, stdfspec)
        to_stage = not os.path.isabs(fname)
        print(ParslFileToWq(fname, stage=to_stage, cache=False))
        return ParslFileToWq(fname, stage=to_stage, cache=False)

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


def _explain_work_queue_result(wq_task):
    """Returns a string with the reason why a task failed according to work queue."""

    wq_result = wq_task.result

    reason = "work queue result: "
    if wq_result == wq.WORK_QUEUE_RESULT_SUCCESS:
        reason += "succesful execution with exit code {}".format(wq_task.return_status)
    elif wq_result == wq.WORK_QUEUE_RESULT_OUTPUT_MISSING:
        reason += "The result file was not transfered from the worker.\n"
        reason += "This usually means that there is a problem with the python setup,\n"
        reason += "or the wrapper that executes the function."
        reason += "\nTrace:\n" + str(wq_task.output)
    elif wq_result == wq.WORK_QUEUE_RESULT_INPUT_MISSING:
        reason += "missing input file"
    elif wq_result == wq.WORK_QUEUE_RESULT_STDOUT_MISSING:
        reason += "stdout has been truncated"
    elif wq_result == wq.WORK_QUEUE_RESULT_SIGNAL:
        reason += "task terminated with a signal"
    elif wq_result == wq.WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION:
        reason += "task used more resources than requested"
    elif wq_result == wq.WORK_QUEUE_RESULT_TASK_TIMEOUT:
        reason += "task ran past the specified end time"
    elif wq_result == wq.WORK_QUEUE_RESULT_UNKNOWN:
        reason += "result could not be classified"
    elif wq_result == wq.WORK_QUEUE_RESULT_FORSAKEN:
        reason += "task failed, but not a task error"
    elif wq_result == wq.WORK_QUEUE_RESULT_MAX_RETRIES:
        reason += "unable to complete after specified number of retries"
    elif wq_result == wq.WORK_QUEUE_RESULT_TASK_MAX_RUN_TIME:
        reason += "task ran for more than the specified time"
    elif wq_result == wq.WORK_QUEUE_RESULT_DISK_ALLOC_FULL:
        reason += "task needed more space to complete task"
    elif wq_result == wq.WORK_QUEUE_RESULT_RMONITOR_ERROR:
        reason += "task failed because the monitor did not produce an output"
    else:
        reason += "unable to process Work Queue system failure"
    return reason
