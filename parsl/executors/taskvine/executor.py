""" TaskVineExecutor utilizes the TaskVine distributed framework developed by the
Cooperative Computing Lab (CCL) at Notre Dame to provide a fault-tolerant,
high-throughput system for delegating Parsl tasks to thousands of remote machines
"""

import getpass
import hashlib
import inspect
import itertools
import logging
import multiprocessing
import os
import queue
import shutil
import subprocess
import tempfile

# Import Python built-in libraries
import threading
import uuid
from concurrent.futures import Future
from datetime import datetime
from typing import List, Literal, Optional, Union

# Import other libraries
import typeguard

# Import Parsl constructs
import parsl.utils as putils
from parsl.addresses import get_any_address
from parsl.data_provider.files import File
from parsl.data_provider.staging import Staging
from parsl.errors import OptionalModuleMissing
from parsl.executors.errors import ExecutorError
from parsl.executors.status_handling import BlockProviderExecutor
from parsl.executors.taskvine import exec_parsl_function
from parsl.executors.taskvine.errors import TaskVineManagerFailure, TaskVineTaskFailure
from parsl.executors.taskvine.factory import _taskvine_factory
from parsl.executors.taskvine.factory_config import TaskVineFactoryConfig
from parsl.executors.taskvine.manager import _taskvine_submit_wait
from parsl.executors.taskvine.manager_config import TaskVineManagerConfig
from parsl.executors.taskvine.utils import ParslFileToVine, ParslTaskToVine
from parsl.process_loggers import wrap_with_logs
from parsl.providers import CondorProvider, LocalProvider
from parsl.providers.base import ExecutionProvider
from parsl.serialize import deserialize, serialize

logger = logging.getLogger(__name__)


class TaskVineExecutor(BlockProviderExecutor, putils.RepresentationMixin):
    """Executor to use TaskVine dynamic workflow system

    The TaskVineExecutor system utilizes the TaskVine framework to
    efficiently delegate Parsl apps to remote machines in clusters and
    grids using a fault-tolerant system. Users can run the
    vine_worker program on remote machines to connect to the
    TaskVineExecutor, and Parsl apps will then be sent out to these
    machines for execution and retrieval.

    This Executor sets up configurations for the TaskVine manager, TaskVine
    factory, and run both in separate processes. Sending tasks and receiving
    results are done through multiprocessing module native to Python.

    Parameters
    ----------

        label: str
            A human readable label for the executor, unique
            with respect to other executors.
            Default is "TaskVineExecutor".

        worker_launch_method: Union[Literal['provider'], Literal['factory'], Literal['manual']]
            Choose to use Parsl provider, TaskVine factory, or
            manual user-provided workers to scale workers.
            Options are among {'provider', 'factory', 'manual'}.
            Default is 'factory'.

        function_exec_mode: Union[Literal['regular'], Literal['serverless']]
            Choose to execute functions with a regular fresh python process or a
            pre-warmed forked python process.
            Default is 'regular'.

        manager_config: TaskVineManagerConfig
            Configuration for the TaskVine manager. Default

        factory_config: TaskVineFactoryConfig
            Configuration for the TaskVine factory.
            Use of factory is disabled by default.

        provider: ExecutionProvider
            The Parsl provider that will spawn worker processes.
            Default to spawning one local vine worker process.

        storage_access: List[Staging]
            Define Parsl file staging providers for this executor.
            Default is None.
    """

    radio_mode = "filesystem"

    @typeguard.typechecked
    def __init__(self,
                 label: str = "TaskVineExecutor",
                 worker_launch_method: Union[Literal['provider'], Literal['factory'], Literal['manual']] = 'factory',
                 function_exec_mode: Union[Literal['regular'], Literal['serverless']] = 'regular',
                 manager_config: TaskVineManagerConfig = TaskVineManagerConfig(),
                 factory_config: TaskVineFactoryConfig = TaskVineFactoryConfig(),
                 provider: Optional[ExecutionProvider] = LocalProvider(init_blocks=1),
                 storage_access: Optional[List[Staging]] = None):

        # Set worker launch option for this executor
        if worker_launch_method == 'factory' or worker_launch_method == 'manual':
            provider = None

        # Initialize the parent class with the execution provider and block error handling enabled.
        # If provider is None, then no worker is launched via the provider method.
        BlockProviderExecutor.__init__(self, provider=provider,
                                       block_error_handler=True)

        # Raise an exception if there's a problem importing TaskVine
        try:
            import ndcctools.taskvine
            logger.debug(f'TaskVine default port: {ndcctools.taskvine.cvine.VINE_DEFAULT_PORT}')
        except ImportError:
            raise OptionalModuleMissing(['taskvine'], "TaskVineExecutor requires the taskvine module.")

        # Executor configurations
        self.label = label
        self.worker_launch_method = worker_launch_method
        self.function_exec_mode = function_exec_mode
        self.manager_config = manager_config
        self.factory_config = factory_config
        self.storage_access = storage_access

        # Queue to send ready tasks from TaskVine executor process to TaskVine manager process
        self._ready_task_queue: multiprocessing.Queue = multiprocessing.Queue()

        # Queue to send finished tasks from TaskVine manager process to TaskVine executor process
        self._finished_task_queue: multiprocessing.Queue = multiprocessing.Queue()

        # Event to signal whether the manager and factory processes should stop running
        self._should_stop = multiprocessing.Event()

        # TaskVine manager process
        self._submit_process = None

        # TaskVine factory process
        self._factory_process = None

        # Executor thread to collect results from TaskVine manager and set
        # tasks' futures to done status.
        self._collector_thread = None

        # track task id of submitted parsl tasks
        # task ids are incremental and start from 0
        self._executor_task_counter = 0

        # track number of tasks that are waiting/running
        self._outstanding_tasks = 0

        # Lock for threads to access self._outstanding_tasks attribute
        self._outstanding_tasks_lock = threading.Lock()

        # Threading lock to manage self.tasks dictionary object, which maps a task id
        # to its future object.
        self._tasks_lock = threading.Lock()

        # Worker command to be given to an execution provider (e.g., local or Condor)
        self._worker_command = ""

        # Path to directory that holds all tasks' data and results.
        self._function_data_dir = ""

        # Helper scripts to prepare package tarballs for Parsl apps
        self._package_analyze_script = shutil.which("poncho_package_analyze")
        self._package_create_script = shutil.which("poncho_package_create")
        if self._package_analyze_script is None or self._package_create_script is None:
            self._poncho_available = False
        else:
            self._poncho_available = True

    def _get_launch_command(self, block_id):
        # Implements BlockProviderExecutor's abstract method.
        # This executor uses different terminology for worker/launch
        # commands than in htex.
        return f"PARSL_WORKER_BLOCK_ID={block_id} {self._worker_command}"

    def __synchronize_manager_factory_comm_settings(self):
        # Synchronize the communication settings between the manager and the factory
        # so the factory can direct workers to contact the manager.

        # If the manager can choose any available port (port number = 0),
        # then it must have a project name
        # so the factory can look it up. Otherwise the factory process will not know the
        # port number as it's only chosen when the TaskVine manager process is run.
        if self.manager_config.port == 0 and self.manager_config.project_name is None:
            self.manager_config.project_name = "parsl-vine-" + str(uuid.uuid4())

        # guess the host name if the project name is not given and none has been supplied
        # explicitly in the manager config.
        if not self.manager_config.project_name and self.manager_config.address is None:
            self.manager_config.address = get_any_address()

        # Factory communication settings are overridden by manager communication settings.
        self.factory_config._project_port = self.manager_config.port
        self.factory_config._project_address = self.manager_config.address
        self.factory_config._project_name = self.manager_config.project_name
        self.factory_config._project_password_file = self.manager_config.project_password_file
        logger.debug('Communication settings between TaskVine manager and factory synchronized.')

    def __create_data_and_logging_dirs(self):
        # Create neccessary data and logging directories

        # Use the current run directory from Parsl
        run_dir = self.run_dir

        # Create directories for data and results
        log_dir = os.path.join(run_dir, self.label)
        os.makedirs(log_dir)
        tmp_prefix = f'{self.label}-{getpass.getuser()}-{datetime.now().strftime("%Y%m%d%H%M%S%f")}-'
        self._function_data_dir = tempfile.TemporaryDirectory(prefix=tmp_prefix)

        # put TaskVine logs outside of a Parsl run as TaskVine caches between runs while
        # Parsl does not.
        vine_log_dir = os.path.join(os.path.dirname(run_dir), self.label)
        if self.manager_config.vine_log_dir is None:
            self.manager_config.vine_log_dir = vine_log_dir

        # factory logs go with manager logs regardless
        self.factory_config.scratch_dir = self.manager_config.vine_log_dir
        logger.debug(f"Function data directory: {self._function_data_dir.name}, log directory: {log_dir}")
        logger.debug(
            f"TaskVine manager log directory: {self.manager_config.vine_log_dir}, "
            f"factory log directory: {self.factory_config.scratch_dir}")

    def start(self):
        """Create submit process and collector thread to create, send, and
        retrieve Parsl tasks within the TaskVine system.
        """

        # Synchronize connection and communication settings between the manager and factory
        self.__synchronize_manager_factory_comm_settings()

        # Create data and logging dirs
        self.__create_data_and_logging_dirs()

        logger.debug("Starting TaskVineExecutor")

        # Create a process to run the TaskVine manager.
        submit_process_kwargs = {"ready_task_queue": self._ready_task_queue,
                                 "finished_task_queue": self._finished_task_queue,
                                 "should_stop": self._should_stop,
                                 "manager_config": self.manager_config}
        self._submit_process = multiprocessing.Process(target=_taskvine_submit_wait,
                                                       name="TaskVine-Submit-Process",
                                                       kwargs=submit_process_kwargs)

        # Create a process to run the TaskVine factory if enabled.
        if self.worker_launch_method == 'factory':
            factory_process_kwargs = {"should_stop": self._should_stop,
                                      "factory_config": self.factory_config}
            self._factory_process = multiprocessing.Process(target=_taskvine_factory,
                                                            name="TaskVine-Factory-Process",
                                                            kwargs=factory_process_kwargs)

        # Run thread to collect results and set tasks' futures.
        self._collector_thread = threading.Thread(target=self._collect_taskvine_results,
                                                  name="TaskVine-Collector-Thread")
        # Interpreter can exit without waiting for this thread.
        self._collector_thread.daemon = True

        # Begin work
        self._submit_process.start()

        # Run worker scaler either with Parsl provider or TaskVine factory.
        # Skip if workers are launched manually.
        if self.worker_launch_method == 'factory':
            self._factory_process.start()
        elif self.worker_launch_method == 'provider':
            self.initialize_scaling()

        self._collector_thread.start()

        logger.debug("All components in TaskVineExecutor started")

    def _path_in_task(self, executor_task_id, *path_components):
        """
        Returns a filename fixed and specific to a task.
        It is used for the following filename's:
            (not given): The subdirectory per task that contains function, result, etc.
            'function': Pickled file that contains the function to be executed.
            'argument': Pickled file that contains the arguments of the function call.
            'result': Pickled file that (will) contain the result of the function.
            'map': Pickled file with a dict between local parsl names, and remote taskvine names.
        """
        task_dir = "{:04d}".format(executor_task_id)
        return os.path.join(self._function_data_dir.name, task_dir, *path_components)

    def submit(self, func, resource_specification, *args, **kwargs):
        """Processes the Parsl app by its arguments and submits the function
        information to the task queue, to be executed using the TaskVine
        system. The args and kwargs are processed for input and output files to
        the Parsl app, so that the files are appropriately specified for the TaskVine task.

        Parameters
        ----------

        func : function
            Parsl app to be submitted to the TaskVine system
        resource_specification: dict
            Dictionary containing relevant info about task.
            Include information about resources of task, execution mode
            of task (out of {regular, serverless}).
        args : list
            Arguments to the Parsl app
        kwargs : dict
            Keyword arguments to the Parsl app
        """

        logger.debug(f'Got resource specification: {resource_specification}')

        # Default execution mode of apps is regular
        exec_mode = resource_specification.get('exec_mode', self.function_exec_mode)

        # Detect resources and features of a submitted Parsl app
        cores = None
        memory = None
        disk = None
        gpus = None
        priority = None
        category = None
        running_time_min = None
        if resource_specification and isinstance(resource_specification, dict):
            for k in resource_specification:
                if k == 'cores':
                    cores = resource_specification[k]
                elif k == 'memory':
                    memory = resource_specification[k]
                elif k == 'disk':
                    disk = resource_specification[k]
                elif k == 'gpus':
                    gpus = resource_specification[k]
                elif k == 'priority':
                    priority = resource_specification[k]
                elif k == 'category':
                    category = resource_specification[k]
                elif k == 'running_time_min':
                    running_time_min = resource_specification[k]

        # Assign executor task id to app
        executor_task_id = self._executor_task_counter
        self._executor_task_counter += 1

        # Create a per task directory for the function, argument, map, and result files
        os.mkdir(self._path_in_task(executor_task_id))

        input_files = []
        output_files = []

        # Determine whether to stage input files that will exist at the workers
        # Input and output files are always cached
        input_files += [self._register_file(f) for f in kwargs.get("inputs", []) if isinstance(f, File)]
        output_files += [self._register_file(f) for f in kwargs.get("outputs", []) if isinstance(f, File)]

        # Also consider any *arg that looks like a file as an input:
        input_files += [self._register_file(f) for f in args if isinstance(f, File)]

        for kwarg, maybe_file in kwargs.items():
            # Add appropriate input and output files from "stdout" and "stderr" keyword arguments
            if kwarg == "stdout" or kwarg == "stderr":
                if maybe_file:
                    output_files.append(self._std_output_to_vine(kwarg, maybe_file))
            # For any other keyword that looks like a file, assume it is an input file
            elif isinstance(maybe_file, File):
                input_files.append(self._register_file(maybe_file))

        # Create a Future object and have it be mapped from the task ID in the tasks dictionary
        fu = Future()
        fu.parsl_executor_task_id = executor_task_id
        with self._tasks_lock:
            self.tasks[str(executor_task_id)] = fu

        # Setup files to be used on a worker to execute the function
        function_file = None
        argument_file = None
        result_file = None
        map_file = None

        # Get path to files that will contain the pickled function,
        # arguments, result, and map of input and output files
        function_file = self._path_in_task(executor_task_id, "function")
        argument_file = self._path_in_task(executor_task_id, "argument")
        result_file = self._path_in_task(executor_task_id, "result")
        map_file = self._path_in_task(executor_task_id, "map")

        logger.debug("Creating executor task {} with function at: {}, argument at: {}, \
                and result to be found at: {}".format(executor_task_id, function_file, argument_file, result_file))

        # Serialize function object and arguments, separately
        self._serialize_object_to_file(function_file, func)
        args_dict = {'args': args, 'kwargs': kwargs}
        self._serialize_object_to_file(argument_file, args_dict)

        # Construct the map file of local filenames at worker
        self._construct_map_file(map_file, input_files, output_files)

        # Register a tarball containing all package dependencies for this app if instructed
        if self.manager_config.app_pack:
            env_pkg = self._prepare_package(func, self.extra_pkgs)
        else:
            env_pkg = None

        # Create message to put into the message queue
        logger.debug("Placing task {} on message queue".format(executor_task_id))

        # Put apps into their categories based on function name if enabled
        if category is None:
            category = func.__name__ if self.manager_config.autocategory else 'parsl-default'

        task_info = ParslTaskToVine(executor_id=executor_task_id,
                                    exec_mode=exec_mode,
                                    category=category,
                                    input_files=input_files,
                                    output_files=output_files,
                                    map_file=map_file,
                                    function_file=function_file,
                                    argument_file=argument_file,
                                    result_file=result_file,
                                    cores=cores,
                                    memory=memory,
                                    disk=disk,
                                    gpus=gpus,
                                    priority=priority,
                                    running_time_min=running_time_min,
                                    env_pkg=env_pkg)

        # Send ready task to manager process
        if not self._submit_process.is_alive():
            raise ExecutorError(self, "taskvine Submit Process is not alive")

        self._ready_task_queue.put_nowait(task_info)

        # Increment outstanding task counter
        with self._outstanding_tasks_lock:
            self._outstanding_tasks += 1

        # Return the future for this function, will be set by collector thread when result
        # comes back from the TaskVine manager.
        return fu

    def _construct_worker_command(self):
        worker_command = self.factory_config.worker_executable
        if self.factory_config._project_password_file:
            worker_command += ' --password {}'.format(self.factory_config._project_password_file)
        if self.factory_config.worker_options:
            worker_command += ' {}'.format(self.factory_config.worker_options)
        if self.factory_config._project_name:
            worker_command += ' -M {}'.format(self.factory_config._project_name)
        else:
            worker_command += ' {} {}'.format(self.factory_config._project_address, self.factory_config._project_port)

        logger.debug("Using worker command: {}".format(worker_command))
        return worker_command

    def _patch_providers(self):
        # Add the worker and password file to files that the provider needs to stage.
        # (Currently only for the CondorProvider)
        if isinstance(self.provider, CondorProvider):
            path_to_worker = shutil.which('vine_worker')
            self._worker_command = './' + self._worker_command
            self.provider.transfer_input_files.append(path_to_worker)
            if self.project_password_file:
                self.provider.transfer_input_files.append(self.project_password_file)

    def _serialize_object_to_file(self, path, obj):
        """Takes any object and serializes it to the file path."""
        serialized_obj = serialize(obj, buffer_threshold=1024 * 1024)
        with open(path, 'wb') as f_out:
            written = 0
            while written < len(serialized_obj):
                written += f_out.write(serialized_obj[written:])

    def _construct_map_file(self, map_file, input_files, output_files):
        """ Map local filepath of parsl files to the filenames at the execution worker.
        If using a shared filesystem, the filepath is mapped to its absolute filename.
        Otherwise, to its original relative filename. In this later case, taskvine
        recreates any directory hierarchy needed."""
        file_translation_map = {}
        for spec in itertools.chain(input_files, output_files):
            local_name = spec.parsl_name
            if self.manager_config.shared_fs:
                remote_name = os.path.abspath(local_name)
            else:
                remote_name = local_name
            file_translation_map[local_name] = remote_name
        self._serialize_object_to_file(map_file, file_translation_map)

    def _register_file(self, parsl_file):
        """Generates a tuple (parsl_file.filepath, stage, cache) to give to
        taskvine. cache is always True.
        stage is True if the file has a relative path. (i.e., not
        a URL or an absolute path)"""
        to_cache = True
        to_stage = False
        if parsl_file.scheme == 'file' or \
           (parsl_file.local_path and os.path.exists(parsl_file.local_path)):
            to_stage = not os.path.isabs(parsl_file.filepath)

        return ParslFileToVine(parsl_file.filepath, to_stage, to_cache)

    def _std_output_to_vine(self, fdname, stdfspec):
        """Find the name of the file that will contain stdout or stderr and
        return a ParslFileToVine with it. These files are never cached"""
        fname, mode = putils.get_std_fname_mode(fdname, stdfspec)
        to_stage = not os.path.isabs(fname)
        return ParslFileToVine(fname, stage=to_stage, cache=False)

    def _prepare_package(self, fn, extra_pkgs):
        """ Look at source code of apps to figure out their package depedencies
        and output a tarball containing those to send along with tasks for execution."""

        if not self._poncho_available:
            raise ExecutorError(self, 'poncho package is not available to individually pack apps.')

        fn_id = id(fn)
        fn_name = fn.__name__
        if fn_id in self.cached_envs:
            logger.debug("Skipping analysis of %s, previously got %s", fn_name, self.cached_envs[fn_id])
            return self.cached_envs[fn_id]
        source_code = inspect.getsource(fn).encode()
        pkg_dir = os.path.join(tempfile.gettempdir(), "python_package-{}".format(os.geteuid()))
        os.makedirs(pkg_dir, exist_ok=True)
        with tempfile.NamedTemporaryFile(suffix='.yaml') as spec:
            logger.info("Analyzing dependencies of %s", fn_name)
            analyze_cmdline = [self._package_analyze_script, exec_parsl_function.__file__, '-', spec.name]
            for p in extra_pkgs:
                analyze_cmdline += ["--extra-pkg", p]
            subprocess.run(analyze_cmdline, input=source_code, check=True)
            with open(spec.name, mode='rb') as f:
                spec_hash = hashlib.sha256(f.read()).hexdigest()
                logger.debug("Spec hash for %s is %s", fn_name, spec_hash)
                pkg = os.path.join(pkg_dir, "pack-{}.tar.gz".format(spec_hash))
            if os.access(pkg, os.R_OK):
                self.cached_envs[fn_id] = pkg
                logger.debug("Cached package for %s found at %s", fn_name, pkg)
                return pkg
            (fd, tarball) = tempfile.mkstemp(dir=pkg_dir, prefix='.tmp', suffix='.tar.gz')
            os.close(fd)
            logger.info("Creating dependency package for %s", fn_name)
            logger.debug("Writing deps for %s to %s", fn_name, tarball)
            subprocess.run([self._package_create_script, spec.name, tarball], stdout=subprocess.DEVNULL, check=True)
            logger.debug("Done with conda-pack; moving %s to %s", tarball, pkg)
            os.rename(tarball, pkg)
            self.cached_envs[fn_id] = pkg
            return pkg

    def initialize_scaling(self):
        """ Compose the launch command and call scale out

        Scales the workers to the appropriate nodes with provider
        """
        # Start scaling in/out
        logger.debug("Starting TaskVineExecutor with provider: %s", self.provider)
        self._worker_command = self._construct_worker_command()
        self._patch_providers()

    @property
    def outstanding(self) -> int:
        """Count the number of outstanding tasks."""
        logger.debug(f"Counted {self._outstanding_tasks} outstanding tasks")
        return self._outstanding_tasks

    @property
    def workers_per_node(self) -> Union[int, float]:
        return 1

    def shutdown(self, *args, **kwargs):
        """Shutdown the executor. Sets flag to cancel the submit process and
        collector thread, which shuts down the TaskVine system submission.
        """
        logger.debug("TaskVine shutdown started")
        self._should_stop.set()

        # Join all processes before exiting
        logger.debug("Joining on submit process")
        self._submit_process.join()
        self._submit_process.close()
        logger.debug("Joining on collector thread")
        self._collector_thread.join()
        if self.worker_launch_method == 'factory':
            logger.debug("Joining on factory process")
            self._factory_process.join()
            self._factory_process.close()

        # Shutdown multiprocessing queues
        self._ready_task_queue.close()
        self._ready_task_queue.join_thread()
        self._finished_task_queue.close()
        self._finished_task_queue.join_thread()

        logger.debug("TaskVine shutdown completed")

    @wrap_with_logs
    def _collect_taskvine_results(self):
        """Sets the values of tasks' futures completed by taskvine.
        """
        logger.debug("Starting Collector Thread")
        try:
            while not self._should_stop.is_set():
                if not self._submit_process.is_alive():
                    raise ExecutorError(self, "taskvine Submit Process is not alive")

                # Get the result message from the _finished_task_queue
                try:
                    task_report = self._finished_task_queue.get(timeout=1)
                except queue.Empty:
                    continue

                # Obtain the future from the tasks dictionary
                with self._tasks_lock:
                    future = self.tasks.pop(task_report.executor_id)

                logger.debug(f'Updating Future for Parsl Task: {task_report.executor_id}. \
                               Task {task_report.executor_id} has result_received set to {task_report.result_received}')
                if task_report.result_received:
                    try:
                        with open(task_report.result_file, 'rb') as f_in:
                            result = deserialize(f_in.read())
                    except Exception as e:
                        logger.error(f'Cannot load result from result file {task_report.result_file}. Exception: {e}')
                        ex = TaskVineTaskFailure('Cannot load result from result file', None)
                        ex.__cause__ = e
                        future.set_exception(ex)
                    else:
                        if isinstance(result, Exception):
                            ex = TaskVineTaskFailure('Task execution raises an exception', result)
                            ex.__cause__ = result
                            future.set_exception(ex)
                        else:
                            future.set_result(result)
                else:
                    # If there are no results, then the task failed according to one of
                    # taskvine modes, such as resource exhaustion.
                    ex = TaskVineTaskFailure(task_report.reason, None)
                    future.set_exception(ex)

                # decrement outstanding task counter
                with self._outstanding_tasks_lock:
                    self._outstanding_tasks -= 1
        finally:
            logger.debug(f"Marking all {self.outstanding} outstanding tasks as failed")
            logger.debug("Acquiring tasks_lock")
            with self._tasks_lock:
                logger.debug("Acquired tasks_lock")
                # set exception for tasks waiting for results that taskvine did not execute
                for fu in self.tasks.values():
                    if not fu.done():
                        fu.set_exception(TaskVineManagerFailure("taskvine executor failed to execute the task."))
        logger.debug("Exiting Collector Thread")
