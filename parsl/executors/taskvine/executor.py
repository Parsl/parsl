""" TaskVineExecutor utilizes the TaskVine distributed framework developed by the
Cooperative Computing Lab (CCL) at Notre Dame to provide a fault-tolerant,
high-throughput system for delegating Parsl tasks to thousands of remote machines
"""

# Import Python built-in libraries
import threading
import multiprocessing
import logging
import tempfile
import hashlib
import subprocess
import os
import socket
import time
import pickle
import queue
import inspect
import shutil
import itertools
import uuid
from ctypes import c_bool
from concurrent.futures import Future
from typing import List, Optional, Union

# Import Parsl constructs
import parsl.utils as putils
from parsl.utils import setproctitle
from parsl.data_provider.staging import Staging
from parsl.serialize import pack_apply_message
from parsl.data_provider.files import File
from parsl.errors import OptionalModuleMissing
from parsl.providers.base import ExecutionProvider
from parsl.providers import LocalProvider, CondorProvider
from parsl.process_loggers import wrap_with_logs
from parsl.executors.errors import ExecutorError
from parsl.executors.errors import UnsupportedFeatureError
from parsl.executors.status_handling import BlockProviderExecutor
from parsl.executors.taskvine import exec_parsl_function
from parsl.executors.taskvine.manager_config import TaskVineManagerConfig
from parsl.executors.taskvine.factory_config import TaskVineFactoryConfig
from parsl.executors.taskvine.errors import TaskVineTaskFailure
from parsl.executors.taskvine.errors import TaskVineManagerFailure
from parsl.executors.taskvine.errors import TaskVineFactoryFailure
from parsl.executors.taskvine.utils import ParslTaskToVine
from parsl.executors.taskvine.utils import VineTaskToParsl
from parsl.executors.taskvine.utils import ParslFileToVine

# Import other libraries
import typeguard

# Import TaskVine python modules
try:
    from ndcctools.taskvine import cvine
    from ndcctools.taskvine import Manager
    from ndcctools.taskvine import Factory
    from ndcctools.taskvine import Task
    from ndcctools.taskvine.cvine import VINE_ALLOCATION_MODE_MAX_THROUGHPUT
    from ndcctools.taskvine.cvine import VINE_ALLOCATION_MODE_EXHAUSTIVE_BUCKETING
    from ndcctools.taskvine.cvine import VINE_ALLOCATION_MODE_MAX
except ImportError:
    _taskvine_enabled = False
else:
    _taskvine_enabled = True

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

        use_factory: bool
            Choose to whether use either the Parsl provider or
            TaskVine factory to scale workers.
            Default is False.

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
                 use_factory: bool = False,
                 manager_config: TaskVineManagerConfig = TaskVineManagerConfig(),
                 factory_config: TaskVineFactoryConfig = TaskVineFactoryConfig(),
                 provider: ExecutionProvider = LocalProvider(init_blocks=1),
                 storage_access: Optional[List[Staging]] = None):

        # If TaskVine factory is used, disable the Parsl provider
        if use_factory:
            provider = LocalProvider(init_blocks=0, max_blocks=0)

        # Initialize the parent class with the execution provider and block error handling enabled.
        BlockProviderExecutor.__init__(self, provider=provider,
                                       block_error_handler=True)

        # Raise an exception if there's a problem importing TaskVine
        if not _taskvine_enabled:
            raise OptionalModuleMissing(['taskvine'], "TaskVineExecutor requires the taskvine module.")

        # Executor configurations
        self.label = label
        self.use_factory = use_factory
        self.manager_config = manager_config
        self.factory_config = factory_config
        self.storage_access = storage_access

        # Queue to send ready tasks from TaskVine executor process to TaskVine manager process
        self.ready_task_queue: multiprocessing.Queue = multiprocessing.Queue()

        # Queue to send finished tasks from TaskVine manager process to TaskVine executor process
        self.finished_task_queue: multiprocessing.Queue = multiprocessing.Queue()

        # Value to signal whether the manager and factory processes should stop running
        self.should_stop = multiprocessing.Value(c_bool, False)

        # TaskVine manager process
        self.submit_process = None

        # TaskVine factory process
        self.factory_process = None

        # Executor thread to collect results from TaskVine manager and set
        # tasks' futures to done status.
        self.collector_thread = None

        # track task id of submitted parsl tasks
        # task ids are incremental and start from 0
        self.executor_task_counter = 0

        # track number of tasks that are waiting/running
        self.outstanding_tasks = 0

        # Lock for threads to access self.outstanding_tasks attribute
        self.outstanding_tasks_lock = threading.Lock()

        # Threading lock to manage self.tasks dictionary object, which maps a task id
        # to its future object.
        self.tasks_lock = threading.Lock()

        # Worker command to be given to an execution provider (e.g., local or Condor)
        self.worker_command = ""

        # Path to directory that holds all tasks' data and results, only used when
        # manager's task mode is 'regular'.
        self.function_data_dir = ""

        # helper scripts to prepare package tarballs for Parsl apps
        self.package_analyze_script = shutil.which("poncho_package_analyze")
        self.package_create_script = shutil.which("poncho_package_create")

    def _get_launch_command(self, block_id):
        # Implements BlockProviderExecutor's abstract method.
        # This executor uses different terminology for worker/launch
        # commands than in htex.
        return f"PARSL_WORKER_BLOCK_ID={block_id} {self.worker_command}"

    def __synchronize_manager_factory_comm_settings(self):
        # Synchronize the communication settings between the manager and the factory
        # so the factory can direct workers to contact the manager.

        # If the manager can choose any available port (port number = 0),
        # then it must have a project name
        # so the factory can look it up. Otherwise the factory process will not know the
        # port number as it's only chosen when the TaskVine manager process is run.
        if self.manager_config.port == 0 and self.manager_config.project_name is None:
            self.manager_config.project_name = "parsl-vine-" + str(uuid.uuid4())

        # guess the host name if the project name is not given
        if not self.manager_config.project_name:
            self.manager_config.address = socket.gethostname()

        # Factory communication settings are overridden by manager communication settings.
        self.factory_config._project_port = self.manager_config.port
        self.factory_config._project_address = self.manager_config.address
        self.factory_config._project_name = self.manager_config.project_name
        self.factory_config._project_password_file = self.manager_config.project_password_file

    def __create_data_and_logging_dirs(self):
        # Create neccessary data and logging directories

        # Use the current run directory from Parsl
        run_dir = self.run_dir

        # Create directories for data and results
        self.function_data_dir = os.path.join(run_dir, self.label, "function_data")
        log_dir = os.path.join(run_dir, self.label)
        logger.debug("function data directory: {}\nlog directory: {}".format(self.function_data_dir, log_dir))
        os.makedirs(log_dir)
        os.makedirs(self.function_data_dir)

        # put TaskVine logs inside run directory of Parsl by default
        if self.manager_config.vine_log_dir is None:
            self.manager_config.vine_log_dir = log_dir
            self.factory_config.scratch_dir = log_dir

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
        submit_process_kwargs = {"ready_task_queue": self.ready_task_queue,
                                 "finished_task_queue": self.finished_task_queue,
                                 "should_stop": self.should_stop,
                                 "manager_config": self.manager_config}
        self.submit_process = multiprocessing.Process(target=_taskvine_submit_wait,
                                                      name="TaskVine-Submit-Process",
                                                      kwargs=submit_process_kwargs)

        # Create a process to run the TaskVine factory if enabled.
        if self.use_factory:
            factory_process_kwargs = {"should_stop": self.should_stop,
                                      "factory_config": self.factory_config}
            self.factory_process = multiprocessing.Process(target=_taskvine_factory,
                                                           name="TaskVine-Factory-Process",
                                                           kwargs=factory_process_kwargs)

        # Run thread to collect results and set tasks' futures.
        self.collector_thread = threading.Thread(target=self._collect_taskvine_results,
                                                 name="TaskVine-Collector-Thread")
        # Interpreter can exit without waiting for this thread.
        self.collector_thread.daemon = True

        # Begin work
        self.submit_process.start()
        self.collector_thread.start()

        # Run worker scaler either with Parsl provider or TaskVine factory
        if self.use_factory:
            self.factory_process.start()
        else:
            self.initialize_scaling()
        logger.debug("All components in TaskVineExecutor started")

    def _path_in_task(self, executor_task_id, *path_components):
        """
        Only used when task mode is `regular`.
        Returns a filename fixed and specific to a task.
        It is used for the following filename's:
            (not given): The subdirectory per task that contains function, result, etc.
            'function': Pickled file that contains the function to be executed.
            'result': Pickled file that (will) contain the result of the function.
            'map': Pickled file with a dict between local parsl names, and remote taskvine names.
        """
        task_dir = "{:04d}".format(executor_task_id)
        return os.path.join(self.function_data_dir, task_dir, *path_components)

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
            of task (out of {regular, python, serverless}), and which app
            type this function was submitted as (out of {python, bash}).
        args : list
            Arguments to the Parsl app
        kwargs : dict
            Keyword arguments to the Parsl app
        """

        # Default execution mode of apps is regular (using TaskVineExecutor serialization and execution mode)
        exec_mode = resource_specification.get('exec_mode', 'regular')

        logger.debug(f'Got resource specification: {resource_specification}')

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
        executor_task_id = self.executor_task_counter
        self.executor_task_counter += 1

        # Create a per task directory for the function, map, and result files
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
        with self.tasks_lock:
            self.tasks[str(executor_task_id)] = fu

        logger.debug("Creating task {} for function {} of type {} with args {}".format(executor_task_id, func, type(func), args))

        function_file = None
        result_file = None
        map_file = None
        # Use executor's serialization method if app mode is 'regular'
        if exec_mode == 'regular':
            # Get path to files that will contain the pickled function, result, and map of input and output files
            function_file = self._path_in_task(executor_task_id, "function")
            result_file = self._path_in_task(executor_task_id, "result")
            map_file = self._path_in_task(executor_task_id, "map")

            logger.debug("Creating executor task {} with function at: {} and result to be found at: {}".format(executor_task_id, function_file, result_file))

            # Pickle the result into object to pass into message buffer
            self._serialize_function(function_file, func, args, kwargs)

            # Construct the map file of local filenames at worker
            self._construct_map_file(map_file, input_files, output_files)

        # Register a tarball containing all package dependencies for this app if instructed
        if self.manager_config.app_pack:
            env_pkg = self._prepare_package(func, self.extra_pkgs)
        else:
            env_pkg = None

        if not self.submit_process.is_alive():
            raise ExecutorError(self, "taskvine Submit Process is not alive")

        # Create message to put into the message queue
        logger.debug("Placing task {} on message queue".format(executor_task_id))

        # Put apps into their categories based on function name if enabled
        if category is None:
            category = func.__name__ if self.manager_config.autocategory else 'parsl-default'

        # support for python and serverless exec mode delayed
        if exec_mode == 'python' or exec_mode == 'serverless':
            raise UnsupportedFeatureError(f'Execution mode {exec_mode} is not currently supported.')
        task_info = ParslTaskToVine(executor_id=executor_task_id,
                                    exec_mode=exec_mode,
                                    func_args=args,
                                    func_kwargs=kwargs,
                                    category=category,
                                    input_files=input_files,
                                    output_files=output_files,
                                    map_file=map_file,
                                    function_file=function_file,
                                    result_file=result_file,
                                    cores=cores,
                                    memory=memory,
                                    disk=disk,
                                    gpus=gpus,
                                    priority=priority,
                                    running_time_min=running_time_min,
                                    env_pkg=env_pkg)

        # Send ready task to manager process
        self.ready_task_queue.put_nowait(task_info)

        # Increment outstanding task counter
        with self.outstanding_tasks_lock:
            self.outstanding_tasks += 1

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
            self.worker_command = './' + self.worker_command
            self.provider.transfer_input_files.append(path_to_worker)
            if self.project_password_file:
                self.provider.transfer_input_files.append(self.project_password_file)

    def _serialize_function(self, fn_path, parsl_fn, parsl_fn_args, parsl_fn_kwargs):
        """Takes the function application parsl_fn(*parsl_fn_args, **parsl_fn_kwargs)
        and serializes it to the file fn_path."""
        function_info = {"byte code": pack_apply_message(parsl_fn, parsl_fn_args, parsl_fn_kwargs,
                                                         buffer_threshold=1024 * 1024)}

        with open(fn_path, "wb") as f_out:
            pickle.dump(function_info, f_out)

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
        with open(map_file, "wb") as f_out:
            pickle.dump(file_translation_map, f_out)

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
            analyze_cmdline = [self.package_analyze_script, exec_parsl_function.__file__, '-', spec.name]
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
            subprocess.run([self.package_create_script, spec.name, tarball], stdout=subprocess.DEVNULL, check=True)
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
        self.worker_command = self._construct_worker_command()
        self._patch_providers()

        if hasattr(self.provider, 'init_blocks'):
            try:
                self.scale_out(blocks=self.provider.init_blocks)
            except Exception as e:
                logger.error("Initial block scaling out failed: {}".format(e))
                raise e

    @property
    def outstanding(self) -> int:
        """Count the number of outstanding tasks."""
        logger.debug(f"Counted {self.outstanding_tasks} outstanding tasks")
        return self.outstanding_tasks

    @property
    def workers_per_node(self) -> Union[int, float]:
        return 1

    def scale_in(self, count):
        """Scale in method. Cancel a given number of blocks
        """
        # Obtain list of blocks to kill
        to_kill = list(self.blocks.keys())[:count]
        kill_ids = [self.blocks[block] for block in to_kill]

        # Cancel the blocks provisioned
        if self.provider:
            self.provider.cancel(kill_ids)
        else:
            logger.error("No execution provider available to scale")

    def shutdown(self, *args, **kwargs):
        """Shutdown the executor. Sets flag to cancel the submit process and
        collector thread, which shuts down the TaskVine system submission.
        """
        logger.debug("TaskVine shutdown started")
        self.should_stop.value = True

        # Remove the workers that are still going
        kill_ids = [self.blocks[block] for block in self.blocks.keys()]
        if self.provider:
            logger.debug("Cancelling blocks")
            self.provider.cancel(kill_ids)

        # Join all processes before exiting
        logger.debug("Joining on submit process")
        self.submit_process.join()
        logger.debug("Joining on collector thread")
        self.collector_thread.join()
        if self.use_factory:
            logger.debug("Joining on factory process")
            self.factory_process.join()

        logger.debug("TaskVine shutdown completed")
        return True

    @wrap_with_logs
    def _collect_taskvine_results(self):
        """Sets the values of tasks' futures completed by taskvine.
        """
        logger.debug("Starting Collector Thread")
        try:
            while not self.should_stop.value:
                if not self.submit_process.is_alive():
                    raise ExecutorError(self, "taskvine Submit Process is not alive")

                # Get the result message from the finished_task_queue
                try:
                    task_report = self.finished_task_queue.get(timeout=1)
                except queue.Empty:
                    continue

                # Obtain the future from the tasks dictionary
                with self.tasks_lock:
                    future = self.tasks.pop(task_report.executor_id)

                logger.debug("Updating Future for Parsl Task {}".format(task_report.executor_id))
                logger.debug(f'task {task_report.executor_id} has result_received set to {task_report.result_received} and result to {task_report.result}')
                if task_report.result_received:
                    future.set_result(task_report.result)
                else:
                    # If there are no results, then the task failed according to one of
                    # taskvine modes, such as resource exhaustion.
                    future.set_exception(TaskVineTaskFailure(task_report.reason, task_report.result))

                # decrement outstanding task counter
                with self.outstanding_tasks_lock:
                    self.outstanding_tasks -= 1
        finally:
            logger.debug(f"Marking all {self.outstanding} outstanding tasks as failed")
            logger.debug("Acquiring tasks_lock")
            with self.tasks_lock:
                logger.debug("Acquired tasks_lock")
                # set exception for tasks waiting for results that taskvine did not execute
                for fu in self.tasks.values():
                    if not fu.done():
                        fu.set_exception(TaskVineManagerFailure("taskvine executor failed to execute the task."))
        logger.debug("Exiting Collector Thread")


@wrap_with_logs
def _taskvine_submit_wait(ready_task_queue=None,
                          finished_task_queue=None,
                          should_stop=None,
                          manager_config=None
                          ):
    """Process to handle Parsl app submissions to the TaskVine objects.
    Takes in Parsl functions submitted using submit(), and creates a
    TaskVine task with the appropriate specifications, which is then
    submitted to TaskVine. After tasks are completed, processes the
    exit status and exit code of the task, and sends results to the
    TaskVine collector thread.
    To avoid python's global interpreter lock with taskvine's wait, this
    function should be launched as a process, not as a lightweight thread. This
    means that any communication should be done using the multiprocessing
    module capabilities, rather than shared memory.
    """
    logger.debug("Starting TaskVine Submit/Wait Process")
    setproctitle("parsl: TaskVine submit/wait")

    # Enable debugging flags and create logging file
    if manager_config.vine_log_dir is not None:
        logger.debug("Setting debugging flags and creating logging file at {}".format(manager_config.vine_log_dir))

    # Create TaskVine queue object
    logger.debug("Creating TaskVine Object")
    try:
        logger.debug("Listening on port {}".format(manager_config.port))
        m = Manager(port=manager_config.port,
                    name=manager_config.project_name,
                    run_info_path=manager_config.vine_log_dir)
    except Exception as e:
        logger.error("Unable to create TaskVine object: {}".format(e))
        raise e

    # Specify TaskVine manager attributes
    if manager_config.project_password_file:
        m.set_password_file(manager_config.project_password_file)

    # Autolabeling resources require monitoring to be enabled
    if manager_config.autolabel:
        m.enable_monitoring()
        if manager_config.autolabel_window is not None:
            m.tune('category-steady-n-tasks', manager_config.autolabel_window)

    # Specify number of workers to wait for before sending the first task
    if manager_config.wait_for_workers:
        m.tune("wait-for-workers", manager_config.wait_for_workers)

    # Enable peer transfer feature between workers if specified
    if manager_config.enable_peer_transfers:
        m.enable_peer_transfers()

    # Get parent pid, useful to shutdown this process when its parent, the taskvine
    # executor process, exits.
    orig_ppid = os.getppid()

    result_file_of_task_id = {}  # Mapping executor task id -> result file for active regular tasks.

    poncho_env_to_file = {}  # Mapping poncho_env file to File object in TaskVine

    # Mapping of parsl local file name to TaskVine File object
    # dict[str] -> vine File object
    parsl_file_name_to_vine_file = {}

    # Mapping of tasks from vine id to parsl id
    # Dict[str] -> str
    vine_id_to_executor_task_id = {}

    # Find poncho run script to activate an environment tarball
    poncho_run_script = shutil.which("poncho_package_run")

    # Declare helper scripts as cache-able and peer-transferable
    package_run_script_file = m.declare_file(poncho_run_script, cache=True, peer_transfer=True)
    exec_parsl_function_file = m.declare_file(exec_parsl_function.__file__, cache=True, peer_transfer=True)

    logger.debug("Entering main loop of TaskVine manager")

    while not should_stop.value:
        # Monitor the task queue
        ppid = os.getppid()
        if ppid != orig_ppid:
            logger.debug("new Process")
            break

        # Submit tasks
        while ready_task_queue.qsize() > 0 and not should_stop.value:
            # Obtain task from ready_task_queue
            try:
                task = ready_task_queue.get(timeout=1)
                logger.debug("Removing executor task from queue")
            except queue.Empty:
                logger.debug("Queue is empty")
                continue
            if task.exec_mode == 'regular':
                # Create command string
                launch_cmd = "python3 exec_parsl_function.py {mapping} {function} {result}"
                if manager_config.init_command != '':
                    launch_cmd = "{init_cmd};" + launch_cmd
                command_str = launch_cmd.format(init_cmd=manager_config.init_command,
                                                mapping=os.path.basename(task.map_file),
                                                function=os.path.basename(task.function_file),
                                                result=os.path.basename(task.result_file))
                logger.debug("Sending executor task {} (mode: regular) with command: {}".format(task.executor_id, command_str))
                try:
                    t = Task(command_str)
                except Exception as e:
                    logger.error("Unable to create executor task (mode:regular): {}".format(e))
                    finished_task_queue.put_nowait(VineTaskToParsl(executor_id=task.executor_id,
                                                                   result_received=False,
                                                                   result=None,
                                                                   reason="task could not be created by taskvine",
                                                                   status=-1))
                    continue
            else:
                raise Exception(f'Unrecognized task mode {task.exec_mode}. Exiting...')

            # Add environment file to the task if possible
            # Prioritize local poncho environment over global poncho environment
            # (local: use app_pack, global: use env_pack)
            poncho_env_file = None

            # check if env_pack is specified
            if manager_config.env_pack is not None:
                # check if the environment file is not already created
                if manager_config.env_pack not in poncho_env_to_file:
                    # if the environment is already packaged as a tarball, then add the file
                    # otherwise it is an environment name or path, so create a poncho tarball then add it
                    if not manager_config.env_pack.endswith('.tar.gz'):
                        env_tarball = str(uuid.uuid4()) + '.tar.gz'
                        subprocess.run([poncho_run_script, manager_config.env_pack, env_tarball], stdout=subprocess.DEVNULL, check=True)
                    poncho_env_file = m.declare_poncho(manager_config.env_pack, cache=True, peer_transfer=True)
                    poncho_env_to_file[manager_config.env_pack] = poncho_env_file
                else:
                    poncho_env_file = poncho_env_to_file[manager_config.env_pack]

            # check if app_pack is used, override if possible
            if task.env_pkg is not None:
                if task.env_pkg not in poncho_env_to_file:
                    poncho_env_file = m.declare_poncho(task.env_pkg, cache=True, peer_transfer=True)
                    poncho_env_to_file[task.env_pkg] = poncho_env_file
                else:
                    poncho_env_file = poncho_env_to_file[task.env_pkg]

            # Add environment to the task
            if poncho_env_file is not None:
                t.add_environment(poncho_env_file)
                t.add_input(package_run_script_file, "poncho_package_run")

            t.set_category(task.category)
            if manager_config.autolabel:
                if manager_config.autolabel_algorithm == 'max-xput':
                    m.set_category_mode(task.category, VINE_ALLOCATION_MODE_MAX_THROUGHPUT)
                elif manager_config.autolabel_algorithm == 'bucketing':
                    m.set_category_mode(task.category, VINE_ALLOCATION_MODE_EXHAUSTIVE_BUCKETING)
                elif manager_config.autolabel_algorithm == 'max':
                    m.set_category_mode(task.category, VINE_ALLOCATION_MODE_MAX)
                else:
                    logger.warning(f'Unrecognized autolabeling algorithm named {manager_config.autolabel_algorithm} for taskvine manager.')
                    raise Exception(f'Unrecognized autolabeling algorithm named {manager_config.autolabel_algorithm} for taskvine manager.')

            if task.cores is not None:
                t.set_cores(task.cores)
            if task.memory is not None:
                t.set_memory(task.memory)
            if task.disk is not None:
                t.set_disk(task.disk)
            if task.gpus is not None:
                t.set_gpus(task.gpus)
            if task.priority is not None:
                t.set_priority(task.priority)
            if task.running_time_min is not None:
                t.set_time_min(task.running_time_min)

            if manager_config.max_retries is not None:
                logger.debug(f"Specifying max_retries {manager_config.max_retries}")
                t.set_retries(manager_config.max_retries)
            else:
                logger.debug("Not specifying max_retries")

            # Specify environment variables for the task
            if manager_config.env_vars is not None:
                for var in manager_config.env_vars:
                    t.set_env_var(str(var), str(manager_config.env_vars[var]))

            if task.exec_mode == 'regular':
                # Add helper files that execute parsl functions on remote nodes
                # only needed for tasks with 'regular' mode
                t.add_input(exec_parsl_function_file, "exec_parsl_function.py")

                # Declare and add task-specific function, data, and result files to task
                task_function_file = m.declare_file(task.function_file, cache=False, peer_transfer=False)
                t.add_input(task_function_file, "function")

                task_map_file = m.declare_file(task.map_file, cache=False, peer_transfer=False)
                t.add_input(task_map_file, "map")

                task_result_file = m.declare_file(task.result_file, cache=False, peer_transfer=False)
                t.add_output(task_result_file, "result")

                result_file_of_task_id[str(task.executor_id)] = task.result_file

            logger.debug("Executor task id: {}".format(task.executor_id))

            # Specify input/output files that need to be staged.
            # Absolute paths are assumed to be in shared filesystem, and thus
            # not staged by taskvine.
            # Files that share the same local path are assumed to be the same
            # and thus use the same Vine File object if detected.
            if not manager_config.shared_fs:
                for spec in task.input_files:
                    if spec.stage:
                        if spec.parsl_name in parsl_file_name_to_vine_file:
                            task_in_file = parsl_file_name_to_vine_file[spec.parsl_name]
                        else:
                            task_in_file = m.declare_file(spec.parsl_name, cache=spec.cache, peer_transfer=True)
                            parsl_file_name_to_vine_file[spec.parsl_name] = task_in_file
                        t.add_input(task_in_file, spec.parsl_name)

                for spec in task.output_files:
                    if spec.stage:
                        if spec.parsl_name in parsl_file_name_to_vine_file:
                            task_out_file = parsl_file_name_to_vine_file[spec.parsl_name]
                        else:
                            task_out_file = m.declare_file(spec.parsl_name, cache=spec.cache, peer_transfer=True)
                        t.add_output(task_out_file, spec.parsl_name)

            # Submit the task to the TaskVine object
            logger.debug("Submitting executor task {}, {} to TaskVine".format(task.executor_id, t))
            try:
                vine_id = m.submit(t)
                logger.debug("Submitted executor task {} to TaskVine".format(task.executor_id))
                vine_id_to_executor_task_id[str(vine_id)] = str(task.executor_id), task.exec_mode
            except Exception as e:
                logger.error("Unable to submit task to taskvine: {}".format(e))
                finished_task_queue.put_nowait(VineTaskToParsl(executor_id=task.executor_id,
                                                               result_received=False,
                                                               result=None,
                                                               reason="task could not be submited to taskvine",
                                                               status=-1))
                continue

            logger.debug("Executor task {} submitted as TaskVine task with id {}".format(task.executor_id, vine_id))

        # If the queue is not empty wait on the TaskVine queue for a task
        task_found = True
        if not m.empty():
            while task_found and not should_stop.value:
                # Obtain the task from the queue
                t = m.wait(1)
                if t is None:
                    task_found = False
                    continue
                logger.debug('Found a task!')
                executor_task_id = vine_id_to_executor_task_id[str(t.id)][0]
                exec_mode_of_task = vine_id_to_executor_task_id[str(t.id)][1]
                vine_id_to_executor_task_id.pop(str(t.id))
                # When a task is found
                if exec_mode_of_task == 'regular':
                    result_file = result_file_of_task_id.pop(executor_task_id)

                    logger.debug(f"completed executor task info: {executor_task_id}, {t.category}, {t.command}, {t.std_output}")

                    # A tasks completes 'succesfully' if it has result file,
                    # and it can be loaded. This may mean that the 'success' is
                    # an exception.
                    logger.debug("Looking for result in {}".format(result_file))
                    try:
                        with open(result_file, "rb") as f_in:
                            result = pickle.load(f_in)
                        logger.debug("Found result in {}".format(result_file))
                        finished_task_queue.put_nowait(VineTaskToParsl(executor_id=executor_task_id,
                                                                       result_received=True,
                                                                       result=result,
                                                                       reason=None,
                                                                       status=t.exit_code))
                    # If a result file could not be generated, explain the
                    # failure according to taskvine error codes. We generate
                    # an exception and wrap it with RemoteExceptionWrapper, to
                    # match the positive case.
                    except Exception as e:
                        reason = _explain_taskvine_result(t)
                        logger.debug("Did not find result in {}".format(result_file))
                        logger.debug("Wrapper Script status: {}\nTaskVine Status: {}"
                                     .format(t.exit_code, t.result))
                        logger.debug("Task with executor id {} / vine id {} failed because:\n{}"
                                     .format(executor_task_id, t.id, reason))
                        finished_task_queue.put_nowait(VineTaskToParsl(executor_id=executor_task_id,
                                                                       result_received=False,
                                                                       result=e,
                                                                       reason=reason,
                                                                       status=t.exit_code))
                else:
                    raise Exception(f'Unknown exec mode for executor task {executor_task_id}: {exec_mode_of_task}.')

    logger.debug("Exiting TaskVine Monitoring Process")
    return 0


def _explain_taskvine_result(vine_task):
    """Returns a string with the reason why a task failed according to taskvine."""

    vine_result = vine_task.result

    reason = "taskvine result: "
    if vine_result == cvine.VINE_RESULT_SUCCESS:
        reason += "succesful execution with exit code {}".format(vine_task.return_status)
    elif vine_result == cvine.VINE_RESULT_OUTPUT_MISSING:
        reason += "The result file was not transfered from the worker.\n"
        reason += "This usually means that there is a problem with the python setup,\n"
        reason += "or the wrapper that executes the function."
        reason += "\nTrace:\n" + str(vine_task.output)
    elif vine_result == cvine.VINE_RESULT_INPUT_MISSING:
        reason += "missing input file"
    elif vine_result == cvine.VINE_RESULT_STDOUT_MISSING:
        reason += "stdout has been truncated"
    elif vine_result == cvine.VINE_RESULT_SIGNAL:
        reason += "task terminated with a signal"
    elif vine_result == cvine.VINE_RESULT_RESOURCE_EXHAUSTION:
        reason += "task used more resources than requested"
    elif vine_result == cvine.VINE_RESULT_MAX_END_TIME:
        reason += "task ran past the specified end time"
    elif vine_result == cvine.VINE_RESULT_UNKNOWN:
        reason += "result could not be classified"
    elif vine_result == cvine.VINE_RESULT_FORSAKEN:
        reason += "task failed, but not a task error"
    elif vine_result == cvine.VINE_RESULT_MAX_RETRIES:
        reason += "unable to complete after specified number of retries"
    elif vine_result == cvine.VINE_RESULT_MAX_WALL_TIME:
        reason += "task ran for more than the specified time"
    elif vine_result == cvine.VINE_RESULT_RMONITOR_ERROR:
        reason += "task failed because the monitor did not produce an output"
    elif vine_result == cvine.VINE_RESULT_OUTPUT_TRANSFER_ERROR:
        reason += "task failed because output transfer fails"
    elif vine_result == cvine.VINE_RESULT_FIXED_LOCATION_MISSING:
        reason += "task failed because no worker could satisfy the fixed \n"
        reason += "location input file requirements"
    else:
        reason += "unable to process TaskVine system failure"
    return reason


@wrap_with_logs
def _taskvine_factory(should_stop, factory_config):
    logger.debug("Starting TaskVine factory process")

    try:
        # create the factory according to the project name if given
        if factory_config._project_name:
            factory = Factory(batch_type=factory_config.batch_type,
                              manager_name=factory_config._project_name,
                              )
        else:
            factory = Factory(batch_type=factory_config.batch_type,
                              manager_host_port=f"{factory_config._project_address}:{factory_config._project_port}",
                             )
    except Exception as e:
        raise TaskVineFactoryFailure(f'Cannot create factory with exception {e}')

    # Set attributes of this factory
    if factory_config._project_password_file:
        factory.password = factory_config._project_password_file
    factory.factory_timeout = factory_config.factory_timeout
    factory.scratch_dir = factory_config.scratch_dir
    factory.min_workers = factory_config.min_workers
    factory.max_workers = factory_config.max_workers
    factory.workers_per_cycle = factory_config.workers_per_cycle

    if factory_config.worker_options:
        factory.extra_options = factory_config.worker_options
    factory.timeout = factory_config.worker_timeout
    if factory_config.cores:
        factory.cores = factory_config.cores
    if factory_config.gpus:
        factory.gpus = factory_config.gpus
    if factory_config.memory:
        factory.memory = factory_config.memory
    if factory_config.disk:
        factory.disk = factory_config.disk
    if factory_config.python_env:
        factory.python_env = factory_config.python_env

    if factory_config.condor_requirements:
        factory.condor_requirements = factory_config.condor_requirements
    if factory_config.batch_options:
        factory.batch_options = factory_config.batch_options

    # setup factory context and sleep for a second in every loop to
    # avoid wasting CPU
    with factory:
        while not should_stop.value:
            time.sleep(1)

    logger.debug("Exiting TaskVine factory process")
    return 0
