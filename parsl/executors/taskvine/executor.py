""" TaskVineExecutor utilizes the TaskVine distributed framework developed by the
Cooperative Computing Lab (CCL) at Notre Dame to provide a fault-tolerant,
high-throughput system for delegating Parsl tasks to thousands of remote machines
"""

import threading
import multiprocessing
import logging
from concurrent.futures import Future
from ctypes import c_bool

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

from parsl.serialize import pack_apply_message
import parsl.utils as putils
from parsl.executors.errors import ExecutorError
from parsl.data_provider.files import File
from parsl.errors import OptionalModuleMissing
from parsl.executors.status_handling import BlockProviderExecutor
from parsl.providers.base import ExecutionProvider
from parsl.providers import LocalProvider, CondorProvider
from parsl.executors.taskvine import exec_parsl_function
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

import typeguard
from typing import Dict, List, Optional, Union
from parsl.data_provider.staging import Staging

from .errors import TaskVineTaskFailure
from .errors import TaskVineFailure

from collections import namedtuple

try:
    import taskvine as vine
    from taskvine import Manager
    from taskvine import Task
    from taskvine import VINE_DEFAULT_PORT
    from taskvine import VINE_ALLOCATION_MODE_MAX_THROUGHPUT
except ImportError:
    _taskvine_enabled = False
    taskvine_DEFAULT_PORT = 0
else:
    _taskvine_enabled = True

package_analyze_script = shutil.which("python_package_analyze")
package_create_script = shutil.which("python_package_create")
package_run_script = shutil.which("python_package_run")

logger = logging.getLogger(__name__)


# Support structure to communicate parsl tasks to the taskvine submit thread.
ParslTaskToVine = namedtuple('ParslTaskToVine',
                             'id category cores memory disk gpus priority running_time_min env_pkg map_file function_file result_file input_files output_files')

# Support structure to communicate final status of taskvine tasks to parsl
# result is only valid if result_received is True
# reason and status are only valid if result_received is False
VineTaskToParsl = namedtuple('VineTaskToParsl', 'id result_received result reason status')

# Support structure to report parsl filenames to taskvine.
# parsl_name is the local_name or filepath attribute of a parsl file object.
# stage tells whether the file should be copied by taskvine to the workers.
# cache tells whether the file should be cached at workers. Only valid if stage == True
ParslFileToVine = namedtuple('ParslFileToVine', 'parsl_name stage cache')


class TaskVineExecutor(BlockProviderExecutor, putils.RepresentationMixin):
    """Executor to use TaskVine batch system

    The TaskVineExecutor system utilizes the TaskVine framework to
    efficiently delegate Parsl apps to remote machines in clusters and
    grids using a fault-tolerant system. Users can run the
    vine_worker program on remote machines to connect to the
    TaskVineExecutor, and Parsl apps will then be sent out to these
    machines for execution and retrieval.


    Parameters
    ----------

        label: str
            A human readable label for the executor, unique
            with respect to other TaskVine master programs.
            Default is "TaskVineExecutor".

        working_dir: str
            Location for Parsl to perform app delegation to the TaskVine
            system. Defaults to current directory.

        project_name: str
            If a project_name is given, then TaskVine will periodically
            report its status and performance back to the global TaskVine catalog,
            which can be viewed here:  http://ccl.cse.nd.edu/software/taskvine/status
            Default is None.  Overrides address.

        project_password_file: str
            Optional password file for the taskvine project. Default is None.

        address: str
            The ip to contact this taskvine master process.
            If not given, uses the address of the current machine as returned
            by socket.gethostname().
            Ignored if project_name is specified.

        port: int
            TCP port on Parsl submission machine for TaskVine workers
            to connect to. Workers will specify this port number when
            trying to connect to Parsl. Default is 9123.

        env: dict{str}
            Dictionary that contains the environmental variables that
            need to be set on the TaskVine worker machine.

        shared_fs: bool
            Define if working in a shared file system or not. If Parsl
            and the TaskVine workers are on a shared file system, TaskVine
            does not need to transfer and rename files for execution.
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
            Default is False. Set to True if pack is True

        pack: bool
            Use conda-pack to prepare a self-contained Python evironment for
            each task. This greatly increases task latency, but does not
            require a common environment or shared FS on execution nodes.
            Implies source=True.

        extra_pkgs: list
            List of extra pip/conda package names to include when packing
            the environment. This may be useful if the app executes other
            (possibly non-Python) programs provided via pip or conda.
            Scanning the app source for imports would not detect these
            dependencies, so they need to be manually specified.

        autolabel: bool
            Use the Resource Monitor to automatically determine resource
            labels based on observed task behavior.

        autolabel_window: int
            Set the number of tasks considered for autolabeling. TaskVine
            will wait for a series of N tasks with steady resource
            requirements before making a decision on labels. Increasing
            this parameter will reduce the number of failed tasks due to
            resource exhaustion when autolabeling, at the cost of increased
            resources spent collecting stats.

        autocategory: bool
            Place each app in its own category by default. If all
            invocations of an app have similar performance characteristics,
            this will provide a reasonable set of categories automatically.

        max_retries: Optional[int]
            Set the number of retries that TaskVine will make when a task
            fails. This is distinct from Parsl level retries configured in
            parsl.config.Config. Set to None to allow TaskVine to retry
            tasks forever. By default, this is set to 1, so that all retries
            will be managed by Parsl.

        init_command: str
            Command line to run before executing a task in a worker.
            Default is ''.

        worker_options: str
            Extra options passed to vine_worker. Default is ''.

        worker_executable: str
            The command used to invoke vine_worker. This can be used
            when the worker needs to be wrapped inside some other command
            (for example, to run the worker inside a container). Default is
            'vine_worker'.

        function_dir: str
            The directory where serialized function invocations are placed
            to be sent to workers. If undefined, this defaults to a directory
            under runinfo/. If shared_filesystem=True, then this directory
            must be visible from both the submitting side and workers.
    """

    radio_mode = "filesystem"

    @typeguard.typechecked
    def __init__(self,
                 label: str = "TaskVineExecutor",
                 provider: ExecutionProvider = LocalProvider(),
                 working_dir: str = ".",
                 project_name: Optional[str] = None,
                 project_password_file: Optional[str] = None,
                 address: Optional[str] = None,
                 port: int = VINE_DEFAULT_PORT,
                 env: Optional[Dict] = None,
                 shared_fs: bool = False,
                 storage_access: Optional[List[Staging]] = None,
                 use_cache: bool = False,
                 source: bool = False,
                 pack: bool = False,
                 extra_pkgs: Optional[List[str]] = None,
                 autolabel: bool = False,
                 autolabel_window: int = 1,
                 autocategory: bool = True,
                 max_retries: Optional[int] = 1,
                 init_command: str = "",
                 worker_options: str = "",
                 full_debug: bool = True,
                 worker_executable: str = 'vine_worker',
                 function_dir: Optional[str] = None):
        BlockProviderExecutor.__init__(self, provider=provider,
                                       block_error_handler=True)
        if not _taskvine_enabled:
            raise OptionalModuleMissing(['taskvine'], "TaskVineExecutor requires the taskvine module.")

        self.label = label
        self.task_queue = multiprocessing.Queue()  # type: multiprocessing.Queue
        self.collector_queue = multiprocessing.Queue()  # type: multiprocessing.Queue
        self.blocks = {}  # type: Dict[str, str]
        self.address = address
        self.port = port
        self.task_counter = -1
        self.project_name = project_name
        self.project_password_file = project_password_file
        self.env = env
        self.init_command = init_command
        self.shared_fs = shared_fs
        self.storage_access = storage_access
        self.use_cache = use_cache
        self.working_dir = working_dir
        self.registered_files = set()  # type: Set[str]
        self.full_debug = full_debug
        self.source = True if pack else source
        self.pack = pack
        self.extra_pkgs = extra_pkgs or []
        self.autolabel = autolabel
        self.autolabel_window = autolabel_window
        self.autocategory = autocategory
        self.max_retries = max_retries
        self.should_stop = multiprocessing.Value(c_bool, False)
        self.cached_envs = {}  # type: Dict[int, str]
        self.worker_options = worker_options
        self.worker_executable = worker_executable
        self.function_dir = function_dir

        if not self.address:
            self.address = socket.gethostname()

        if self.project_password_file is not None and not os.path.exists(self.project_password_file):
            raise TaskVineFailure('Could not find password file: {}'.format(self.project_password_file))

        if self.project_password_file is not None:
            if os.path.exists(self.project_password_file) is False:
                logger.debug("Password File does not exist, no file used")
                self.project_password_file = None

        # Build foundations of the launch command
        self.launch_cmd = ("{package_prefix}python3 exec_parsl_function.py {mapping} {function} {result}")
        if self.init_command != "":
            self.launch_cmd = self.init_command + "; " + self.launch_cmd

    def _get_launch_command(self, block_id):
        # this executor uses different terminology for worker/launch
        # commands than in htex
        return f"PARSL_WORKER_BLOCK_ID={block_id} {self.worker_command}"

    def start(self):
        """Create submit process and collector thread to create, send, and
        retrieve Parsl tasks within the TaskVine system.
        """
        self.tasks_lock = threading.Lock()

        # Create directories for data and results
        if not self.function_dir:
            self.function_data_dir = os.path.join(self.run_dir, self.label, "function_data")
        else:
            tp = str(time.time())
            tx = os.path.join(self.function_dir, tp)
            os.makedirs(tx)
            self.function_data_dir = os.path.join(self.function_dir, tp, self.label, "function_data")
        self.package_dir = os.path.join(self.run_dir, self.label, "package_data")
        self.vine_log_dir = os.path.join(self.run_dir, self.label)
        logger.debug("function data directory: {}\nlog directory: {}".format(self.function_data_dir, self.vine_log_dir))
        os.makedirs(self.vine_log_dir)
        os.makedirs(self.function_data_dir)
        os.makedirs(self.package_dir)

        logger.debug("Starting TaskVineExecutor")

        # Create a Process to perform TaskVine submissions
        submit_process_kwargs = {"task_queue": self.task_queue,
                                 "launch_cmd": self.launch_cmd,
                                 "data_dir": self.function_data_dir,
                                 "collector_queue": self.collector_queue,
                                 "full": self.full_debug,
                                 "shared_fs": self.shared_fs,
                                 "autolabel": self.autolabel,
                                 "autolabel_window": self.autolabel_window,
                                 "autocategory": self.autocategory,
                                 "max_retries": self.max_retries,
                                 "should_stop": self.should_stop,
                                 "port": self.port,
                                 "vine_log_dir": self.vine_log_dir,
                                 "project_password_file": self.project_password_file,
                                 "project_name": self.project_name}
        self.submit_process = multiprocessing.Process(target=_taskvine_submit_wait,
                                                      name="TaskVine-Submit-Process",
                                                      kwargs=submit_process_kwargs)

        self.collector_thread = threading.Thread(target=self._collect_taskvine_results,
                                                 name="TaskVine-collector-thread")
        self.collector_thread.daemon = True

        # Begin both processes
        self.submit_process.start()
        self.collector_thread.start()

        # Initialize scaling for the provider
        self.initialize_scaling()

    def _path_in_task(self, task_id, *path_components):
        """Returns a filename specific to a task.
        It is used for the following filename's:
            (not given): The subdirectory per task that contains function, result, etc.
            'function': Pickled file that contains the function to be executed.
            'result': Pickled file that (will) contain the result of the function.
            'map': Pickled file with a dict between local parsl names, and remote taskvine names.
        """
        task_dir = "{:04d}".format(task_id)
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
        args : list
            Arguments to the Parsl app
        kwargs : dict
            Keyword arguments to the Parsl app
        """
        cores = None
        memory = None
        disk = None
        gpus = None
        priority = None
        category = None
        running_time_min = None
        if resource_specification and isinstance(resource_specification, dict):
            logger.debug("Got resource specification: {}".format(resource_specification))

            required_resource_types = set(['cores', 'memory', 'disk'])
            acceptable_resource_types = set(['cores', 'memory', 'disk', 'gpus', 'priority', 'running_time_min'])
            keys = set(resource_specification.keys())

            if not keys.issubset(acceptable_resource_types):
                message = "Task resource specification only accepts these types of resources: {}".format(
                        ', '.join(acceptable_resource_types))
                logger.error(message)
                raise ExecutorError(self, message)

            # this checks that either all of the required resource types are specified, or
            # that none of them are: the `required_resource_types` are not actually required,
            # but if one is specified, then they all must be.
            key_check = required_resource_types.intersection(keys)
            required_keys_ok = len(key_check) == 0 or key_check == required_resource_types
            if not self.autolabel and not required_keys_ok:
                logger.error("Running with `autolabel=False`. In this mode, "
                             "task resource specification requires "
                             "three resources to be specified simultaneously: cores, memory, and disk")
                raise ExecutorError(self, "Task resource specification requires "
                                          "three resources to be specified simultaneously: cores, memory, and disk. "
                                          "Try setting autolabel=True if you are unsure of the resource usage")

            for k in keys:
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
                    output_files.append(self._std_output_to_vine(kwarg, maybe_file))
            # For any other keyword that looks like a file, assume it is an input file
            elif isinstance(maybe_file, File):
                input_files.append(self._register_file(maybe_file))

        # Create a Future object and have it be mapped from the task ID in the tasks dictionary
        fu = Future()
        fu.parsl_executor_task_id = task_id
        logger.debug("Getting tasks_lock to set vine-level task entry")
        with self.tasks_lock:
            logger.debug("Got tasks_lock to set vine-level task entry")
            self.tasks[str(task_id)] = fu

        logger.debug("Creating task {} for function {} with args {}".format(task_id, func, args))

        # Pickle the result into object to pass into message buffer
        function_file = self._path_in_task(task_id, "function")
        result_file = self._path_in_task(task_id, "result")
        map_file = self._path_in_task(task_id, "map")

        logger.debug("Creating Task {} with function at: {}".format(task_id, function_file))
        logger.debug("Creating Task {} with result to be found at: {}".format(task_id, result_file))

        self._serialize_function(function_file, func, args, kwargs)

        if self.pack:
            env_pkg = self._prepare_package(func, self.extra_pkgs)
        else:
            env_pkg = None

        logger.debug("Constructing map for local filenames at worker for task {}".format(task_id))
        self._construct_map_file(map_file, input_files, output_files)

        if not self.submit_process.is_alive():
            raise ExecutorError(self, "taskvine Submit Process is not alive")

        # Create message to put into the message queue
        logger.debug("Placing task {} on message queue".format(task_id))
        if category is None:
            category = func.__name__ if self.autocategory else 'parsl-default'
        self.task_queue.put_nowait(ParslTaskToVine(task_id,
                                                   category,
                                                   cores,
                                                   memory,
                                                   disk,
                                                   gpus,
                                                   priority,
                                                   running_time_min,
                                                   env_pkg,
                                                   map_file,
                                                   function_file,
                                                   result_file,
                                                   input_files,
                                                   output_files))

        return fu

    def _construct_worker_command(self):
        worker_command = self.worker_executable
        if self.project_password_file:
            worker_command += ' --password {}'.format(self.project_password_file)
        if self.worker_options:
            worker_command += ' {}'.format(self.worker_options)
        if self.project_name:
            worker_command += ' -M {}'.format(self.project_name)
        else:
            worker_command += ' {} {}'.format(self.address, self.port)

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

        # Either build a dictionary with the source of the function, or pickle
        # the function directly:
        if self.source:
            function_info = {"source code": inspect.getsource(parsl_fn),
                             "name": parsl_fn.__name__,
                             "args": parsl_fn_args,
                             "kwargs": parsl_fn_kwargs}
        else:
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
        taskvine. cache is always False if self.use_cache is False.
        Otherwise, it is set to True if parsl_file is used more than once.
        stage is True if the file needs to be copied by taskvine. (i.e., not
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

        return ParslFileToVine(parsl_file.filepath, to_stage, to_cache)

    def _std_output_to_vine(self, fdname, stdfspec):
        """Find the name of the file that will contain stdout or stderr and
        return a ParslFileToVine with it. These files are never cached"""
        fname, mode = putils.get_std_fname_mode(fdname, stdfspec)
        to_stage = not os.path.isabs(fname)
        return ParslFileToVine(fname, stage=to_stage, cache=False)

    def _prepare_package(self, fn, extra_pkgs):
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
            analyze_cmdline = [package_analyze_script, exec_parsl_function.__file__, '-', spec.name]
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
            subprocess.run([package_create_script, spec.name, tarball], stdout=subprocess.DEVNULL, check=True)
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
        """Count the number of outstanding tasks. This is inefficiently
        implemented and probably could be replaced with a counter.
        """
        outstanding = 0
        with self.tasks_lock:
            for fut in self.tasks.values():
                if not fut.done():
                    outstanding += 1
        logger.debug(f"Counted {outstanding} outstanding tasks")
        return outstanding

    @property
    def workers_per_node(self) -> Union[int, float]:
        return 1

    def scale_in(self, count):
        """Scale in method. Not implemented.
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

        logger.debug("Joining on submit process")
        self.submit_process.join()
        logger.debug("Joining on collector thread")
        self.collector_thread.join()

        logger.debug("TaskVine shutdown completed")
        return True

    @wrap_with_logs
    def _collect_taskvine_results(self):
        """Sets the values of tasks' futures of tasks completed by taskvine.
        """
        logger.debug("Starting Collector Thread")
        try:
            while not self.should_stop.value:
                if not self.submit_process.is_alive():
                    raise ExecutorError(self, "taskvine Submit Process is not alive")

                # Get the result message from the collector_queue
                try:
                    task_report = self.collector_queue.get(timeout=1)
                except queue.Empty:
                    continue

                # Obtain the future from the tasks dictionary
                with self.tasks_lock:
                    future = self.tasks.pop(task_report.id)

                logger.debug("Updating Future for Parsl Task {}".format(task_report.id))
                if task_report.result_received:
                    future.set_result(task_report.result)
                else:
                    # If there are no results, then the task failed according to one of
                    # taskvine modes, such as resource exhaustion.
                    future.set_exception(TaskVineTaskFailure(task_report.reason, task_report.result))
        finally:
            logger.debug("Marking all outstanding tasks as failed")
            logger.debug("Acquiring tasks_lock")
            with self.tasks_lock:
                logger.debug("Acquired tasks_lock")
                # set exception for tasks waiting for results that taskvine did not execute
                for fu in self.tasks.values():
                    if not fu.done():
                        fu.set_exception(TaskVineFailure("taskvine executor failed to execute the task."))
        logger.debug("Exiting Collector Thread")


@wrap_with_logs
def _taskvine_submit_wait(task_queue=multiprocessing.Queue(),
                          launch_cmd=None,
                          env=None,
                          collector_queue=multiprocessing.Queue(),
                          data_dir=".",
                          full=False,
                          shared_fs=False,
                          autolabel=False,
                          autolabel_window=None,
                          autocategory=False,
                          max_retries=0,
                          should_stop=None,
                          port=VINE_DEFAULT_PORT,
                          vine_log_dir=None,
                          project_password_file=None,
                          project_name=None):
    """Thread to handle Parsl app submissions to the TaskVine objects.
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
    vine_debug_log = None
    if vine_log_dir is not None:
        logger.debug("Setting debugging flags and creating logging file")
        vine_debug_log = os.path.join(vine_log_dir, "debug_log")

    # Create TaskVine queue object
    logger.debug("Creating TaskVine Object")
    try:
        logger.debug("Listening on port {}".format(port))
        q = Manager(port, debug_log=vine_debug_log)
    except Exception as e:
        logger.error("Unable to create TaskVine object: {}".format(e))
        raise e

    # Specify TaskVine queue attributes
    if project_name:
        q.set_name(project_name)

    if project_password_file:
        q.set_password_file(project_password_file)

    if autolabel:
        q.enable_monitoring()
        if autolabel_window is not None:
            q.tune('category-steady-n-tasks', autolabel_window)

    # Only write logs when the vine_log_dir is specified, which it most likely will be
    if vine_log_dir is not None:
        vine_perf_log = os.path.join(vine_log_dir, "perf_log")
        vine_trans_log = os.path.join(vine_log_dir, "transaction_log")
        if full and autolabel:
            vine_resource_log = os.path.join(vine_log_dir, "resource_logs")
            q.enable_monitoring_full(dirname=vine_resource_log)
        q.enable_perf_log(vine_perf_log)
        q.enable_transactions_log(vine_trans_log)

    orig_ppid = os.getppid()

    result_file_of_task_id = {}  # Mapping taskid -> result file for active tasks.

    while not should_stop.value:
        # Monitor the task queue
        ppid = os.getppid()
        if ppid != orig_ppid:
            logger.debug("new Process")
            break

        # Submit tasks
        while task_queue.qsize() > 0 and not should_stop.value:
            # Obtain task from task_queue
            try:
                task = task_queue.get(timeout=1)
                logger.debug("Removing task from queue")
            except queue.Empty:
                continue

            pkg_pfx = ""
            if task.env_pkg is not None:
                pkg_pfx = "./{} -e {} ".format(os.path.basename(package_run_script),
                                               os.path.basename(task.env_pkg))

            # Create command string
            logger.debug(launch_cmd)
            command_str = launch_cmd.format(package_prefix=pkg_pfx,
                                            mapping=os.path.basename(task.map_file),
                                            function=os.path.basename(task.function_file),
                                            result=os.path.basename(task.result_file))
            logger.debug(command_str)

            # Create TaskVine task for the command
            logger.debug("Sending task {} with command: {}".format(task.id, command_str))
            try:
                t = Task(command_str)
            except Exception as e:
                logger.error("Unable to create task: {}".format(e))
                collector_queue.put_nowait(VineTaskToParsl(id=task.id,
                                                           result_received=False,
                                                           result=None,
                                                           reason="task could not be created by taskvine",
                                                           status=-1))
                continue

            t.set_category(task.category)
            if autolabel:
                q.set_category_mode(task.category, VINE_ALLOCATION_MODE_MAX_THROUGHPUT)

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

            if max_retries is not None:
                logger.debug(f"Specifying max_retries {max_retries}")
                t.set_retries(max_retries)
            else:
                logger.debug("Not specifying max_retries")

            # Specify environment variables for the task
            if env is not None:
                for var in env:
                    t.set_env_var(var, env[var])

            if task.env_pkg is not None:
                t.add_input_file(package_run_script, cache=True)
                t.add_input_file(task.env_pkg, cache=True)

            # Specify script, and data/result files for task
            t.add_input_file(exec_parsl_function.__file__, cache=True)
            t.add_input_file(task.function_file, cache=False)
            t.add_input_file(task.map_file, cache=False)
            t.add_output_file(task.result_file, cache=False)
            t.set_tag(str(task.id))
            result_file_of_task_id[str(task.id)] = task.result_file

            logger.debug("Parsl ID: {}".format(task.id))

            # Specify input/output files that need to be staged.
            # Absolute paths are assumed to be in shared filesystem, and thus
            # not staged by taskvine.
            if not shared_fs:
                for spec in task.input_files:
                    if spec.stage:
                        t.add_input_file(spec.parsl_name, spec.parsl_name, cache=spec.cache)
                for spec in task.output_files:
                    if spec.stage:
                        t.add_output_file(spec.parsl_name, spec.parsl_name, cache=spec.cache)

            # Submit the task to the TaskVine object
            logger.debug("Submitting task {} to TaskVine".format(task.id))
            try:
                vine_id = q.submit(t)
            except Exception as e:
                logger.error("Unable to submit task to taskvine: {}".format(e))
                collector_queue.put_nowait(VineTaskToParsl(id=task.id,
                                                           result_received=False,
                                                           result=None,
                                                           reason="task could not be submited to taskvine",
                                                           status=-1))
                continue
            logger.info("Task {} submitted to TaskVine with id {}".format(task.id, vine_id))

        # If the queue is not empty wait on the TaskVine queue for a task
        task_found = True
        if not q.empty():
            while task_found and not should_stop.value:
                # Obtain the task from the queue
                t = q.wait(1)
                if t is None:
                    task_found = False
                    continue
                # When a task is found:
                parsl_id = t.tag
                logger.debug("Completed TaskVine task {}, parsl task {}".format(t.id, t.tag))
                result_file = result_file_of_task_id.pop(t.tag)

                # A tasks completes 'succesfully' if it has result file,
                # and it can be loaded. This may mean that the 'success' is
                # an exception.
                logger.debug("Looking for result in {}".format(result_file))
                try:
                    with open(result_file, "rb") as f_in:
                        result = pickle.load(f_in)
                    logger.debug("Found result in {}".format(result_file))
                    collector_queue.put_nowait(VineTaskToParsl(id=parsl_id,
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
                    logger.debug("Task with id parsl {} / vine {} failed because:\n{}"
                                 .format(parsl_id, t.id, reason))
                    collector_queue.put_nowait(VineTaskToParsl(id=parsl_id,
                                                               result_received=False,
                                                               result=e,
                                                               reason=reason,
                                                               status=t.exit_code))
    logger.debug("Exiting TaskVine Monitoring Process")
    return 0


def _explain_taskvine_result(vine_task):
    """Returns a string with the reason why a task failed according to taskvine."""

    vine_result = vine_task.result

    reason = "taskvine result: "
    if vine_result == vine.VINE_RESULT_SUCCESS:
        reason += "succesful execution with exit code {}".format(vine_task.return_status)
    elif vine_result == vine.VINE_RESULT_OUTPUT_MISSING:
        reason += "The result file was not transfered from the worker.\n"
        reason += "This usually means that there is a problem with the python setup,\n"
        reason += "or the wrapper that executes the function."
        reason += "\nTrace:\n" + str(vine_task.output)
    elif vine_result == vine.VINE_RESULT_INPUT_MISSING:
        reason += "missing input file"
    elif vine_result == vine.VINE_RESULT_STDOUT_MISSING:
        reason += "stdout has been truncated"
    elif vine_result == vine.VINE_RESULT_SIGNAL:
        reason += "task terminated with a signal"
    elif vine_result == vine.VINE_RESULT_RESOURCE_EXHAUSTION:
        reason += "task used more resources than requested"
    elif vine_result == vine.VINE_RESULT_TASK_TIMEOUT:
        reason += "task ran past the specified end time"
    elif vine_result == vine.VINE_RESULT_UNKNOWN:
        reason += "result could not be classified"
    elif vine_result == vine.VINE_RESULT_FORSAKEN:
        reason += "task failed, but not a task error"
    elif vine_result == vine.VINE_RESULT_MAX_RETRIES:
        reason += "unable to complete after specified number of retries"
    elif vine_result == vine.VINE_RESULT_TASK_MAX_RUN_TIME:
        reason += "task ran for more than the specified time"
    elif vine_result == vine.VINE_RESULT_DISK_ALLOC_FULL:
        reason += "task needed more space to complete task"
    elif vine_result == vine.VINE_RESULT_RMONITOR_ERROR:
        reason += "task failed because the monitor did not produce an output"
    else:
        reason += "unable to process TaskVine system failure"
    return reason
