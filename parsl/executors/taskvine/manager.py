import hashlib
import logging
import os
import queue
import shutil
import subprocess
import uuid

from parsl.executors.taskvine import exec_parsl_function
from parsl.executors.taskvine.utils import VineTaskToParsl, run_parsl_function
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

try:
    from ndcctools.taskvine import FunctionCall, Manager, Task, cvine
    from ndcctools.taskvine.cvine import (
        VINE_ALLOCATION_MODE_EXHAUSTIVE_BUCKETING,
        VINE_ALLOCATION_MODE_MAX,
        VINE_ALLOCATION_MODE_MAX_THROUGHPUT,
    )
except ImportError:
    _taskvine_enabled = False
else:
    _taskvine_enabled = True

logger = logging.getLogger(__name__)


def _set_manager_attributes(m, config):
    """ Set various manager global attributes."""
    if config.project_password_file:
        m.set_password_file(config.project_password_file)

    # Autolabeling resources require monitoring to be enabled
    if config.autolabel:
        m.enable_monitoring()
        if config.autolabel_window is not None:
            m.tune('category-steady-n-tasks', config.autolabel_window)

    # Specify number of workers to wait for before sending the first task
    if config.wait_for_workers:
        m.tune("wait-for-workers", config.wait_for_workers)

    # Enable peer transfer feature between workers if specified
    if config.enable_peer_transfers:
        m.enable_peer_transfers()
    else:
        m.disable_peer_transfers()

    # Set catalog report to parsl if project name exists
    if m.name:
        m.set_property("framework", "parsl")

    if config.tune_parameters is not None:
        for k, v in config.tune_parameters.items():
            m.tune(k, v)


def _prepare_environment_serverless(manager_config, env_cache_dir, poncho_create_script):
    # Return path to a packaged poncho environment
    poncho_env_path = ''
    if not manager_config.shared_fs:
        if manager_config.env_pack is None:
            raise Exception('TaskVine manager needs env_pack to be specified when running tasks in serverless mode and with no shared_fs')

        poncho_env_path = manager_config.env_pack

        # If a conda environment name or path is given, then use the hash of the headers of
        # all contained packages as the name of the to-be-packaged poncho tarball,
        # and package it if it's not cached.
        if not poncho_env_path.endswith('tar.gz'):
            if os.path.isabs(poncho_env_path):
                conda_env_signature = hashlib.md5(subprocess.check_output(['conda', 'list', '-p', poncho_env_path, '--json'])).hexdigest()
                logger.debug(f'Signature of conda environment at {poncho_env_path}: {conda_env_signature}')
            else:
                conda_env_signature = hashlib.md5(subprocess.check_output(['conda', 'list', '-n', poncho_env_path, '--json'])).hexdigest()
                logger.debug(f'Signature of conda environment named {poncho_env_path}: {conda_env_signature}')

            # If env is cached then use it,
            # else create a new env tarball
            poncho_env_path = os.path.join(env_cache_dir, '.'.join([conda_env_signature, 'tar.gz']))
            if not os.path.isfile(poncho_env_path):
                logger.debug(f'No cached poncho environment. Creating poncho environment for library task at {poncho_env_path}')
                try:
                    subprocess.run([poncho_create_script, manager_config.env_pack, poncho_env_path], stdout=subprocess.DEVNULL, check=True)
                except Exception:
                    logger.error('Cannot create a poncho environment. Removing it.')
                    if os.path.isfile(poncho_env_path):
                        os.remove(poncho_env_path)
                    raise
            else:
                logger.debug(f'Found cached poncho environment at {poncho_env_path}. Reusing it.')
        else:
            logger.debug(f'Use the given poncho environment at {manager_config.env_pack} to setup library task.')
    return poncho_env_path


def _prepare_environment_regular(m, manager_config, t, task, poncho_env_to_file, poncho_create_script):
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
                logger.debug(f'Creating a poncho environment at {env_tarball} from conda environment {manager_config.env_pack}')
                subprocess.run([poncho_create_script, manager_config.env_pack, env_tarball], stdout=subprocess.DEVNULL, check=True)
            else:
                env_tarball = manager_config.env_pack
            poncho_env_file = m.declare_poncho(env_tarball, cache=True, peer_transfer=True)
            poncho_env_to_file[manager_config.env_pack] = poncho_env_file
        else:
            poncho_env_file = poncho_env_to_file[manager_config.env_pack]
            logger.debug(f'Found cached poncho environment for {manager_config.env_pack}. Reusing it.')

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
    _set_manager_attributes(m, manager_config)

    # Get parent pid, useful to shutdown this process when its parent, the taskvine
    # executor process, exits.
    orig_ppid = os.getppid()

    result_file_of_task_id = {}  # Mapping executor task id -> result file.

    poncho_env_to_file = {}  # Mapping poncho_env id to File object in TaskVine

    # Mapping of parsl local file name to TaskVine File object
    # dict[str] -> vine File object
    parsl_file_name_to_vine_file = {}

    # Mapping of tasks from vine id to parsl id
    # Dict[str] -> str
    vine_id_to_executor_task_id = {}

    # Find poncho scripts to create and activate an environment tarball
    poncho_create_script = shutil.which("poncho_package_create")

    # Declare helper script as cache-able and peer-transferable
    exec_parsl_function_file = m.declare_file(exec_parsl_function.__file__, cache=True, peer_transfer=True)

    # Flag to make sure library for serverless tasks is declared and installed only once.
    lib_installed = False

    # Create cache dir for environment files
    env_cache_dir = os.path.join(manager_config.vine_log_dir, 'vine-cache', 'vine-poncho-env-cache')
    os.makedirs(env_cache_dir, exist_ok=True)

    logger.debug("Entering main loop of TaskVine manager")

    while not should_stop.is_set():
        # Check if executor process is still running
        ppid = os.getppid()
        if ppid != orig_ppid:
            logger.debug("Executor process is detected to have exited. Exiting..")
            break

        # Submit tasks
        while ready_task_queue.qsize() > 0 or m.empty() and not should_stop.is_set():
            # Obtain task from ready_task_queue
            try:
                task = ready_task_queue.get(timeout=1)
                logger.debug("Removing executor task from queue")
            except queue.Empty:
                logger.debug("Queue is empty")
                continue
            if task.exec_mode == 'regular':
                # Create command string
                launch_cmd = "python3 exec_parsl_function.py {mapping} {function} {argument} {result}"
                if manager_config.init_command != '':
                    launch_cmd = "{init_cmd} " + launch_cmd
                command_str = launch_cmd.format(init_cmd=manager_config.init_command,
                                                mapping=os.path.basename(task.map_file),
                                                function=os.path.basename(task.function_file),
                                                argument=os.path.basename(task.argument_file),
                                                result=os.path.basename(task.result_file))
                logger.debug("Sending executor task {} (mode: regular) with command: {}".format(task.executor_id, command_str))
                try:
                    t = Task(command_str)
                except Exception as e:
                    logger.error("Unable to create executor task (mode:regular): {}".format(e))
                    finished_task_queue.put_nowait(VineTaskToParsl(executor_id=task.executor_id,
                                                                   result_received=False,
                                                                   result_file=None,
                                                                   reason="task could not be created by taskvine",
                                                                   status=-1))
                    continue
            elif task.exec_mode == 'serverless':
                if not lib_installed:
                    # Declare and install common library for serverless tasks.
                    # Library requires an environment setup properly, which is
                    # different from setup of regular tasks.
                    # If shared_fs is True, then no environment preparation is done.
                    # Only the core serverless code is created.
                    poncho_env_path = _prepare_environment_serverless(manager_config, env_cache_dir, poncho_create_script)

                    # Don't automatically add environment so manager can declare and cache the vine file associated with the environment file
                    add_env = False
                    serverless_lib = m.create_library_from_functions('common-parsl-taskvine-lib',
                                                                     run_parsl_function,
                                                                     poncho_env=poncho_env_path,
                                                                     init_command=manager_config.init_command,
                                                                     add_env=add_env)

                    # Configure the library if provided
                    if manager_config.library_config:
                        lib_cores = manager_config.library_config.get('cores', None)
                        lib_memory = manager_config.library_config.get('memory', None)
                        lib_disk = manager_config.library_config.get('disk', None)
                        lib_slots = manager_config.library_config.get('num_slots', None)
                        if lib_cores:
                            serverless_lib.set_cores(lib_cores)
                        if lib_memory:
                            serverless_lib.set_memory(lib_memory)
                        if lib_disk:
                            serverless_lib.set_disk(lib_disk)
                        if lib_slots:
                            serverless_lib.set_function_slots(lib_slots)

                    if poncho_env_path:
                        serverless_lib_env_file = m.declare_poncho(poncho_env_path, cache=True, peer_transfer=True)
                        serverless_lib.add_environment(serverless_lib_env_file)
                        poncho_env_to_file[manager_config.env_pack] = serverless_lib_env_file
                        logger.debug(f'Created library task using poncho environment at {poncho_env_path}.')
                    else:
                        logger.debug('Created minimal library task with no environment.')

                    m.install_library(serverless_lib)
                    lib_installed = True
                try:
                    # run_parsl_function only needs remote names of map_file, function_file, argument_file,
                    # and result_file, which are simply named map, function, argument, result.
                    # These names are given when these files are declared below.
                    t = FunctionCall('common-parsl-taskvine-lib', run_parsl_function.__name__, 'map', 'function', 'argument', 'result')
                except Exception as e:
                    logger.error("Unable to create executor task (mode:serverless): {}".format(e))
                    finished_task_queue.put_nowait(VineTaskToParsl(executor_id=task.executor_id,
                                                                   result_received=False,
                                                                   result_file=None,
                                                                   reason="task could not be created by taskvine",
                                                                   status=-1))
            else:
                raise Exception(f'Unrecognized task mode {task.exec_mode}. Exiting...')

            # prepare environment for regular tasks if not using shared_fs
            if task.exec_mode == 'regular' and not manager_config.shared_fs:
                _prepare_environment_regular(m, manager_config, t, task, poncho_env_to_file, poncho_create_script)

            t.set_category(task.category)

            # Set autolabel mode
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

            # Specify environment variables for the task
            if manager_config.env_vars is not None:
                for var in manager_config.env_vars:
                    t.set_env_var(str(var), str(manager_config.env_vars[var]))

            if task.exec_mode == 'regular':
                # Add helper files that execute parsl functions on remote nodes
                # only needed to add as file for tasks with 'regular' mode
                t.add_input(exec_parsl_function_file, "exec_parsl_function.py")

            # Declare and add task-specific function, data, and result files to task
            task_function_file = m.declare_file(task.function_file, cache=False, peer_transfer=False)
            t.add_input(task_function_file, "function")

            task_argument_file = m.declare_file(task.argument_file, cache=False, peer_transfer=False)
            t.add_input(task_argument_file, "argument")

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
                            parsl_file_name_to_vine_file[spec.parsl_name] = task_out_file
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
                                                               result_file=None,
                                                               reason="task could not be submited to taskvine",
                                                               status=-1))
                continue

            logger.debug("Executor task {} submitted as TaskVine task with id {}".format(task.executor_id, vine_id))

        # If the queue is not empty wait on the TaskVine queue for a task
        task_found = True
        while not m.empty() and task_found and not should_stop.is_set():
            # Obtain the task from the queue
            t = m.wait(1)
            if t is None:
                task_found = False
                continue
            logger.debug('Found a task')
            executor_task_id = vine_id_to_executor_task_id[str(t.id)][0]
            vine_id_to_executor_task_id.pop(str(t.id))

            # When a task is found
            result_file = result_file_of_task_id.pop(executor_task_id)

            logger.debug(f"completed executor task info: {executor_task_id}, {t.category}, {t.command}, {t.std_output}")

            # A tasks completes 'succesfully' if it has result file.
            # A check whether the Python object represented using this file can be
            # deserialized happens later in the collector thread of the executor
            # process.
            logger.debug("Looking for result in {}".format(result_file))
            if os.path.exists(result_file):
                logger.debug("Found result in {}".format(result_file))
                finished_task_queue.put_nowait(VineTaskToParsl(executor_id=executor_task_id,
                                                               result_received=True,
                                                               result_file=result_file,
                                                               reason=None,
                                                               status=t.exit_code))
            # If a result file could not be generated, explain the
            # failure according to taskvine error codes.
            else:
                reason = _explain_taskvine_result(t)
                logger.debug("Did not find result in {}".format(result_file))
                logger.debug("Wrapper Script status: {}\nTaskVine Status: {}".format(t.exit_code, t.result))
                logger.debug("Task with executor id {} / vine id {} failed because:\n{}".format(executor_task_id, t.id, reason))
                finished_task_queue.put_nowait(VineTaskToParsl(executor_id=executor_task_id,
                                                               result_received=False,
                                                               result_file=None,
                                                               reason=reason,
                                                               status=t.exit_code))

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
