import atexit
import logging
import os
import pathlib
import pickle
import random
import typeguard
import inspect
import threading
import sys
import datetime
from getpass import getuser
from typing import Optional
from uuid import uuid4
from socket import gethostname
from concurrent.futures import Future
from functools import partial

import parsl
from parsl.app.errors import RemoteExceptionWrapper
from parsl.app.futures import DataFuture
from parsl.config import Config
from parsl.data_provider.data_manager import DataManager
from parsl.data_provider.files import File
from parsl.dataflow.error import BadCheckpoint, ConfigurationError, DependencyError, DuplicateTaskError
from parsl.dataflow.flow_control import FlowControl, Timer
from parsl.dataflow.futures import AppFuture
from parsl.dataflow.memoization import Memoizer
from parsl.dataflow.rundirs import make_rundir
from parsl.dataflow.states import States
from parsl.dataflow.usage_tracking.usage import UsageTracker
from parsl.executors.threads import ThreadPoolExecutor
from parsl.utils import get_version, get_std_fname_mode

from parsl.monitoring.message_type import MessageType

logger = logging.getLogger(__name__)


class DataFlowKernel(object):
    """The DataFlowKernel adds dependency awareness to an existing executor.

    It is responsible for managing futures, such that when dependencies are resolved,
    pending tasks move to the runnable state.

    Here is a simplified diagram of what happens internally::

         User             |        DFK         |    Executor
        ----------------------------------------------------------
                          |                    |
               Task-------+> +Submit           |
             App_Fu<------+--|                 |
                          |  Dependencies met  |
                          |         task-------+--> +Submit
                          |        Ex_Fu<------+----|

    """

    def __init__(self, config=Config()):
        """Initialize the DataFlowKernel.

        Parameters
        ----------
        config : Config
            A specification of all configuration options. For more details see the
            :class:~`parsl.config.Config` documentation.
        """

        # this will be used to check cleanup only happens once
        self.cleanup_called = False

        if isinstance(config, dict):
            raise ConfigurationError(
                    'Expected `Config` class, received dictionary. For help, '
                    'see http://parsl.readthedocs.io/en/stable/stubs/parsl.config.Config.html')
        self._config = config
        self.run_dir = make_rundir(config.run_dir)

        if config.initialize_logging:
            parsl.set_file_logger("{}/parsl.log".format(self.run_dir), level=logging.DEBUG)

        logger.debug("Starting DataFlowKernel with config\n{}".format(config))

        if sys.version_info < (3, 6):
            logger.warning("Support for python versions < 3.6 is deprecated and will be removed after parsl 0.10")

        logger.info("Parsl version: {}".format(get_version()))

        self.checkpoint_lock = threading.Lock()

        self.usage_tracker = UsageTracker(self)
        self.usage_tracker.send_message()

        # Monitoring
        self.run_id = str(uuid4())
        self.tasks_completed_count = 0
        self.tasks_failed_count = 0
        self.tasks_dep_fail_count = 0

        self.monitoring = config.monitoring
        # hub address and port for interchange to connect
        self.hub_address = None
        self.hub_interchange_port = None
        if self.monitoring:
            if self.monitoring.logdir is None:
                self.monitoring.logdir = self.run_dir
            self.hub_address = self.monitoring.hub_address
            self.hub_interchange_port = self.monitoring.start(self.run_id)

        self.time_began = datetime.datetime.now()
        self.time_completed = None

        # TODO: make configurable
        logger.info("Run id is: " + self.run_id)

        self.workflow_name = None
        if self.monitoring is not None and self.monitoring.workflow_name is not None:
            self.workflow_name = self.monitoring.workflow_name
        else:
            for frame in inspect.stack():
                fname = os.path.basename(str(frame.filename))
                parsl_file_names = ['dflow.py', 'typeguard.py', '__init__.py']
                # Find first file name not considered a parsl file
                if fname not in parsl_file_names:
                    self.workflow_name = fname
                    break
            else:
                self.workflow_name = "unnamed"

        self.workflow_version = str(self.time_began.replace(microsecond=0))
        if self.monitoring is not None and self.monitoring.workflow_version is not None:
            self.workflow_version = self.monitoring.workflow_version

        workflow_info = {
                'python_version': "{}.{}.{}".format(sys.version_info.major,
                                                    sys.version_info.minor,
                                                    sys.version_info.micro),
                'parsl_version': get_version(),
                "time_began": self.time_began,
                'time_completed': None,
                'workflow_duration': None,
                'run_id': self.run_id,
                'workflow_name': self.workflow_name,
                'workflow_version': self.workflow_version,
                'rundir': self.run_dir,
                'tasks_completed_count': self.tasks_completed_count,
                'tasks_failed_count': self.tasks_failed_count,
                'user': getuser(),
                'host': gethostname(),
        }

        if self.monitoring:
            self.monitoring.send(MessageType.WORKFLOW_INFO,
                                 workflow_info)

        checkpoints = self.load_checkpoints(config.checkpoint_files)
        self.memoizer = Memoizer(self, memoize=config.app_cache, checkpoint=checkpoints)
        self.checkpointed_tasks = 0
        self._checkpoint_timer = None
        self.checkpoint_mode = config.checkpoint_mode

        # the flow control keeps track of executors and provider task states;
        # must be set before executors are added since add_executors calls
        # flowcontrol.add_executors.
        self.flowcontrol = FlowControl(self)

        self.executors = {}
        self.data_manager = DataManager(self)
        data_manager_executor = ThreadPoolExecutor(max_threads=config.data_management_max_threads, label='data_manager')
        self.add_executors(config.executors + [data_manager_executor])

        if self.checkpoint_mode == "periodic":
            try:
                h, m, s = map(int, config.checkpoint_period.split(':'))
                checkpoint_period = (h * 3600) + (m * 60) + s
                self._checkpoint_timer = Timer(self.checkpoint, interval=checkpoint_period, name="Checkpoint")
            except Exception:
                logger.error("invalid checkpoint_period provided: {0} expected HH:MM:SS".format(config.checkpoint_period))
                self._checkpoint_timer = Timer(self.checkpoint, interval=(30 * 60), name="Checkpoint")

        self.task_count = 0
        self.tasks = {}
        self.submitter_lock = threading.Lock()

        atexit.register(self.atexit_cleanup)

    def _create_task_log_info(self, task_id):
        """
        Create the dictionary that will be included in the log.
        """

        info_to_monitor = ['func_name', 'fn_hash', 'memoize', 'hashsum', 'fail_count', 'status',
                           'id', 'time_submitted', 'time_returned', 'executor']

        task_log_info = {"task_" + k: self.tasks[task_id][k] for k in info_to_monitor}
        task_log_info['run_id'] = self.run_id
        task_log_info['timestamp'] = datetime.datetime.now()
        task_log_info['task_status_name'] = self.tasks[task_id]['status'].name
        task_log_info['tasks_failed_count'] = self.tasks_failed_count
        task_log_info['tasks_completed_count'] = self.tasks_completed_count
        task_log_info['task_inputs'] = str(self.tasks[task_id]['kwargs'].get('inputs', None))
        task_log_info['task_outputs'] = str(self.tasks[task_id]['kwargs'].get('outputs', None))
        task_log_info['task_stdin'] = self.tasks[task_id]['kwargs'].get('stdin', None)
        stdout_spec = self.tasks[task_id]['kwargs'].get('stdout', None)
        stderr_spec = self.tasks[task_id]['kwargs'].get('stderr', None)
        try:
            stdout_name, _ = get_std_fname_mode('stdout', stdout_spec)
        except Exception as e:
            logger.warning("Incorrect stdout format {} for Task {}".format(stdout_spec, task_id))
            stdout_name = str(e)
        try:
            stderr_name, _ = get_std_fname_mode('stderr', stderr_spec)
        except Exception as e:
            logger.warning("Incorrect stderr format {} for Task {}".format(stderr_spec, task_id))
            stderr_name = str(e)
        task_log_info['task_stdout'] = stdout_name
        task_log_info['task_stderr'] = stderr_name
        task_log_info['task_fail_history'] = ",".join(self.tasks[task_id]['fail_history'])
        task_log_info['task_depends'] = None
        if self.tasks[task_id]['depends'] is not None:
            task_log_info['task_depends'] = ",".join([str(t.tid) for t in self.tasks[task_id]['depends']
                                                      if isinstance(t, AppFuture) or isinstance(t, DataFuture)])
        return task_log_info

    def _count_deps(self, depends):
        """Internal.

        Count the number of unresolved futures in the list depends.
        """
        count = 0
        for dep in depends:
            if isinstance(dep, Future):
                if not dep.done():
                    count += 1

        return count

    @property
    def config(self):
        """Returns the fully initialized config that the DFK is actively using.

        Returns:
             - config (dict)
        """
        return self._config

    def handle_exec_update(self, task_id, future):
        """This function is called only as a callback from an execution
        attempt reaching a final state (either successfully or failing).

        It will launch retries if necessary, and update the task
        structure.

        Args:
             task_id (string) : Task id which is a uuid string
             future (Future) : The future object corresponding to the task which
             makes this callback
        """

        try:
            res = future.result()
            if isinstance(res, RemoteExceptionWrapper):
                res.reraise()

        except Exception as e:
            logger.debug("Task {} failed".format(task_id))
            # We keep the history separately, since the future itself could be
            # tossed.
            self.tasks[task_id]['fail_history'].append(str(e))
            self.tasks[task_id]['fail_count'] += 1

            if self.tasks[task_id]['status'] == States.dep_fail:
                logger.info("Task {} failed due to dependency failure so skipping retries".format(task_id))
            elif self.tasks[task_id]['fail_count'] <= self._config.retries:
                self.tasks[task_id]['status'] = States.pending
                logger.info("Task {} marked for retry".format(task_id))

            else:
                logger.exception("Task {} failed after {} retry attempts".format(task_id,
                                                                                 self._config.retries))
                self.tasks[task_id]['status'] = States.failed
                self.tasks_failed_count += 1
                self.tasks[task_id]['time_returned'] = datetime.datetime.now()

        else:
            self.tasks[task_id]['status'] = States.done
            self.tasks_completed_count += 1

            logger.info("Task {} completed".format(task_id))
            self.tasks[task_id]['time_returned'] = datetime.datetime.now()

        if self.tasks[task_id]['app_fu'].stdout is not None:
            logger.info("Standard output for task {} available at {}".format(task_id, self.tasks[task_id]['app_fu'].stdout))
        if self.tasks[task_id]['app_fu'].stderr is not None:
            logger.info("Standard error for task {} available at {}".format(task_id, self.tasks[task_id]['app_fu'].stderr))

        if self.monitoring:
            task_log_info = self._create_task_log_info(task_id)
            self.monitoring.send(MessageType.TASK_INFO, task_log_info)

        # it might be that in the course of the update, we've gone back to being
        # pending - in which case, we should consider ourself for relaunch
        if self.tasks[task_id]['status'] == States.pending:
            self.launch_if_ready(task_id)

        with self.tasks[task_id]['app_fu']._update_lock:

            if not future.done():
                raise ValueError("done callback called, despite future not reporting itself as done")

            try:
                res = future.result()
                if isinstance(res, RemoteExceptionWrapper):
                    res.reraise()

                self.tasks[task_id]['app_fu'].set_result(future.result())
            except Exception as e:
                if self.tasks[task_id]['retries_left'] > 0:
                    # ignore this exception, because assume some later
                    # parent executor, started external to this class,
                    # will provide the answer
                    pass
                else:
                    self.tasks[task_id]['app_fu'].set_exception(e)

        return

    def handle_app_update(self, task_id, future):
        """This function is called as a callback when an AppFuture
        is in its final state.

        It will trigger post-app processing such as checkpointing.

        Args:
             task_id (string) : Task id
             future (Future) : The relevant app future (which should be
                 consistent with the task structure 'app_fu' entry

        """

        if not self.tasks[task_id]['app_fu'].done():
            logger.error("Internal consistency error: app_fu is not done for task {}".format(task_id))
        if not self.tasks[task_id]['app_fu'] == future:
            logger.error("Internal consistency error: callback future is not the app_fu in task structure, for task {}".format(task_id))

        self.memoizer.update_memo(task_id, self.tasks[task_id], future)

        if self.checkpoint_mode == 'task_exit':
            self.checkpoint(tasks=[task_id])

        # If checkpointing is turned on, wiping app_fu is left to the checkpointing code
        # else we wipe it here.
        if self.checkpoint_mode is None:
            self.wipe_task(task_id)
        return

    def wipe_task(self, task_id):
        """ Remove task with task_id from the internal tasks table
        """
        del self.tasks[task_id]

    @staticmethod
    def check_staging_inhibited(kwargs):
        return kwargs.get('staging_inhibit_output', False)

    def launch_if_ready(self, task_id):
        """
        launch_if_ready will launch the specified task, if it is ready
        to run (for example, without dependencies, and in pending state).

        This should be called by any piece of the DataFlowKernel that
        thinks a task may have become ready to run.

        It is not an error to call launch_if_ready on a task that is not
        ready to run - launch_if_ready will not incorrectly launch that
        task.

        launch_if_ready is thread safe, so may be called from any thread
        or callback.
        """
        # after launching the task, self.tasks[task_id] is no longer
        # guaranteed to exist (because it can complete fast as part of the
        # submission - eg memoization)
        task_record = self.tasks.get(task_id)
        if task_record is None:
            # assume this task has already been processed to completion
            logger.debug("Task {} has no task record. Assuming it has already been processed to completion.".format(task_id))
            return
        if self._count_deps(task_record['depends']) == 0:

            # We can now launch *task*
            new_args, kwargs, exceptions = self.sanitize_and_wrap(task_id,
                                                                  task_record['args'],
                                                                  task_record['kwargs'])
            task_record['args'] = new_args
            task_record['kwargs'] = kwargs
            if not exceptions:
                # There are no dependency errors
                exec_fu = None
                # Acquire a lock, retest the state, launch
                with task_record['task_launch_lock']:
                    if task_record['status'] == States.pending:
                        try:
                            exec_fu = self.launch_task(
                                task_id, task_record['func'], *new_args, **kwargs)
                        except Exception as e:
                            # task launched failed somehow. the execution might
                            # have been launched and an exception raised after
                            # that, though. that's hard to detect from here.
                            # we don't attempt retries here. This is an error with submission
                            # even though it might come from user code such as a plugged-in
                            # executor or memoization hash function.

                            logger.debug("Got an exception launching task", exc_info=True)
                            self.tasks[task_id]['retries_left'] = 0
                            exec_fu = Future()
                            exec_fu.set_exception(e)
            else:
                logger.info(
                    "Task {} failed due to dependency failure".format(task_id))
                # Raise a dependency exception
                task_record['status'] = States.dep_fail
                self.tasks_dep_fail_count += 1

                if self.monitoring is not None:
                    task_log_info = self._create_task_log_info(task_id)
                    self.monitoring.send(MessageType.TASK_INFO, task_log_info)

                self.tasks[task_id]['retries_left'] = 0
                exec_fu = Future()
                exec_fu.set_exception(DependencyError(exceptions,
                                                      task_id))

            if exec_fu:

                try:
                    exec_fu.add_done_callback(partial(self.handle_exec_update, task_id))
                except Exception as e:
                    # this exception is ignored here because it is assumed that exception
                    # comes from directly executing handle_exec_update (because exec_fu is
                    # done already). If the callback executes later, then any exception
                    # coming out of the callback will be ignored and not propate anywhere,
                    # so this block attempts to keep the same behaviour here.
                    logger.error("add_done_callback got an exception {} which will be ignored".format(e))

                task_record['exec_fu'] = exec_fu

    def launch_task(self, task_id, executable, *args, **kwargs):
        """Handle the actual submission of the task to the executor layer.

        If the app task has the executors attributes not set (default=='all')
        the task is launched on a randomly selected executor from the
        list of executors. This behavior could later be updated to support
        binding to executors based on user specified criteria.

        If the app task specifies a particular set of executors, it will be
        targeted at those specific executors.

        Args:
            task_id (uuid string) : A uuid string that uniquely identifies the task
            executable (callable) : A callable object
            args (list of positional args)
            kwargs (arbitrary keyword arguments)


        Returns:
            Future that tracks the execution of the submitted executable
        """
        self.tasks[task_id]['time_submitted'] = datetime.datetime.now()

        memo_fu = self.memoizer.check_memo(task_id, self.tasks[task_id])
        if memo_fu:
            logger.info("Reusing cached result for task {}".format(task_id))
            return memo_fu

        executor_label = self.tasks[task_id]["executor"]
        try:
            executor = self.executors[executor_label]
        except Exception:
            logger.exception("Task {} requested invalid executor {}: config is\n{}".format(task_id, executor_label, self._config))
            raise ValueError("Task {} requested invalid executor {}".format(task_id, executor_label))

        if self.monitoring is not None and self.monitoring.resource_monitoring_enabled:
            wrapper_logging_level = logging.DEBUG if self.monitoring.monitoring_debug else logging.INFO
            executable = self.monitoring.monitor_wrapper(executable, task_id,
                                                         self.monitoring.monitoring_hub_url,
                                                         self.run_id,
                                                         wrapper_logging_level,
                                                         self.monitoring.resource_monitoring_interval)

        with self.submitter_lock:
            exec_fu = executor.submit(executable, self.tasks[task_id]['resource_specification'], *args, **kwargs)
        self.tasks[task_id]['status'] = States.launched
        if self.monitoring is not None:
            task_log_info = self._create_task_log_info(task_id)
            self.monitoring.send(MessageType.TASK_INFO, task_log_info)

        self.tasks[task_id]['retries_left'] = self._config.retries - \
            self.tasks[task_id]['fail_count']
        logger.info("Task {} launched on executor {}".format(task_id, executor.label))
        return exec_fu

    def _add_input_deps(self, executor, args, kwargs, func):
        """Look for inputs of the app that are files. Give the data manager
        the opportunity to replace a file with a data future for that file,
        for example wrapping the result of a staging action.

        Args:
            - executor (str) : executor where the app is going to be launched
            - args (List) : Positional args to app function
            - kwargs (Dict) : Kwargs to app function
        """

        # Return if the task is a data management task, rather than doing
        # data managament on it.
        if executor == 'data_manager':
            return args, kwargs, func

        inputs = kwargs.get('inputs', [])
        for idx, f in enumerate(inputs):
            (inputs[idx], func) = self.data_manager.optionally_stage_in(f, func, executor)

        for kwarg, f in kwargs.items():
            (kwargs[kwarg], func) = self.data_manager.optionally_stage_in(f, func, executor)

        newargs = list(args)
        for idx, f in enumerate(newargs):
            (newargs[idx], func) = self.data_manager.optionally_stage_in(f, func, executor)

        return tuple(newargs), kwargs, func

    def _add_output_deps(self, executor, args, kwargs, app_fut, func):
        logger.debug("Adding output dependencies")
        outputs = kwargs.get('outputs', [])
        app_fut._outputs = []
        for idx, f in enumerate(outputs):
            if isinstance(f, File) and not self.check_staging_inhibited(kwargs):
                # replace a File with a DataFuture - either completing when the stageout
                # future completes, or if no stage out future is returned, then when the
                # app itself completes.

                # The staging code will get a clean copy which it is allowed to mutate,
                # while the DataFuture-contained original will not be modified by any staging.
                f_copy = f.cleancopy()
                outputs[idx] = f_copy

                logger.debug("Submitting stage out for output file {}".format(repr(f)))
                stageout_fut = self.data_manager.stage_out(f_copy, executor, app_fut)
                if stageout_fut:
                    logger.debug("Adding a dependency on stageout future for {}".format(repr(f)))
                    app_fut._outputs.append(DataFuture(stageout_fut, f, tid=app_fut.tid))
                else:
                    logger.debug("No stageout dependency for {}".format(repr(f)))
                    app_fut._outputs.append(DataFuture(app_fut, f, tid=app_fut.tid))

                # this is a hook for post-task stageout
                # note that nothing depends on the output - which is maybe a bug
                # in the not-very-tested stageout system?
                newfunc = self.data_manager.replace_task_stage_out(f_copy, func, executor)
                if newfunc:
                    func = newfunc
            else:
                logger.debug("Not performing staging for: {}".format(repr(f)))
                app_fut._outputs.append(DataFuture(app_fut, f, tid=app_fut.tid))
        return func

    def _gather_all_deps(self, args, kwargs):
        """Count the number of unresolved futures on which a task depends.

        Args:
            - args (List[args]) : The list of args list to the fn
            - kwargs (Dict{kwargs}) : The dict of all kwargs passed to the fn

        Returns:
            - count, [list of dependencies]

        """
        # Check the positional args
        depends = []

        def check_dep(d):
            if isinstance(d, Future):
                depends.extend([d])

        for dep in args:
            check_dep(dep)

        # Check for explicit kwargs ex, fu_1=<fut>
        for key in kwargs:
            dep = kwargs[key]
            check_dep(dep)

        # Check for futures in inputs=[<fut>...]
        for dep in kwargs.get('inputs', []):
            check_dep(dep)

        return depends

    def sanitize_and_wrap(self, task_id, args, kwargs):
        """This function should be called only when all the futures we track have been resolved.

        If the user hid futures a level below, we will not catch
        it, and will (most likely) result in a type error.

        Args:
             task_id (uuid str) : Task id
             func (Function) : App function
             args (List) : Positional args to app function
             kwargs (Dict) : Kwargs to app function

        Return:
             partial function evaluated with all dependencies in  args, kwargs and kwargs['inputs'] evaluated.

        """
        dep_failures = []

        # Replace item in args
        new_args = []
        for dep in args:
            if isinstance(dep, Future):
                try:
                    new_args.extend([dep.result()])
                except Exception as e:
                    dep_failures.extend([e])
            else:
                new_args.extend([dep])

        # Check for explicit kwargs ex, fu_1=<fut>
        for key in kwargs:
            dep = kwargs[key]
            if isinstance(dep, Future):
                try:
                    kwargs[key] = dep.result()
                except Exception as e:
                    dep_failures.extend([e])

        # Check for futures in inputs=[<fut>...]
        if 'inputs' in kwargs:
            new_inputs = []
            for dep in kwargs['inputs']:
                if isinstance(dep, Future):
                    try:
                        new_inputs.extend([dep.result()])
                    except Exception as e:
                        dep_failures.extend([e])

                else:
                    new_inputs.extend([dep])
            kwargs['inputs'] = new_inputs

        return new_args, kwargs, dep_failures

    def submit(self, func, app_args, executors='all', fn_hash=None, cache=False, ignore_for_cache=None, app_kwargs={}):
        """Add task to the dataflow system.

        If the app task has the executors attributes not set (default=='all')
        the task will be launched on a randomly selected executor from the
        list of executors. If the app task specifies a particular set of
        executors, it will be targeted at the specified executors.

        >>> IF all deps are met:
        >>>   send to the runnable queue and launch the task
        >>> ELSE:
        >>>   post the task in the pending queue

        Args:
            - func : A function object

        KWargs :
            - app_args : Args to the function
            - executors (list or string) : List of executors this call could go to.
                    Default='all'
            - fn_hash (Str) : Hash of the function and inputs
                    Default=None
            - cache (Bool) : To enable memoization or not
            - ignore_for_cache (list) : List of kwargs to be ignored for memoization/checkpointing
            - app_kwargs (dict) : Rest of the kwargs to the fn passed as dict.

        Returns:
               (AppFuture) [DataFutures,]

        """

        if ignore_for_cache is None:
            ignore_for_cache = []

        if self.cleanup_called:
            raise ValueError("Cannot submit to a DFK that has been cleaned up")

        task_id = self.task_count
        self.task_count += 1
        if isinstance(executors, str) and executors.lower() == 'all':
            choices = list(e for e in self.executors if e != 'data_manager')
        elif isinstance(executors, list):
            choices = executors
        else:
            raise ValueError("Task {} supplied invalid type for executors: {}".format(task_id, type(executors)))
        executor = random.choice(choices)

        # The below uses func.__name__ before it has been wrapped by any staging code.

        label = app_kwargs.get('label')
        for kw in ['stdout', 'stderr']:
            if kw in app_kwargs:
                if app_kwargs[kw] == parsl.AUTO_LOGNAME:
                    if kw not in ignore_for_cache:
                        ignore_for_cache += [kw]
                    app_kwargs[kw] = os.path.join(
                                self.run_dir,
                                'task_logs',
                                str(int(task_id / 10000)).zfill(4),  # limit logs to 10k entries per directory
                                'task_{}_{}{}.{}'.format(
                                    str(task_id).zfill(4),
                                    func.__name__,
                                    '' if label is None else '_{}'.format(label),
                                    kw)
                    )

        resource_specification = app_kwargs.get('parsl_resource_specification', {})

        task_def = {'depends': None,
                    'executor': executor,
                    'func_name': func.__name__,
                    'fn_hash': fn_hash,
                    'memoize': cache,
                    'hashsum': None,
                    'exec_fu': None,
                    'fail_count': 0,
                    'fail_history': [],
                    'ignore_for_cache': ignore_for_cache,
                    'status': States.unsched,
                    'id': task_id,
                    'time_submitted': None,
                    'time_returned': None,
                    'resource_specification': resource_specification}

        app_fu = AppFuture(task_def)

        # Transform remote input files to data futures
        app_args, app_kwargs, func = self._add_input_deps(executor, app_args, app_kwargs, func)

        func = self._add_output_deps(executor, app_args, app_kwargs, app_fu, func)

        task_def.update({
                    'args': app_args,
                    'func': func,
                    'kwargs': app_kwargs,
                    'app_fu': app_fu})

        if task_id in self.tasks:
            raise DuplicateTaskError(
                "internal consistency error: Task {0} already exists in task list".format(task_id))
        else:
            self.tasks[task_id] = task_def

        # Get the list of dependencies for the task
        depends = self._gather_all_deps(app_args, app_kwargs)
        task_def['depends'] = depends

        depend_descs = []
        for d in depends:
            if isinstance(d, AppFuture) or isinstance(d, DataFuture):
                depend_descs.append("task {}".format(d.tid))
            else:
                depend_descs.append(repr(d))

        if depend_descs != []:
            waiting_message = "waiting on {}".format(", ".join(depend_descs))
        else:
            waiting_message = "not waiting on any dependency"

        logger.info("Task {} submitted for App {}, {}".format(task_id,
                                                              task_def['func_name'],
                                                              waiting_message))

        task_def['task_launch_lock'] = threading.Lock()

        app_fu.add_done_callback(partial(self.handle_app_update, task_id))
        task_def['status'] = States.pending
        logger.debug("Task {} set to pending state with AppFuture: {}".format(task_id, task_def['app_fu']))

        # at this point add callbacks to all dependencies to do a launch_if_ready
        # call whenever a dependency completes.

        # we need to be careful about the order of setting the state to pending,
        # adding the callbacks, and caling launch_if_ready explicitly once always below.

        # I think as long as we call launch_if_ready once after setting pending, then
        # we can add the callback dependencies at any point: if the callbacks all fire
        # before then, they won't cause a launch, but the one below will. if they fire
        # after we set it pending, then the last one will cause a launch, and the
        # explicit one won't.

        for d in depends:

            def callback_adapter(dep_fut):
                self.launch_if_ready(task_id)

            try:
                d.add_done_callback(callback_adapter)
            except Exception as e:
                logger.error("add_done_callback got an exception {} which will be ignored".format(e))

        self.launch_if_ready(task_id)

        return app_fu

    # it might also be interesting to assert that all DFK
    # tasks are in a "final" state (3,4,5) when the DFK
    # is closed down, and report some kind of warning.
    # although really I'd like this to drain properly...
    # and a drain function might look like this.
    # If tasks have their states changed, this won't work properly
    # but we can validate that...
    def log_task_states(self):
        logger.info("Summary of tasks in DFK:")

        keytasks = {state: 0 for state in States}

        for tid in self.tasks:
            keytasks[self.tasks[tid]['status']] += 1
        # Fetch from counters since tasks get wiped
        keytasks[States.done] = self.tasks_completed_count
        keytasks[States.failed] = self.tasks_failed_count
        keytasks[States.dep_fail] = self.tasks_dep_fail_count

        for state in States:
            if keytasks[state]:
                logger.info("Tasks in state {}: {}".format(str(state), keytasks[state]))

        total_summarized = sum(keytasks.values())
        if total_summarized != self.task_count:
            logger.error("Task count summarisation was inconsistent: summarised {} tasks, but task counter registered {} tasks".format(
                total_summarized, self.task_count))
        logger.info("End of summary")

    def _create_remote_dirs_over_channel(self, provider, channel):
        """ Create script directories across a channel

        Parameters
        ----------
        provider: Provider obj
           Provider for which scritps dirs are being created
        channel: Channel obk
           Channel over which the remote dirs are to be created
        """
        run_dir = self.run_dir
        if channel.script_dir is None:
            channel.script_dir = os.path.join(run_dir, 'submit_scripts')

            # Only create dirs if we aren't on a shared-fs
            if not channel.isdir(run_dir):
                parent, child = pathlib.Path(run_dir).parts[-2:]
                remote_run_dir = os.path.join(parent, child)
                channel.script_dir = os.path.join(remote_run_dir, 'remote_submit_scripts')
                provider.script_dir = os.path.join(run_dir, 'local_submit_scripts')

        channel.makedirs(channel.script_dir, exist_ok=True)

    def add_executors(self, executors):
        for executor in executors:
            executor.run_dir = self.run_dir
            executor.hub_address = self.hub_address
            executor.hub_port = self.hub_interchange_port
            if hasattr(executor, 'provider'):
                if hasattr(executor.provider, 'script_dir'):
                    executor.provider.script_dir = os.path.join(self.run_dir, 'submit_scripts')
                    os.makedirs(executor.provider.script_dir, exist_ok=True)

                    if hasattr(executor.provider, 'channels'):
                        logger.debug("Creating script_dir across multiple channels")
                        for channel in executor.provider.channels:
                            self._create_remote_dirs_over_channel(executor.provider, channel)
                    else:
                        self._create_remote_dirs_over_channel(executor.provider, executor.provider.channel)

            self.executors[executor.label] = executor
            executor.start()
        self.flowcontrol.add_executors(executors)

    def atexit_cleanup(self):
        if not self.cleanup_called:
            self.cleanup()

    def wait_for_current_tasks(self):
        """Waits for all tasks in the task list to be completed, by waiting for their
        AppFuture to be completed. This method will not necessarily wait for any tasks
        added after cleanup has started (such as data stageout?)
        """

        logger.info("Waiting for all remaining tasks to complete")
        for task_id in list(self.tasks):
            # .exception() is a less exception throwing way of
            # waiting for completion than .result()
            if task_id not in self.tasks:
                logger.debug("Task {} no longer in task list".format(task_id))
            else:
                fut = self.tasks[task_id]['app_fu']
                if not fut.done():
                    logger.debug("Waiting for task {} to complete".format(task_id))
                    fut.exception()
        logger.info("All remaining tasks completed")

    def cleanup(self):
        """DataFlowKernel cleanup.

        This involves releasing all resources explicitly.

        If the executors are managed by the DFK, then we call scale_in on each of
        the executors and call executor.shutdown. Otherwise, executor cleanup is left to
        the user.
        """
        logger.info("DFK cleanup initiated")

        # this check won't detect two DFK cleanups happening from
        # different threads extremely close in time because of
        # non-atomic read/modify of self.cleanup_called
        if self.cleanup_called:
            raise Exception("attempt to clean up DFK when it has already been cleaned-up")
        self.cleanup_called = True

        self.log_task_states()

        # Checkpointing takes priority over the rest of the tasks
        # checkpoint if any valid checkpoint method is specified
        if self.checkpoint_mode is not None:
            self.checkpoint()

            if self._checkpoint_timer:
                logger.info("Stopping checkpoint timer")
                self._checkpoint_timer.close()

        # Send final stats
        self.usage_tracker.send_message()
        self.usage_tracker.close()

        logger.info("Terminating flow_control and strategy threads")
        self.flowcontrol.close()

        for executor in self.executors.values():
            if executor.managed and not executor.bad_state_is_set:
                if executor.scaling_enabled:
                    job_ids = executor.provider.resources.keys()
                    executor.scale_in(len(job_ids))
                executor.shutdown()

        self.time_completed = datetime.datetime.now()

        if self.monitoring:
            self.monitoring.send(MessageType.WORKFLOW_INFO,
                                 {'tasks_failed_count': self.tasks_failed_count,
                                  'tasks_completed_count': self.tasks_completed_count,
                                  "time_began": self.time_began,
                                  'time_completed': self.time_completed,
                                  'workflow_duration': (self.time_completed - self.time_began).total_seconds(),
                                  'run_id': self.run_id, 'rundir': self.run_dir})

            self.monitoring.close()

        logger.info("DFK cleanup complete")

    def checkpoint(self, tasks=None):
        """Checkpoint the dfk incrementally to a checkpoint file.

        When called, every task that has been completed yet not
        checkpointed is checkpointed to a file.

        Kwargs:
            - tasks (List of task ids) : List of task ids to checkpoint. Default=None
                                         if set to None, we iterate over all tasks held by the DFK.

        .. note::
            Checkpointing only works if memoization is enabled

        Returns:
            Checkpoint dir if checkpoints were written successfully.
            By default the checkpoints are written to the RUNDIR of the current
            run under RUNDIR/checkpoints/{tasks.pkl, dfk.pkl}
        """
        with self.checkpoint_lock:
            checkpoint_queue = None
            if tasks:
                checkpoint_queue = tasks
            else:
                checkpoint_queue = list(self.tasks.keys())

            checkpoint_dir = '{0}/checkpoint'.format(self.run_dir)
            checkpoint_dfk = checkpoint_dir + '/dfk.pkl'
            checkpoint_tasks = checkpoint_dir + '/tasks.pkl'

            if not os.path.exists(checkpoint_dir):
                os.makedirs(checkpoint_dir, exist_ok=True)

            with open(checkpoint_dfk, 'wb') as f:
                state = {'rundir': self.run_dir,
                         'task_count': self.task_count
                         }
                pickle.dump(state, f)

            count = 0

            with open(checkpoint_tasks, 'ab') as f:
                for task_id in checkpoint_queue:
                    if task_id in self.tasks and \
                       self.tasks[task_id]['app_fu'] is not None and \
                       self.tasks[task_id]['app_fu'].done() and \
                       self.tasks[task_id]['app_fu'].exception() is None:
                        hashsum = self.tasks[task_id]['hashsum']
                        self.wipe_task(task_id)
                        # self.tasks[task_id]['app_fu'] = None
                        if not hashsum:
                            continue
                        t = {'hash': hashsum,
                             'exception': None,
                             'result': None}
                        try:
                            # Asking for the result will raise an exception if
                            # the app had failed. Should we even checkpoint these?
                            # TODO : Resolve this question ?
                            r = self.memoizer.hash_lookup(hashsum).result()
                        except Exception as e:
                            t['exception'] = e
                        else:
                            t['result'] = r

                        # We are using pickle here since pickle dumps to a file in 'ab'
                        # mode behave like a incremental log.
                        pickle.dump(t, f)
                        count += 1
                        logger.debug("Task {} checkpointed".format(task_id))

            self.checkpointed_tasks += count

            if count == 0:
                if self.checkpointed_tasks == 0:
                    logger.warning("No tasks checkpointed so far in this run. Please ensure caching is enabled")
                else:
                    logger.debug("No tasks checkpointed in this pass.")
            else:
                logger.info("Done checkpointing {} tasks".format(count))

            return checkpoint_dir

    def _load_checkpoints(self, checkpointDirs):
        """Load a checkpoint file into a lookup table.

        The data being loaded from the pickle file mostly contains input
        attributes of the task: func, args, kwargs, env...
        To simplify the check of whether the exact task has been completed
        in the checkpoint, we hash these input params and use it as the key
        for the memoized lookup table.

        Args:
            - checkpointDirs (list) : List of filepaths to checkpoints
              Eg. ['runinfo/001', 'runinfo/002']

        Returns:
            - memoized_lookup_table (dict)
        """
        memo_lookup_table = {}

        for checkpoint_dir in checkpointDirs:
            logger.info("Loading checkpoints from {}".format(checkpoint_dir))
            checkpoint_file = os.path.join(checkpoint_dir, 'tasks.pkl')
            try:
                with open(checkpoint_file, 'rb') as f:
                    while True:
                        try:
                            data = pickle.load(f)
                            # Copy and hash only the input attributes
                            memo_fu = Future()
                            if data['exception']:
                                memo_fu.set_exception(data['exception'])
                            else:
                                memo_fu.set_result(data['result'])
                            memo_lookup_table[data['hash']] = memo_fu

                        except EOFError:
                            # Done with the checkpoint file
                            break
            except FileNotFoundError:
                reason = "Checkpoint file was not found: {}".format(
                    checkpoint_file)
                logger.error(reason)
                raise BadCheckpoint(reason)
            except Exception:
                reason = "Failed to load checkpoint: {}".format(
                    checkpoint_file)
                logger.error(reason)
                raise BadCheckpoint(reason)

            logger.info("Completed loading checkpoint: {0} with {1} tasks".format(checkpoint_file,
                                                                                  len(memo_lookup_table.keys())))
        return memo_lookup_table

    def load_checkpoints(self, checkpointDirs):
        """Load checkpoints from the checkpoint files into a dictionary.

        The results are used to pre-populate the memoizer's lookup_table

        Kwargs:
             - checkpointDirs (list) : List of run folder to use as checkpoints
               Eg. ['runinfo/001', 'runinfo/002']

        Returns:
             - dict containing, hashed -> future mappings
        """
        self.memo_lookup_table = None

        if not checkpointDirs:
            return {}

        if type(checkpointDirs) is not list:
            raise BadCheckpoint("checkpointDirs expects a list of checkpoints")

        return self._load_checkpoints(checkpointDirs)


class DataFlowKernelLoader(object):
    """Manage which DataFlowKernel is active.

    This is a singleton class containing only class methods. You should not
    need to instantiate this class.
    """

    _dfk = None

    @classmethod
    def clear(cls):
        """Clear the active DataFlowKernel so that a new one can be loaded."""
        cls._dfk = None

    @classmethod
    @typeguard.typechecked
    def load(cls, config: Optional[Config] = None):
        """Load a DataFlowKernel.

        Args:
            - config (Config) : Configuration to load. This config will be passed to a
              new DataFlowKernel instantiation which will be set as the active DataFlowKernel.
        Returns:
            - DataFlowKernel : The loaded DataFlowKernel object.
        """
        if cls._dfk is not None:
            raise RuntimeError('Config has already been loaded')

        if config is None:
            cls._dfk = DataFlowKernel(Config())
        else:
            cls._dfk = DataFlowKernel(config)

        return cls._dfk

    @classmethod
    def wait_for_current_tasks(cls):
        """Waits for all tasks in the task list to be completed, by waiting for their
        AppFuture to be completed. This method will not necessarily wait for any tasks
        added after cleanup has started such as data stageout.
        """
        cls.dfk().wait_for_current_tasks()

    @classmethod
    def dfk(cls):
        """Return the currently-loaded DataFlowKernel."""
        if cls._dfk is None:
            raise RuntimeError('Must first load config')
        return cls._dfk
