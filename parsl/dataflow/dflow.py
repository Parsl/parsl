import atexit
import itertools
import logging
import os
import pickle
import random
import threading
import inspect
import sys
from datetime import datetime

from concurrent.futures import Future
from functools import partial

import libsubmit
import parsl
from parsl.app.errors import RemoteException
from parsl.config import Config
from parsl.data_provider.data_manager import DataManager
from parsl.data_provider.files import File
from parsl.dataflow.error import *
from parsl.dataflow.flow_control import FlowControl, FlowNoControl, Timer
from parsl.dataflow.futures import AppFuture
from parsl.dataflow.memoization import Memoizer
from parsl.dataflow.rundirs import make_rundir
from parsl.dataflow.states import States
from parsl.dataflow.usage_tracking.usage import UsageTracker
from parsl.utils import get_version
from parsl.app.errors import RemoteException
from parsl.monitoring import app_monitor
from parsl.monitoring.db_logger import get_db_logger

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
        logger.debug("Starting DataFlowKernel with config\n{}".format(config))
        self.run_dir = make_rundir(config.run_dir)
        parsl.set_file_logger("{}/parsl.log".format(self.run_dir),
                              level=logging.DEBUG)

        logger.info("Parsl version: {}".format(get_version()))
        logger.info("Libsubmit version: {}".format(libsubmit.__version__))

        self.checkpoint_lock = threading.Lock()

        self.usage_tracker = UsageTracker(self)
        self.usage_tracker.send_message()

        # ES logging
        self.db_logger_config = config.db_logger_config
        self.db_logger = get_db_logger(enable_es_logging=False) if self.db_logger_config is None else get_db_logger(**self.db_logger_config)
        self.workflow_name = str(inspect.stack()[1][1])
        self.time_began = datetime.now()
        self.time_completed = None
        self.run_id = self.workflow_name + "-" + str(self.time_began.minute)
        self.dashboard = self.db_logger_config.get('dashboard_link', None) if self.db_logger_config is not None else None
        # TODO: make configurable
        logger.info("Run id is: " + self.run_id)
        if self.dashboard is not None:
            logger.info("Dashboard is found at " + self.dashboard)
        self.db_logger.info("Python version: {}".format(sys.version_info))
        self.db_logger.info("Parsl version: {}".format(get_version()))
        self.db_logger.info("Libsubmit version: {}".format(libsubmit.__version__))
        self.db_logger.info("DFK start", extra={"time_began": str(self.time_began.strftime('%Y-%m-%d %H:%M:%S')),
                            'time_completed': str(self.time_completed), 'task_run_id': self.run_id, 'rundir': self.run_dir})
        self.db_logger.info("Name of script/workflow: " + self.run_id, extra={'task_run_id': self.run_id})
        for executor in self._config.executors:
            self.db_logger.info("Listed executor: " + executor.label, extra={'task_run_id': self.run_id})
        # ES logging end

        checkpoints = self.load_checkpoints(config.checkpoint_files)
        self.memoizer = Memoizer(self, memoize=config.app_cache, checkpoint=checkpoints)
        self.checkpointed_tasks = 0
        self._checkpoint_timer = None
        self.checkpoint_mode = config.checkpoint_mode

        data_manager = DataManager.get_data_manager(
            max_threads=config.data_management_max_threads,
            executors=config.executors
        )
        self.executors = {e.label: e for e in config.executors + [data_manager]}
        for executor in self.executors.values():
            executor.run_dir = self.run_dir  # FIXME we should have a real interface for this
            executor.start()

        if self.checkpoint_mode == "periodic":
            try:
                h, m, s = map(int, config.checkpoint_period.split(':'))
                checkpoint_period = (h * 3600) + (m * 60) + s
                self._checkpoint_timer = Timer(self.checkpoint, interval=checkpoint_period)
            except Exception as e:
                logger.error("invalid checkpoint_period provided:{0} expected HH:MM:SS".format(period))
                self._checkpoint_timer = Timer(self.checkpoint, interval=(30 * 60))

        if any([x.managed for x in config.executors]):
            self.flowcontrol = FlowControl(self)
        else:
            self.flowcontrol = FlowNoControl(self)

        self.task_count = 0
        self.fut_task_lookup = {}
        self.tasks = {}
        self.task_launch_lock = threading.Lock()

        atexit.register(self.atexit_cleanup)

    @staticmethod
    def _count_deps(depends, task_id):
        """Internal.

        Count the number of unresolved futures in the list depends.
        """
        count = 0
        for dep in depends:
            if isinstance(dep, Future) or issubclass(type(dep), Future):
                if not dep.done():
                    count += 1

        return count

    @property
    def config(self):
        """Returns the fully initialized config that the DFK is actively using.

        DO *NOT* update.

        Returns:
             - config (dict)
        """
        return self._config

    def handle_update(self, task_id, future, memo_cbk=False):
        """This function is called only as a callback from a task being done.

        Move done task from runnable -> done
        Move newly doable tasks from pending -> runnable , and launch

        Args:
             task_id (string) : Task id which is a uuid string
             future (Future) : The future object corresponding to the task which
             makes this callback

        KWargs:
             memo_cbk(Bool) : Indicates that the call is coming from a memo update,
             that does not require additional memo updates.
        """
        final_state_flag = False

        try:
            res = future.result()
            if isinstance(res, RemoteException):
                res.reraise()

        except Exception as e:
            logger.exception("Task {} failed".format(task_id))

            # We keep the history separately, since the future itself could be
            # tossed.
            self.tasks[task_id]['fail_history'].append(future._exception)
            self.tasks[task_id]['fail_count'] += 1

            if not self._config.lazy_errors:
                logger.debug("Eager fail, skipping retry logic")
                self.tasks[task_id]['status'] = States.failed
                if self.db_logger_config is not None and self.db_logger_config.get('enable_es_logging', False):
                    task_log_info = {"task_" + k: v for k, v in self.tasks[task_id].items()}
                    task_log_info['task_status_name'] = self.tasks[task_id]['status'].name
                    task_log_info['task_fail_mode'] = 'eager'
                    self.db_logger.info("Task Fail", extra=task_log_info)
                raise e

            if self.tasks[task_id]['fail_count'] <= self._config.retries:
                self.tasks[task_id]['status'] = States.pending
                logger.debug("Task {} marked for retry".format(task_id))
                if self.db_logger_config is not None and self.db_logger_config.get('enable_es_logging', False):
                    task_log_info = {'task_' + k: v for k, v in self.tasks[task_id].items()}
                    task_log_info['task_status_name'] = self.tasks[task_id]['status'].name
                    task_log_info['task_' + 'fail_mode'] = 'lazy'
                    self.db_logger.info("Task Retry", extra=task_log_info)

            else:
                logger.info("Task {} failed after {} retry attempts".format(task_id,
                                                                            self._config.retries))
                self.tasks[task_id]['status'] = States.failed
                final_state_flag = True

                if self.db_logger_config is not None and self.db_logger_config.get('enable_es_logging', False):
                    task_log_info = {'task_' + k: v for k, v in self.tasks[task_id].items()}
                    task_log_info['task_status_name'] = self.tasks[task_id]['status'].name
                    task_log_info['task_' + 'fail_mode'] = 'lazy'
                    self.db_logger.info("Task Retry Failed", extra=task_log_info)

        else:
            self.tasks[task_id]['status'] = States.done
            final_state_flag = True

            logger.info("Task {} completed".format(task_id))
            self.tasks[task_id]['time_completed'] = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            if self.db_logger_config is not None and self.db_logger_config.get('enable_es_logging', False):
                task_log_info = {'task_' + k: v for k, v in self.tasks[task_id].items()}
                task_log_info['task_status_name'] = self.tasks[task_id]['status'].name
                self.db_logger.info("Task Done", extra=task_log_info)

        if not memo_cbk and final_state_flag is True:
            # Update the memoizer with the new result if this is not a
            # result from a memo lookup and the task has reached a terminal state.
            self.memoizer.update_memo(task_id, self.tasks[task_id], future)

            if self.checkpoint_mode is 'task_exit':
                self.checkpoint(tasks=[task_id])

        # Submit _*_stage_out tasks for output data futures that correspond with remote files
        if (self.tasks[task_id]['app_fu'] and
                self.tasks[task_id]['status'] == States.done and
                self.tasks[task_id]['executor'] != 'data_manager' and
                self.tasks[task_id]['func_name'] != '_file_stage_in' and
                self.tasks[task_id]['func_name'] != '_ftp_stage_in' and
                self.tasks[task_id]['func_name'] != '_http_stage_in'):
            for dfu in self.tasks[task_id]['app_fu'].outputs:
                f = dfu.file_obj
                if isinstance(f, File) and f.is_remote():
                    f.stage_out(self.tasks[task_id]['executor'])

        # Identify tasks that have resolved dependencies and launch
        for tid in list(self.tasks):
            # Skip all non-pending tasks
            if self.tasks[tid]['status'] != States.pending:
                continue

            if self._count_deps(self.tasks[tid]['depends'], tid) == 0:
                # We can now launch *task*
                new_args, kwargs, exceptions = self.sanitize_and_wrap(task_id,
                                                                      self.tasks[tid]['args'],
                                                                      self.tasks[tid]['kwargs'])
                self.tasks[tid]['args'] = new_args
                self.tasks[tid]['kwargs'] = kwargs
                if not exceptions:
                    # There are no dependency errors
                    exec_fu = None
                    # Acquire a lock, retest the state, launch
                    with self.task_launch_lock:
                        if self.tasks[tid]['status'] == States.pending:
                            self.tasks[tid]['status'] = States.running
                            exec_fu = self.launch_task(
                                tid, self.tasks[tid]['func'], *new_args, **kwargs)

                    if exec_fu:
                        self.tasks[task_id]['exec_fu'] = exec_fu
                        try:
                            self.tasks[tid]['app_fu'].update_parent(exec_fu)
                            self.tasks[tid]['exec_fu'] = exec_fu
                        except AttributeError as e:
                            logger.error(
                                "Task {}: Caught AttributeError at update_parent".format(tid))
                            raise e
                else:
                    logger.info(
                        "Task {} deferred due to dependency failure".format(tid))
                    # Raise a dependency exception
                    self.tasks[tid]['status'] = States.dep_fail
                    if self.db_logger_config is not None and self.db_logger_config.get('enable_es_logging', False):
                        task_log_info = {'task_' + k: v for k, v in self.tasks[task_id].items()}
                        task_log_info['task_status_name'] = self.tasks[task_id]['status'].name
                        task_log_info['task_' + 'fail_mode'] = 'lazy'
                        self.db_logger.info("Task Dep Fail", extra=task_log_info)

                    try:
                        fu = Future()
                        fu.retries_left = 0
                        self.tasks[tid]['exec_fu'] = fu
                        self.tasks[tid]['app_fu'].update_parent(fu)
                        fu.set_exception(DependencyError(exceptions,
                                                         tid,
                                                         None))

                    except AttributeError as e:
                        logger.error(
                            "Task {} AttributeError at update_parent".format(tid))
                        raise e

        return

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
        hit, memo_fu = self.memoizer.check_memo(task_id, self.tasks[task_id])
        if hit:
            self.handle_update(task_id, memo_fu, memo_cbk=True)
            return memo_fu

        executor_label = self.tasks[task_id]["executor"]
        try:
            executor = self.executors[executor_label]
        except Exception as e:
            logger.exception("Task {} requested invalid executor {}: config is\n{}".format(task_id, executor_label, self._config))
        if self.db_logger_config is not None and self.db_logger_config.get('enable_remote_monitoring', False):
            executable = app_monitor.monitor_wrapper(executable, task_id, self.db_logger_config, self.run_id)
        exec_fu = executor.submit(executable, *args, **kwargs)
        self.tasks[task_id]['status'] = States.running
        self.tasks[task_id]['time_started'] = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        if self.db_logger_config is not None and self.db_logger_config.get('enable_es_logging', False):
            task_log_info = {'task_' + k: v for k, v in self.tasks[task_id].items()}
            task_log_info['task_status_name'] = self.tasks[task_id]['status'].name
            self.db_logger.info("Task Launch", extra=task_log_info)
        exec_fu.retries_left = self._config.retries - \
            self.tasks[task_id]['fail_count']
        exec_fu.add_done_callback(partial(self.handle_update, task_id))
        logger.info("Task {} launched on executor {}".format(task_id, executor.label))
        return exec_fu

    def _add_input_deps(self, executor, args, kwargs):
        """Look for inputs of the app that are remote files. Submit stage_in
        apps for such files and replace the file objects in the inputs list with
        corresponding DataFuture objects.

        Args:
            - executor (str) : executor where the app is going to be launched
            - args (List) : Positional args to app function
            - kwargs (Dict) : Kwargs to app function
        """

        # Return if the task is _*_stage_in
        if executor == 'data_manager':
            return

        inputs = kwargs.get('inputs', [])
        for idx, f in enumerate(inputs):
            if isinstance(f, File) and f.is_remote():
                inputs[idx] = f.stage_in(executor)

    @staticmethod
    def _count_all_deps(task_id, args, kwargs):
        """Internal.

        Count the number of unresolved futures in the list depends.

        Args:
            - task_id (uuid string) : Task_id
            - args (List[args]) : The list of args list to the fn
            - kwargs (Dict{kwargs}) : The dict of all kwargs passed to the fn

        Returns:
            - count, [list of dependencies]

        """
        # Check the positional args
        depends = []
        count = 0
        for dep in args:
            if isinstance(dep, Future) or issubclass(dep.__class__, Future):
                if not dep.done():
                    count += 1
                depends.extend([dep])

        # Check for explicit kwargs ex, fu_1=<fut>
        for key in kwargs:
            dep = kwargs[key]
            if isinstance(dep, Future) or issubclass(dep.__class__, Future):
                if not dep.done():
                    count += 1
                depends.extend([dep])

        # Check for futures in inputs=[<fut>...]
        for dep in kwargs.get('inputs', []):
            if issubclass(dep.__class__, Future) or isinstance(dep, Future):
                if not dep.done():
                    count += 1
                depends.extend([dep])

        # logger.debug("Task:{0}   dep_cnt:{1}  deps:{2}".format(task_id, count, depends))
        return count, depends

    @staticmethod
    def sanitize_and_wrap(task_id, args, kwargs):
        """This function should be called **ONLY** when all the futures we track have been resolved.

        If the user hid futures a level below, we will not catch
        it, and will (most likely) result in a type error .

        Args:
             task_id (uuid str) : Task id
             func (Function) : App function
             args (List) : Positional args to app function
             kwargs (Dict) : Kwargs to app function

        Return:
             partial Function evaluated with all dependencies in  args, kwargs and kwargs['inputs'] evaluated.

        """
        dep_failures = []

        # Replace item in args
        new_args = []
        for dep in args:
            if isinstance(dep, Future) or issubclass(type(dep), Future):
                try:
                    new_args.extend([dep.result()])
                except Exception as e:
                    dep_failures.extend([e])
            else:
                new_args.extend([dep])

        # Check for explicit kwargs ex, fu_1=<fut>
        for key in kwargs:
            dep = kwargs[key]
            if isinstance(dep, Future) or issubclass(type(dep), Future):
                try:
                    kwargs[key] = dep.result()
                except Exception as e:
                    dep_failures.extend([e])

        # Check for futures in inputs=[<fut>...]
        if 'inputs' in kwargs:
            new_inputs = []
            for dep in kwargs['inputs']:
                if isinstance(dep, Future) or issubclass(type(dep), Future):
                    try:
                        new_inputs.extend([dep.result()])
                    except Exception as e:
                        dep_failures.extend([e])

                else:
                    new_inputs.extend([dep])
            kwargs['inputs'] = new_inputs

        return new_args, kwargs, dep_failures

    def submit(self, func, *args, executors='all', fn_hash=None, cache=False, **kwargs):
        """Add task to the dataflow system.

        If the app task has the executors attributes not set (default=='all')
        the task will be launched on a randomly selected executor from the
        list of executors. This behavior could later be updated to support
        binding to executors based on user specified criteria.

        If the app task specifies a particular set of executors, it will be
        targetted at those specific executors.

        >>> IF all deps are met:
        >>>   send to the runnable queue and launch the task
        >>> ELSE:
        >>>   post the task in the pending queue

        Args:
            - func : A function object
            - *args : Args to the function

        KWargs :
            - executors (list or string) : List of executors this call could go to.
                    Default='all'
            - fn_hash (Str) : Hash of the function and inputs
                    Default=None
            - cache (Bool) : To enable memoization or not
            - kwargs (dict) : Rest of the kwargs to the fn passed as dict.

        Returns:
               (AppFuture) [DataFutures,]

        """
        task_id = self.task_count
        self.task_count += 1
        if isinstance(executors, str) and executors.lower() == 'all':
            choices = list(e for e in self.executors if e != 'data_manager')
        elif isinstance(executors, list):
            choices = executors
        executor = random.choice(choices)

        task_def = {'depends': None,
                    'executor': executor,
                    'func': func,
                    'func_name': func.__name__,
                    'args': args,
                    'kwargs': kwargs,
                    'fn_hash': fn_hash,
                    'memoize': cache,
                    'callback': None,
                    'dep_cnt': None,
                    'exec_fu': None,
                    'checkpoint': None,
                    'fail_count': 0,
                    'fail_history': [],
                    'env': None,
                    'status': States.unsched,
                    'id': task_id,
                    'time_started': None,
                    'time_completed': None,
                    'run_id': self.run_id,
                    'app_fu': None}

        if task_id in self.tasks:
            raise DuplicateTaskError(
                "Task {0} in pending list".format(task_id))
        else:
            self.tasks[task_id] = task_def

        # Transform remote input files to data futures
        self._add_input_deps(executor, args, kwargs)

        # Get the dep count and a list of dependencies for the task
        dep_cnt, depends = self._count_all_deps(task_id, args, kwargs)
        self.tasks[task_id]['dep_cnt'] = dep_cnt
        self.tasks[task_id]['depends'] = depends

        # Extract stdout and stderr to pass to AppFuture:
        task_stdout = kwargs.get('stdout')
        task_stderr = kwargs.get('stderr')

        logger.info("Task {} submitted for App {}, waiting on tasks {}".format(task_id,
                                                                               task_def['func_name'],
                                                                               [fu.tid for fu in depends]))

        # Handle three cases here:
        # No pending deps
        #     - But has failures -> dep_fail
        #     - No failures -> running
        # Has pending deps -> pending
        if dep_cnt == 0:

            new_args, kwargs, exceptions = self.sanitize_and_wrap(
                task_id, args, kwargs)
            self.tasks[task_id]['args'] = new_args
            self.tasks[task_id]['kwargs'] = kwargs

            if not exceptions:
                self.tasks[task_id]['exec_fu'] = self.launch_task(
                    task_id, func, *new_args, **kwargs)
                self.tasks[task_id]['app_fu'] = AppFuture(self.tasks[task_id]['exec_fu'],
                                                          tid=task_id,
                                                          stdout=task_stdout,
                                                          stderr=task_stderr)
                logger.debug("Task {} launched with AppFuture: {}".format(task_id, task_def['app_fu']))

            else:
                fu = Future()
                fu.set_exception(DependencyError(exceptions,
                                                 "Failures in input dependencies",
                                                 None))
                fu.retries_left = 0
                self.tasks[task_id]['exec_fu'] = fu
                app_fu = AppFuture(self.tasks[task_id]['exec_fu'],
                                   tid=task_id,
                                   stdout=task_stdout,
                                   stderr=task_stderr)
                self.tasks[task_id]['app_fu'] = app_fu
                self.tasks[task_id]['status'] = States.dep_fail
                logger.debug("Task {} failed due to failure in parent task(s):{}".format(task_id,
                                                                                         task_def['app_fu']))

        else:
            # Send to pending, create the AppFuture with no parent and have it set
            # when an executor future is available.
            self.tasks[task_id]['app_fu'] = AppFuture(None, tid=task_id,
                                                      stdout=task_stdout,
                                                      stderr=task_stderr)
            self.tasks[task_id]['status'] = States.pending
            logger.debug("Task {} launched with AppFuture: {}".format(task_id, task_def['app_fu']))

        return task_def['app_fu']

    # it might also be interesting to assert that all DFK
    # tasks are in a "final" state (3,4,5) when the DFK
    # is closed down, and report some kind of warning.
    # although really I'd like this to drain properly...
    # and a drain function might look like this.
    # If tasks have their states changed, this won't work properly
    # but we can validate that...
    def log_task_states(self):
        logger.info("Summary of tasks in DFK:")

        total_summarised = 0

        keytasks = []
        for tid in self.tasks:
            keytasks.append((self.tasks[tid]['status'], tid))

        def first(t):
            return t[0]

        sorted_keytasks = sorted(keytasks, key=first)

        grouped_sorted_keytasks = itertools.groupby(sorted_keytasks, key=first)

        # caution: g is an iterator that also advances the
        # grouped_sorted_tasks iterator, so looping over
        # both grouped_sorted_keytasks and g can only be done
        # in certain patterns

        for k, g in grouped_sorted_keytasks:

            ts = []

            for t in g:
                tid = t[1]
                ts.append(str(tid))
                total_summarised = total_summarised + 1

            tids_string = ", ".join(ts)

            logger.info("Tasks in state {}: {}".format(str(k), tids_string))

        total_in_tasks = len(self.tasks)
        if total_summarised != total_in_tasks:
            logger.error("Task count summarisation was inconsistent: summarised {} tasks, but tasks list contains {} tasks".format(
                total_summarised, total_in_tasks))

        logger.info("End of summary")

    def atexit_cleanup(self):
        if not self.cleanup_called:
            self.cleanup()

    def cleanup(self):
        """DataFlowKernel cleanup.

        This involves killing resources explicitly and sending die messages to IPP workers.

        If the executors are managed (created by the DFK), then we call scale_in on each of
        the executors and call executor.shutdown. Otherwise, we do nothing, and executor
        cleanup is left to the user.
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
            if executor.managed:
                if executor.scaling_enabled:
                    job_ids = executor.provider.resources.keys()
                    executor.scale_in(len(job_ids))
                executor.shutdown()

        self.time_completed = datetime.now()
        self.db_logger.info("DFK end", extra={"time_began": str(self.time_began.strftime('%Y-%m-%d %H:%M:%S')),
                            'time_completed': str(self.time_completed.strftime('%Y-%m-%d %H:%M:%S')), 'task_run_id': self.run_id, 'rundir': self.run_dir})
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
                checkpoint_queue = self.tasks

            checkpoint_dir = '{0}/checkpoint'.format(self.run_dir)
            checkpoint_dfk = checkpoint_dir + '/dfk.pkl'
            checkpoint_tasks = checkpoint_dir + '/tasks.pkl'

            if not os.path.exists(checkpoint_dir):
                try:
                    os.makedirs(checkpoint_dir)
                except FileExistsError as e:
                    pass

            with open(checkpoint_dfk, 'wb') as f:
                state = {'rundir': self.run_dir,
                         'task_count': self.task_count
                         }
                pickle.dump(state, f)

            count = 0

            with open(checkpoint_tasks, 'ab') as f:
                for task_id in checkpoint_queue:
                    if not self.tasks[task_id]['checkpoint'] and \
                       self.tasks[task_id]['status'] == States.done:
                        hashsum = self.tasks[task_id]['hashsum']
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
                        self.tasks[task_id]['checkpoint'] = True
                        logger.debug("Task {} checkpointed".format(task_id))

            self.checkpointed_tasks += count

            if count == 0:
                if self.checkpointed_tasks == 0:
                    logger.warn("No tasks checkpointed so far in this run. Please ensure caching is enabled")
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
            except Exception as e:
                reason = "Failed to load checkpoint: {}".format(
                    checkpoint_file)
                logger.error(reason)
                raise BadCheckpoint(reason)

            logger.info("Completed loading checkpoint:{0} with {1} tasks".format(checkpoint_file,
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
    def load(cls, config):
        """Load a DataFlowKernel.

        Args:
            - config (Config) : Configuration to load. This config will be passed to a
              new DataFlowKernel instantiation which will be set as the active DataFlowKernel.
        Returns:
            - DataFlowKernel : The loaded DataFlowKernel object.
        """
        if cls._dfk is not None:
            raise RuntimeError('Config has already been loaded')
        cls._dfk = DataFlowKernel(config)

        return cls._dfk

    @classmethod
    def dfk(cls):
        """Return the currently-loaded DataFlowKernel."""
        if cls._dfk is None:
            raise RuntimeError('Must first load config')
        return cls._dfk
