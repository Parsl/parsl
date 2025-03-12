from __future__ import annotations

import atexit
import concurrent.futures as cf
import datetime
import inspect
import logging
import os
import pickle
import random
import sys
import threading
import time
from concurrent.futures import Future
from functools import partial
from getpass import getuser
from socket import gethostname
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union
from uuid import uuid4

import typeguard
from typeguard import typechecked

import parsl
from parsl.app.errors import RemoteExceptionWrapper
from parsl.app.futures import DataFuture
from parsl.config import Config
from parsl.data_provider.data_manager import DataManager
from parsl.data_provider.files import File
from parsl.dataflow.dependency_resolvers import SHALLOW_DEPENDENCY_RESOLVER
from parsl.dataflow.errors import DependencyError, JoinError
from parsl.dataflow.futures import AppFuture
from parsl.dataflow.memoization import Memoizer
from parsl.dataflow.rundirs import make_rundir
from parsl.dataflow.states import FINAL_FAILURE_STATES, FINAL_STATES, States
from parsl.dataflow.taskrecord import TaskRecord
from parsl.errors import (
    ConfigurationError,
    InternalConsistencyError,
    NoDataFlowKernelError,
)
from parsl.executors.base import ParslExecutor
from parsl.executors.status_handling import BlockProviderExecutor
from parsl.executors.threads import ThreadPoolExecutor
from parsl.jobs.job_status_poller import JobStatusPoller
from parsl.monitoring import MonitoringHub
from parsl.monitoring.message_type import MessageType
from parsl.monitoring.remote import monitor_wrapper
from parsl.process_loggers import wrap_with_logs
from parsl.usage_tracking.usage import UsageTracker
from parsl.utils import Timer, get_all_checkpoints, get_std_fname_mode, get_version

logger = logging.getLogger(__name__)


class DataFlowKernel:
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

    @typechecked
    def __init__(self, config: Config) -> None:
        """Initialize the DataFlowKernel.

        Parameters
        ----------
        config : Config
            A specification of all configuration options. For more details see the
            :class:~`parsl.config.Config` documentation.
        """

        # this will be used to check cleanup only happens once
        self.cleanup_called = False

        self._config = config
        self.run_dir = make_rundir(config.run_dir)

        if config.initialize_logging:
            parsl.set_file_logger("{}/parsl.log".format(self.run_dir), level=logging.DEBUG)

        logger.info("Starting DataFlowKernel with config\n{}".format(config))

        logger.info("Parsl version: {}".format(get_version()))

        self.checkpoint_lock = threading.Lock()

        self.usage_tracker = UsageTracker(self)
        self.usage_tracker.send_start_message()

        self.task_state_counts_lock = threading.Lock()
        self.task_state_counts = {state: 0 for state in States}

        # Monitoring
        self.run_id = str(uuid4())

        self.monitoring: Optional[MonitoringHub]
        self.monitoring = config.monitoring

        if self.monitoring:
            self.monitoring.start(self.run_dir, self.config.run_dir)

        self.time_began = datetime.datetime.now()
        self.time_completed: Optional[datetime.datetime] = None

        logger.info("Run id is: " + self.run_id)

        self.workflow_name = None
        if self.monitoring is not None and self.monitoring.workflow_name is not None:
            self.workflow_name = self.monitoring.workflow_name
        else:
            for frame in inspect.stack():
                logger.debug("Considering candidate for workflow name: {}".format(frame.filename))
                fname = os.path.basename(str(frame.filename))
                parsl_file_names = ['dflow.py', 'typeguard.py', '__init__.py']
                # Find first file name not considered a parsl file
                if fname not in parsl_file_names:
                    self.workflow_name = fname
                    logger.debug("Using {} as workflow name".format(fname))
                    break
            else:
                logger.debug("Could not choose a name automatically")
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
                'run_id': self.run_id,
                'workflow_name': self.workflow_name,
                'workflow_version': self.workflow_version,
                'rundir': self.run_dir,
                'tasks_completed_count': self.task_state_counts[States.exec_done],
                'tasks_failed_count': self.task_state_counts[States.failed],
                'user': getuser(),
                'host': gethostname(),
        }

        if self.monitoring:
            self.monitoring.send((MessageType.WORKFLOW_INFO,
                                 workflow_info))

        if config.checkpoint_files is not None:
            checkpoint_files = config.checkpoint_files
        elif config.checkpoint_files is None and config.checkpoint_mode is not None:
            checkpoint_files = get_all_checkpoints(self.run_dir)
        else:
            checkpoint_files = []

        self.memoizer = Memoizer(self, memoize=config.app_cache, checkpoint_files=checkpoint_files)
        self.checkpointed_tasks = 0
        self._checkpoint_timer = None
        self.checkpoint_mode = config.checkpoint_mode
        self.checkpointable_tasks: List[TaskRecord] = []

        # this must be set before executors are added since add_executors calls
        # job_status_poller.add_executors.
        self.job_status_poller = JobStatusPoller(strategy=self.config.strategy,
                                                 strategy_period=self.config.strategy_period,
                                                 max_idletime=self.config.max_idletime)

        self.executors: Dict[str, ParslExecutor] = {}

        self.data_manager = DataManager(self)
        parsl_internal_executor = ThreadPoolExecutor(max_threads=config.internal_tasks_max_threads, label='_parsl_internal')
        self.add_executors(config.executors)
        self.add_executors([parsl_internal_executor])

        if self.checkpoint_mode == "periodic":
            if config.checkpoint_period is None:
                raise ConfigurationError("Checkpoint period must be specified with periodic checkpoint mode")
            else:
                try:
                    h, m, s = map(int, config.checkpoint_period.split(':'))
                except Exception:
                    raise ConfigurationError("invalid checkpoint_period provided: {0} expected HH:MM:SS".format(config.checkpoint_period))
                checkpoint_period = (h * 3600) + (m * 60) + s
                self._checkpoint_timer = Timer(self.checkpoint, interval=checkpoint_period, name="Checkpoint")

        self.task_count = 0
        self.tasks: Dict[int, TaskRecord] = {}
        self.submitter_lock = threading.Lock()

        self.dependency_launch_pool = cf.ThreadPoolExecutor(max_workers=1, thread_name_prefix="Dependency-Launch")

        self.dependency_resolver = self.config.dependency_resolver if self.config.dependency_resolver is not None \
            else SHALLOW_DEPENDENCY_RESOLVER

        atexit.register(self.atexit_cleanup)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        mode = self.config.exit_mode
        logger.debug("Exiting context manager, with exit mode '%s'", mode)
        if mode == "cleanup":
            logger.info("Calling cleanup for DFK")
            self.cleanup()
        elif mode == "skip":
            logger.info("Skipping all cleanup handling")
        elif mode == "wait":
            if exc_type is None:
                logger.info("Waiting for all tasks to complete")
                self.wait_for_current_tasks()
                self.cleanup()
            else:
                logger.info("There was an exception - cleaning up without waiting for task completion")
                self.cleanup()
        else:
            raise InternalConsistencyError(f"Exit case for {mode} should be unreachable, validated by typeguard on Config()")

    def _send_task_log_info(self, task_record: TaskRecord) -> None:
        if self.monitoring:
            task_log_info = self._create_task_log_info(task_record)
            self.monitoring.send((MessageType.TASK_INFO, task_log_info))

    def _create_task_log_info(self, task_record: TaskRecord) -> Dict[str, Any]:
        """
        Create the dictionary that will be included in the log.
        """
        info_to_monitor = ['func_name', 'memoize', 'hashsum', 'fail_count', 'fail_cost', 'status',
                           'id', 'time_invoked', 'try_time_launched', 'time_returned', 'try_time_returned', 'executor']

        # mypy cannot verify that these task_record[k] references are valid:
        # They are valid if all entries in info_to_monitor are declared in the definition of TaskRecord
        # This type: ignore[literal-required] asserts that fact.
        task_log_info = {"task_" + k: task_record[k] for k in info_to_monitor}  # type: ignore[literal-required]

        task_log_info['run_id'] = self.run_id
        task_log_info['try_id'] = task_record['try_id']
        task_log_info['timestamp'] = datetime.datetime.now()
        task_log_info['task_status_name'] = task_record['status'].name
        task_log_info['tasks_failed_count'] = self.task_state_counts[States.failed]
        task_log_info['tasks_completed_count'] = self.task_state_counts[States.exec_done]
        task_log_info['tasks_memo_completed_count'] = self.task_state_counts[States.memo_done]
        task_log_info['from_memo'] = task_record['from_memo']
        task_log_info['task_inputs'] = str(task_record['kwargs'].get('inputs', None))
        task_log_info['task_outputs'] = str(task_record['kwargs'].get('outputs', None))
        task_log_info['task_stdin'] = task_record['kwargs'].get('stdin', None)

        def std_spec_to_name(name, spec):
            if spec is None:
                name = ""
            elif isinstance(spec, File):
                name = spec.url
            else:
                # fallthrough case is various str, os.PathLike, tuple modes that
                # can be interpreted by get_std_fname_mode.
                try:
                    name, _ = get_std_fname_mode(name, spec)
                except Exception:
                    logger.exception(f"Could not parse {name} specification {spec} for task {task_record['id']}")
                    name = ""
            return name

        stdout_spec = task_record['kwargs'].get('stdout')
        task_log_info['task_stdout'] = std_spec_to_name('stdout', stdout_spec)

        stderr_spec = task_record['kwargs'].get('stderr')
        task_log_info['task_stderr'] = std_spec_to_name('stderr', stderr_spec)

        task_log_info['task_fail_history'] = ",".join(task_record['fail_history'])
        task_log_info['task_depends'] = None
        if task_record['depends'] is not None:
            task_log_info['task_depends'] = ",".join([str(t.tid) for t in task_record['depends']
                                                      if isinstance(t, AppFuture) or isinstance(t, DataFuture)])
        task_log_info['task_joins'] = None

        if isinstance(task_record['joins'], list):
            task_log_info['task_joins'] = ",".join([str(t.tid) for t in task_record['joins']
                                                    if isinstance(t, AppFuture) or isinstance(t, DataFuture)])
        elif isinstance(task_record['joins'], Future):
            task_log_info['task_joins'] = ",".join([str(t.tid) for t in [task_record['joins']]
                                                    if isinstance(t, AppFuture) or isinstance(t, DataFuture)])

        return task_log_info

    def _count_deps(self, depends: Sequence[Future]) -> int:
        """Count the number of unresolved futures in the list depends.
        """
        count = 0
        for dep in depends:
            if not dep.done():
                count += 1

        return count

    @property
    def config(self) -> Config:
        """Returns the fully initialized config that the DFK is actively using.

        Returns:
             - Config object
        """
        return self._config

    def handle_exec_update(self, task_record: TaskRecord, future: Future) -> None:
        """This function is called only as a callback from an execution
        attempt reaching a final state (either successfully or failing).

        It will launch retries if necessary, and update the task
        structure.

        Args:
             task_record (dict) : Task record
             future (Future) : The future object corresponding to the task which
             makes this callback
        """

        task_id = task_record['id']

        task_record['try_time_returned'] = datetime.datetime.now()

        if not future.done():
            raise InternalConsistencyError("done callback called, despite future not reporting itself as done")

        try:
            res = self._unwrap_remote_exception_wrapper(future)

        except Exception as e:
            logger.info(f"Task {task_id} try {task_record['try_id']} failed with exception of type {type(e).__name__}")
            # We keep the history separately, since the future itself could be
            # tossed.
            task_record['fail_history'].append(repr(e))
            task_record['fail_count'] += 1
            if self._config.retry_handler:
                try:
                    cost = self._config.retry_handler(e, task_record)
                except Exception as retry_handler_exception:
                    logger.exception("retry_handler raised an exception - will not retry")

                    # this can be any amount > self._config.retries, to stop any more
                    # retries from happening
                    task_record['fail_cost'] = self._config.retries + 1

                    # make the reported exception be the retry handler's exception,
                    # rather than the execution level exception
                    e = retry_handler_exception
                else:
                    task_record['fail_cost'] += cost
            else:
                task_record['fail_cost'] += 1

            if task_record['status'] == States.dep_fail:
                logger.info("Task {} failed due to dependency failure so skipping retries".format(task_id))
                task_record['time_returned'] = datetime.datetime.now()
                self._send_task_log_info(task_record)
                with task_record['app_fu']._update_lock:
                    task_record['app_fu'].set_exception(e)

            elif task_record['fail_cost'] <= self._config.retries:

                # record the final state for this try before we mutate for retries
                self.update_task_state(task_record, States.fail_retryable)
                self._send_task_log_info(task_record)

                task_record['try_id'] += 1
                self.update_task_state(task_record, States.pending)
                task_record['try_time_launched'] = None
                task_record['try_time_returned'] = None
                task_record['fail_history'] = []
                self._send_task_log_info(task_record)

                logger.info("Task {} marked for retry".format(task_id))

            else:
                logger.exception("Task {} failed after {} retry attempts".format(task_id,
                                                                                 task_record['try_id']))
                task_record['time_returned'] = datetime.datetime.now()
                self.update_task_state(task_record, States.failed)
                task_record['time_returned'] = datetime.datetime.now()
                self._send_task_log_info(task_record)
                with task_record['app_fu']._update_lock:
                    task_record['app_fu'].set_exception(e)

        else:
            if task_record['from_memo']:
                self._complete_task(task_record, States.memo_done, res)
                self._send_task_log_info(task_record)
            else:
                if not task_record['join']:
                    self._complete_task(task_record, States.exec_done, res)
                    self._send_task_log_info(task_record)
                else:
                    # This is a join task, and the original task's function code has
                    # completed. That means that the future returned by that code
                    # will be available inside the executor future, so we can now
                    # record the inner app ID in monitoring, and add a completion
                    # listener to that inner future.

                    joinable = future.result()

                    # Fail with a TypeError if the joinapp python body returned
                    # something we can't join on.
                    if isinstance(joinable, Future):
                        self.update_task_state(task_record, States.joining)
                        task_record['joins'] = joinable
                        task_record['join_lock'] = threading.Lock()
                        self._send_task_log_info(task_record)
                        joinable.add_done_callback(partial(self.handle_join_update, task_record))
                    elif joinable == []:  # got a list, but it had no entries, and specifically, no Futures.
                        self.update_task_state(task_record, States.joining)
                        task_record['joins'] = joinable
                        task_record['join_lock'] = threading.Lock()
                        self._send_task_log_info(task_record)
                        self.handle_join_update(task_record, None)
                    elif isinstance(joinable, list) and [j for j in joinable if not isinstance(j, Future)] == []:
                        self.update_task_state(task_record, States.joining)
                        task_record['joins'] = joinable
                        task_record['join_lock'] = threading.Lock()
                        self._send_task_log_info(task_record)
                        for inner_future in joinable:
                            inner_future.add_done_callback(partial(self.handle_join_update, task_record))
                    else:
                        task_record['time_returned'] = datetime.datetime.now()
                        self.update_task_state(task_record, States.failed)
                        task_record['time_returned'] = datetime.datetime.now()
                        self._send_task_log_info(task_record)
                        with task_record['app_fu']._update_lock:
                            task_record['app_fu'].set_exception(
                                TypeError(f"join_app body must return a Future or list of Futures, got {joinable} of type {type(joinable)}"))

        self._log_std_streams(task_record)

        # it might be that in the course of the update, we've gone back to being
        # pending - in which case, we should consider ourself for relaunch
        if task_record['status'] == States.pending:
            self.launch_if_ready(task_record)

    def handle_join_update(self, task_record: TaskRecord, inner_app_future: Optional[AppFuture]) -> None:
        with task_record['join_lock']:
            # inner_app_future has completed, which is one (potentially of many)
            # futures the outer task is joining on.

            # If the outer task is joining on a single future, then
            # use the result of the inner_app_future as the final result of
            # the outer app future.

            # If the outer task is joining on a list of futures, then
            # check if the list is all done, and if so, return a list
            # of the results. Otherwise, this callback can do nothing and
            # processing will happen in another callback (on the final Future
            # to complete)

            # There is no retry handling here: inner apps are responsible for
            # their own retrying, and joining state is responsible for passing
            # on whatever the result of that retrying was (if any).

            outer_task_id = task_record['id']
            logger.debug(f"Join callback for task {outer_task_id}, inner_app_future {inner_app_future}")

            if task_record['status'] != States.joining:
                logger.debug(f"Join callback for task {outer_task_id} skipping because task is not in joining state")
                return

            joinable = task_record['joins']

            if isinstance(joinable, list):
                for future in joinable:
                    if not future.done():
                        logger.debug(f"A joinable future {future} is not done for task {outer_task_id} - skipping callback")
                        return  # abandon this callback processing if joinables are not all done

            # now we know each joinable Future is done
            # so now look for any exceptions
            exceptions_tids: List[Tuple[BaseException, str]]
            exceptions_tids = []
            if isinstance(joinable, Future):
                je = joinable.exception()
                if je is not None:
                    tid = self.render_future_description(joinable)
                    exceptions_tids = [(je, tid)]
            elif isinstance(joinable, list):
                for future in joinable:
                    je = future.exception()
                    if je is not None:
                        tid = self.render_future_description(future)
                        exceptions_tids.append((je, tid))
            else:
                raise TypeError(f"Unknown joinable type {type(joinable)}")

            if exceptions_tids:
                logger.debug("Task {} failed due to failure of an inner join future".format(outer_task_id))
                e = JoinError(exceptions_tids, outer_task_id)
                # We keep the history separately, since the future itself could be
                # tossed.
                task_record['fail_history'].append(repr(e))
                task_record['fail_count'] += 1
                # no need to update the fail cost because join apps are never
                # retried

                self.update_task_state(task_record, States.failed)
                task_record['time_returned'] = datetime.datetime.now()
                with task_record['app_fu']._update_lock:
                    task_record['app_fu'].set_exception(e)

            else:
                # all the joinables succeeded, so construct a result:
                if isinstance(joinable, Future):
                    assert inner_app_future is joinable
                    res = joinable.result()
                elif isinstance(joinable, list):
                    res = []
                    for future in joinable:
                        res.append(future.result())
                else:
                    raise TypeError(f"Unknown joinable type {type(joinable)}")
                self._complete_task(task_record, States.exec_done, res)

            self._log_std_streams(task_record)

            self._send_task_log_info(task_record)

    def handle_app_update(self, task_record: TaskRecord, future: AppFuture) -> None:
        """This function is called as a callback when an AppFuture
        is in its final state.

        It will trigger post-app processing such as checkpointing.

        Args:
             task_record : Task record
             future (Future) : The relevant app future (which should be
                 consistent with the task structure 'app_fu' entry

        """

        task_id = task_record['id']

        if not task_record['app_fu'].done():
            logger.error("Internal consistency error: app_fu is not done for task {}".format(task_id))
        if not task_record['app_fu'] == future:
            logger.error("Internal consistency error: callback future is not the app_fu in task structure, for task {}".format(task_id))

        self.memoizer.update_memo(task_record, future)

        # Cover all checkpointing cases here:
        # Do we need to checkpoint now, or queue for later,
        # or do nothing?
        if self.checkpoint_mode == 'task_exit':
            self.checkpoint(tasks=[task_record])
        elif self.checkpoint_mode in ('manual', 'periodic', 'dfk_exit'):
            with self.checkpoint_lock:
                self.checkpointable_tasks.append(task_record)
        elif self.checkpoint_mode is None:
            pass
        else:
            raise InternalConsistencyError(f"Invalid checkpoint mode {self.checkpoint_mode}")

        self.wipe_task(task_id)
        return

    def _complete_task(self, task_record: TaskRecord, new_state: States, result: Any) -> None:
        """Set a task into a completed state
        """
        assert new_state in FINAL_STATES
        assert new_state not in FINAL_FAILURE_STATES
        old_state = task_record['status']

        self.update_task_state(task_record, new_state)

        logger.info(f"Task {task_record['id']} completed ({old_state.name} -> {new_state.name})")
        task_record['time_returned'] = datetime.datetime.now()

        with task_record['app_fu']._update_lock:
            task_record['app_fu'].set_result(result)

    def update_task_state(self, task_record: TaskRecord, new_state: States) -> None:
        """Updates a task record state, and recording an appropriate change
        to task state counters.
        """

        with self.task_state_counts_lock:
            if 'status' in task_record:
                self.task_state_counts[task_record['status']] -= 1
            self.task_state_counts[new_state] += 1
            task_record['status'] = new_state

    @staticmethod
    def _unwrap_remote_exception_wrapper(future: Future) -> Any:
        result = future.result()
        if isinstance(result, RemoteExceptionWrapper):
            result.reraise()
        return result

    def wipe_task(self, task_id: int) -> None:
        """Remove task with task_id from the internal tasks table
        """
        if self.config.garbage_collect:
            del self.tasks[task_id]

    @staticmethod
    def check_staging_inhibited(kwargs: Dict[str, Any]) -> bool:
        return kwargs.get('_parsl_staging_inhibit', False)

    def launch_if_ready(self, task_record: TaskRecord) -> None:
        """Schedules a task record for re-inspection to see if it is ready
        for launch and for launch if it is ready. The call will return
        immediately.

        This should be called by any piece of the DataFlowKernel that
        thinks a task may have become ready to run.

        It is not an error to call launch_if_ready on a task that is not
        ready to run - launch_if_ready will not incorrectly launch that
        task.

        launch_if_ready is thread safe, so may be called from any thread
        or callback.
        """
        self.dependency_launch_pool.submit(self._launch_if_ready_async, task_record)

    @wrap_with_logs
    def _launch_if_ready_async(self, task_record: TaskRecord) -> None:
        """
        _launch_if_ready will launch the specified task, if it is ready
        to run (for example, without dependencies, and in pending state).
        """
        exec_fu = None

        task_id = task_record['id']
        with task_record['task_launch_lock']:

            if task_record['status'] != States.pending:
                logger.debug(f"Task {task_id} is not pending, so launch_if_ready skipping")
                return

            if self._count_deps(task_record['depends']) != 0:
                logger.debug(f"Task {task_id} has outstanding dependencies, so launch_if_ready skipping")
                return

            # We can now launch the task or handle any dependency failures

            new_args, kwargs, exceptions_tids = self._unwrap_futures(task_record['args'],
                                                                     task_record['kwargs'])
            task_record['args'] = new_args
            task_record['kwargs'] = kwargs

            if not exceptions_tids:
                # There are no dependency errors
                try:
                    exec_fu = self.launch_task(task_record)
                    assert isinstance(exec_fu, Future)
                except Exception as e:
                    # task launched failed somehow. the execution might
                    # have been launched and an exception raised after
                    # that, though. that's hard to detect from here.
                    # we don't attempt retries here. This is an error with submission
                    # even though it might come from user code such as a plugged-in
                    # executor or memoization hash function.

                    logger.debug("Got an exception launching task", exc_info=True)
                    exec_fu = Future()
                    exec_fu.set_exception(e)
            else:
                logger.info(
                    "Task {} failed due to dependency failure".format(task_id))
                # Raise a dependency exception
                self.update_task_state(task_record, States.dep_fail)

                self._send_task_log_info(task_record)

                exec_fu = Future()
                exec_fu.set_exception(DependencyError(exceptions_tids,
                                                      task_id))

        if exec_fu:
            assert isinstance(exec_fu, Future)
            try:
                exec_fu.add_done_callback(partial(self.handle_exec_update, task_record))
            except Exception:
                # this exception is ignored here because it is assumed that exception
                # comes from directly executing handle_exec_update (because exec_fu is
                # done already). If the callback executes later, then any exception
                # coming out of the callback will be ignored and not propate anywhere,
                # so this block attempts to keep the same behaviour here.
                logger.error("add_done_callback got an exception which will be ignored", exc_info=True)

            task_record['exec_fu'] = exec_fu

    def launch_task(self, task_record: TaskRecord) -> Future:
        """Handle the actual submission of the task to the executor layer.

        Args:
            task_record : The task record

        Returns:
            Future that tracks the execution of the submitted function
        """
        task_id = task_record['id']
        function = task_record['func']
        args = task_record['args']
        kwargs = task_record['kwargs']

        task_record['try_time_launched'] = datetime.datetime.now()

        memo_fu = self.memoizer.check_memo(task_record)
        if memo_fu:
            logger.info("Reusing cached result for task {}".format(task_id))
            task_record['from_memo'] = True
            assert isinstance(memo_fu, Future)
            return memo_fu

        task_record['from_memo'] = False
        executor_label = task_record["executor"]
        try:
            executor = self.executors[executor_label]
        except Exception:
            logger.exception("Task {} requested invalid executor {}: config is\n{}".format(task_id, executor_label, self._config))
            raise ValueError("Task {} requested invalid executor {}".format(task_id, executor_label))

        try_id = task_record['fail_count']

        if self.monitoring is not None and self.monitoring.resource_monitoring_enabled:
            wrapper_logging_level = logging.DEBUG if self.monitoring.monitoring_debug else logging.INFO
            (function, args, kwargs) = monitor_wrapper(f=function,
                                                       args=args,
                                                       kwargs=kwargs,
                                                       x_try_id=try_id,
                                                       x_task_id=task_id,
                                                       monitoring_hub_url=self.monitoring.monitoring_hub_url,
                                                       run_id=self.run_id,
                                                       logging_level=wrapper_logging_level,
                                                       sleep_dur=self.monitoring.resource_monitoring_interval,
                                                       radio_mode=executor.radio_mode,
                                                       monitor_resources=executor.monitor_resources(),
                                                       run_dir=self.run_dir)

        with self.submitter_lock:
            exec_fu = executor.submit(function, task_record['resource_specification'], *args, **kwargs)
        self.update_task_state(task_record, States.launched)

        self._send_task_log_info(task_record)

        if hasattr(exec_fu, "parsl_executor_task_id"):
            logger.info(
                f"Parsl task {task_id} try {try_id} launched on executor {executor.label} "
                f"with executor id {exec_fu.parsl_executor_task_id}")

        else:
            logger.info(f"Parsl task {task_id} try {try_id} launched on executor {executor.label}")

        self._log_std_streams(task_record)

        return exec_fu

    def _add_input_deps(self, executor: str, args: Sequence[Any], kwargs: Dict[str, Any], func: Callable) -> Tuple[Sequence[Any], Dict[str, Any],
                                                                                                                   Callable]:
        """Look for inputs of the app that are files. Give the data manager
        the opportunity to replace a file with a data future for that file,
        for example wrapping the result of a staging action.

        Args:
            - executor (str) : executor where the app is going to be launched
            - args (List) : Positional args to app function
            - kwargs (Dict) : Kwargs to app function
        """

        # Return if the task is a data management task, rather than doing
        #  data management on it.
        if self.check_staging_inhibited(kwargs):
            logger.debug("Not performing input staging")
            return args, kwargs, func

        inputs = kwargs.get('inputs', [])
        for idx, f in enumerate(inputs):
            (inputs[idx], func) = self.data_manager.optionally_stage_in(f, func, executor)

        for kwarg, f in kwargs.items():
            # stdout and stderr files should not be staging in (they will be staged *out*
            # in _add_output_deps)
            if kwarg in ['stdout', 'stderr']:
                continue
            (kwargs[kwarg], func) = self.data_manager.optionally_stage_in(f, func, executor)

        newargs = list(args)
        for idx, f in enumerate(newargs):
            (newargs[idx], func) = self.data_manager.optionally_stage_in(f, func, executor)

        return tuple(newargs), kwargs, func

    def _add_output_deps(self, executor: str, args: Sequence[Any], kwargs: Dict[str, Any], app_fut: AppFuture, func: Callable) -> Callable:
        logger.debug("Adding output dependencies")
        outputs = kwargs.get('outputs', [])
        app_fut._outputs = []

        # Pass over all possible outputs: the outputs kwarg, stdout and stderr
        # and for each of those, perform possible stage-out. This can result in:
        # a DataFuture to be exposed in app_fut to represent the completion of
        # that stageout (sometimes backed by a new sub-workflow for separate-task
        # stageout), a replacement for the function to be executed (intended to
        # be the original function wrapped with an in-task stageout wrapper), a
        # rewritten File object to be passed to task to be executed

        def stageout_one_file(file: File, rewritable_func: Callable):
            if not self.check_staging_inhibited(kwargs):
                # replace a File with a DataFuture - either completing when the stageout
                # future completes, or if no stage out future is returned, then when the
                # app itself completes.

                # The staging code will get a clean copy which it is allowed to mutate,
                # while the DataFuture-contained original will not be modified by any staging.
                f_copy = file.cleancopy()

                logger.debug("Submitting stage out for output file {}".format(repr(file)))
                stageout_fut = self.data_manager.stage_out(f_copy, executor, app_fut)
                if stageout_fut:
                    logger.debug("Adding a dependency on stageout future for {}".format(repr(file)))
                    df = DataFuture(stageout_fut, file, tid=app_fut.tid)
                else:
                    logger.debug("No stageout dependency for {}".format(repr(file)))
                    df = DataFuture(app_fut, file, tid=app_fut.tid)

                # this is a hook for post-task stageout
                # note that nothing depends on the output - which is maybe a bug
                # in the not-very-tested stageout system?
                rewritable_func = self.data_manager.replace_task_stage_out(f_copy, rewritable_func, executor)
                return rewritable_func, f_copy, df
            else:
                logger.debug("Not performing output staging for: {}".format(repr(file)))
                return rewritable_func, file, DataFuture(app_fut, file, tid=app_fut.tid)

        for idx, file in enumerate(outputs):
            func, outputs[idx], o = stageout_one_file(file, func)
            app_fut._outputs.append(o)

        file = kwargs.get('stdout')
        if isinstance(file, File):
            func, kwargs['stdout'], app_fut._stdout_future = stageout_one_file(file, func)

        file = kwargs.get('stderr')
        if isinstance(file, File):
            func, kwargs['stderr'], app_fut._stderr_future = stageout_one_file(file, func)

        return func

    def _gather_all_deps(self, args: Sequence[Any], kwargs: Dict[str, Any]) -> List[Future]:
        """Assemble a list of all Futures passed as arguments, kwargs or in the inputs kwarg.

        Args:
            - args: The list of args pass to the app
            - kwargs: The dict of all kwargs passed to the app

        Returns:
            - list of dependencies

        """
        depends: List[Future] = []

        def check_dep(d: Any) -> None:
            try:
                depends.extend(self.dependency_resolver.traverse_to_gather(d))
            except Exception:
                logger.exception("Exception in dependency_resolver.traverse_to_gather")
                raise

        # Check the positional args
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

    def _unwrap_futures(self, args: Sequence[Any], kwargs: Dict[str, Any]) \
            -> Tuple[Sequence[Any], Dict[str, Any], Sequence[Tuple[Exception, str]]]:
        """This function should be called when all dependencies have completed.

        It will rewrite the arguments for that task, replacing each Future
        with the result of that future.

        If the user hid futures a level below, we will not catch
        it, and will (most likely) result in a type error.

        Args:
             args (List) : Positional args to app function
             kwargs (Dict) : Kwargs to app function

        Return:
            a rewritten args list
            a rewritten kwargs dict
            pairs of exceptions, task ids from any Futures which stored
            exceptions rather than results.
        """
        dep_failures = []

        def append_failure(e: Exception, dep: Future) -> None:
            tid = self.render_future_description(dep)
            dep_failures.extend([(e, tid)])

        # Replace item in args
        new_args = []
        for dep in args:
            try:
                new_args.extend([self.dependency_resolver.traverse_to_unwrap(dep)])
            except Exception as e:
                append_failure(e, dep)

        # Check for explicit kwargs ex, fu_1=<fut>
        for key in kwargs:
            dep = kwargs[key]
            try:
                kwargs[key] = self.dependency_resolver.traverse_to_unwrap(dep)
            except Exception as e:
                append_failure(e, dep)

        # Check for futures in inputs=[<fut>...]
        if 'inputs' in kwargs:
            new_inputs = []
            for dep in kwargs['inputs']:
                try:
                    new_inputs.extend([self.dependency_resolver.traverse_to_unwrap(dep)])
                except Exception as e:
                    append_failure(e, dep)
            kwargs['inputs'] = new_inputs

        return new_args, kwargs, dep_failures

    def submit(self,
               func: Callable,
               app_args: Sequence[Any],
               executors: Union[str, Sequence[str]],
               cache: bool,
               ignore_for_cache: Optional[Sequence[str]],
               app_kwargs: Dict[str, Any],
               join: bool = False) -> AppFuture:
        """Add task to the dataflow system.

        If the app task has the executors attributes not set (default=='all')
        the task will be launched on a randomly selected executor from the
        list of executors. If the app task specifies a particular set of
        executors, it will be targeted at the specified executors.

        Args:
            - func : A function object

        KWargs :
            - app_args : Args to the function
            - executors (list or string) : List of executors this call could go to.
                    Default='all'
            - cache (Bool) : To enable memoization or not
            - ignore_for_cache (sequence) : List of kwargs to be ignored for memoization/checkpointing
            - app_kwargs (dict) : Rest of the kwargs to the fn passed as dict.

        Returns:
            AppFuture

        """

        if ignore_for_cache is None:
            ignore_for_cache = []
        else:
            # duplicate so that it can be modified safely later
            ignore_for_cache = list(ignore_for_cache)

        if self.cleanup_called:
            raise NoDataFlowKernelError("Cannot submit to a DFK that has been cleaned up")

        task_id = self.task_count
        self.task_count += 1
        if isinstance(executors, str) and executors.lower() == 'all':
            choices = list(e for e in self.executors if e != '_parsl_internal')
        elif isinstance(executors, list):
            choices = executors
        else:
            raise ValueError("Task {} supplied invalid type for executors: {}".format(task_id, type(executors)))
        executor = random.choice(choices)
        logger.debug("Task {} will be sent to executor {}".format(task_id, executor))

        resource_specification = app_kwargs.get('parsl_resource_specification', {})

        task_record: TaskRecord
        task_record = {'args': app_args,
                       'depends': [],
                       'dfk': self,
                       'executor': executor,
                       'func': func,
                       'func_name': func.__name__,
                       'kwargs': app_kwargs,
                       'memoize': cache,
                       'hashsum': None,
                       'exec_fu': None,
                       'fail_count': 0,
                       'fail_cost': 0,
                       'fail_history': [],
                       'from_memo': None,
                       'ignore_for_cache': ignore_for_cache,
                       'join': join,
                       'joins': None,
                       'try_id': 0,
                       'id': task_id,
                       'task_launch_lock': threading.Lock(),
                       'time_invoked': datetime.datetime.now(),
                       'time_returned': None,
                       'try_time_launched': None,
                       'try_time_returned': None,
                       'resource_specification': resource_specification}

        self.update_task_state(task_record, States.unsched)

        for kw in ['stdout', 'stderr']:
            if kw in app_kwargs:
                if app_kwargs[kw] == parsl.AUTO_LOGNAME:
                    if kw not in ignore_for_cache:
                        ignore_for_cache += [kw]
                    if self.config.std_autopath is None:
                        app_kwargs[kw] = self.default_std_autopath(task_record, kw)
                    else:
                        app_kwargs[kw] = self.config.std_autopath(task_record, kw)

        app_fu = AppFuture(task_record)
        task_record['app_fu'] = app_fu

        # Transform remote input files to data futures
        app_args, app_kwargs, func = self._add_input_deps(executor, app_args, app_kwargs, func)

        func = self._add_output_deps(executor, app_args, app_kwargs, app_fu, func)

        logger.debug("Added output dependencies")

        # Replace the function invocation in the TaskRecord with whatever file-staging
        # substitutions have been made.
        task_record.update({
                    'args': app_args,
                    'func': func,
                    'kwargs': app_kwargs})

        assert task_id not in self.tasks

        self.tasks[task_id] = task_record

        logger.debug("Gathering dependencies")
        # Get the list of dependencies for the task
        depends = self._gather_all_deps(app_args, app_kwargs)
        logger.debug("Gathered dependencies")
        task_record['depends'] = depends

        depend_descs = []
        for d in depends:
            depend_descs.append(self.render_future_description(d))

        if depend_descs != []:
            waiting_message = "waiting on {}".format(", ".join(depend_descs))
        else:
            waiting_message = "not waiting on any dependency"

        logger.info("Task {} submitted for App {}, {}".format(task_id,
                                                              task_record['func_name'],
                                                              waiting_message))

        app_fu.add_done_callback(partial(self.handle_app_update, task_record))
        self.update_task_state(task_record, States.pending)
        logger.debug("Task {} set to pending state with AppFuture: {}".format(task_id, task_record['app_fu']))

        self._send_task_log_info(task_record)

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

            def callback_adapter(dep_fut: Future) -> None:
                self.launch_if_ready(task_record)

            try:
                d.add_done_callback(callback_adapter)
            except Exception as e:
                logger.error("add_done_callback got an exception {} which will be ignored".format(e))

        self.launch_if_ready(task_record)

        return app_fu

    # it might also be interesting to assert that all DFK
    # tasks are in a "final" state (3,4,5) when the DFK
    # is closed down, and report some kind of warning.
    # although really I'd like this to drain properly...
    # and a drain function might look like this.
    # If tasks have their states changed, this won't work properly
    # but we can validate that...
    def log_task_states(self) -> None:
        logger.info("Summary of tasks in DFK:")

        with self.task_state_counts_lock:
            for state in States:
                logger.info("Tasks in state {}: {}".format(str(state), self.task_state_counts[state]))

        logger.info("End of summary")

    def add_executors(self, executors: Sequence[ParslExecutor]) -> None:
        for executor in executors:
            executor.run_id = self.run_id
            executor.run_dir = self.run_dir
            if self.monitoring:
                executor.hub_address = self.monitoring.hub_address
                executor.hub_zmq_port = self.monitoring.hub_zmq_port
                executor.submit_monitoring_radio = self.monitoring.radio
            if hasattr(executor, 'provider'):
                if hasattr(executor.provider, 'script_dir'):
                    executor.provider.script_dir = os.path.join(self.run_dir, 'submit_scripts')
                    os.makedirs(executor.provider.script_dir, exist_ok=True)

            self.executors[executor.label] = executor
            executor.start()
        block_executors = [e for e in executors if isinstance(e, BlockProviderExecutor)]
        self.job_status_poller.add_executors(block_executors)

    def atexit_cleanup(self) -> None:
        logger.warning("Python is exiting with a DFK still running. "
                       "You should call parsl.dfk().cleanup() before "
                       "exiting to release any resources")

    def wait_for_current_tasks(self) -> None:
        """Waits for all tasks in the task list to be completed, by waiting for their
        AppFuture to be completed. This method will not necessarily wait for any tasks
        added after cleanup has started (such as data stageout?)
        """

        logger.info("Waiting for all remaining tasks to complete")

        # .values is made into a list immediately to reduce (although not
        # eliminate) a race condition where self.tasks can be modified
        # elsewhere by a completing task being removed from the dictionary.
        task_records = list(self.tasks.values())
        for task_record in task_records:
            # .exception() is a less exception throwing way of
            # waiting for completion than .result()
            fut = task_record['app_fu']
            if not fut.done():
                fut.exception()
            # now app future is done, poll until DFK state is final: a
            # DFK state being final and the app future being done do not imply each other.
            while task_record['status'] not in FINAL_STATES:
                time.sleep(0.1)

        logger.info("All remaining tasks completed")

    @wrap_with_logs
    def cleanup(self) -> None:
        """DataFlowKernel cleanup.

        This involves releasing all resources explicitly.

        We call scale_in on each of the executors and call executor.shutdown.
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
        self.usage_tracker.send_end_message()
        self.usage_tracker.close()

        logger.info("Closing job status poller")
        self.job_status_poller.close()
        logger.info("Terminated job status poller")

        logger.info("Shutting down executors")

        for executor in self.executors.values():
            logger.info(f"Shutting down executor {executor.label}")
            executor.shutdown()
            logger.info(f"Shut down executor {executor.label}")

        logger.info("Terminated executors")
        self.time_completed = datetime.datetime.now()

        if self.monitoring:
            logger.info("Sending final monitoring message")
            self.monitoring.send((MessageType.WORKFLOW_INFO,
                                 {'tasks_failed_count': self.task_state_counts[States.failed],
                                  'tasks_completed_count': self.task_state_counts[States.exec_done],
                                  "time_began": self.time_began,
                                  'time_completed': self.time_completed,
                                  'run_id': self.run_id, 'rundir': self.run_dir}))

            logger.info("Terminating monitoring")
            self.monitoring.close()
            logger.info("Terminated monitoring")

        logger.info("Terminating dependency launch pool")
        self.dependency_launch_pool.shutdown()
        logger.info("Terminated dependency launch pool")

        logger.info("Unregistering atexit hook")
        atexit.unregister(self.atexit_cleanup)
        logger.info("Unregistered atexit hook")

        if DataFlowKernelLoader._dfk is self:
            logger.info("Unregistering default DFK")
            parsl.clear()
            logger.info("Unregistered default DFK")
        else:
            logger.debug("Cleaning up non-default DFK - not unregistering")

        logger.info("DFK cleanup complete")

    def checkpoint(self, tasks: Optional[Sequence[TaskRecord]] = None) -> str:
        """Checkpoint the dfk incrementally to a checkpoint file.

        When called, every task that has been completed yet not
        checkpointed is checkpointed to a file.

        Kwargs:
            - tasks (List of task records) : List of task ids to checkpoint. Default=None
                                         if set to None, we iterate over all tasks held by the DFK.

        .. note::
            Checkpointing only works if memoization is enabled

        Returns:
            Checkpoint dir if checkpoints were written successfully.
            By default the checkpoints are written to the RUNDIR of the current
            run under RUNDIR/checkpoints/tasks.pkl
        """
        with self.checkpoint_lock:
            if tasks:
                checkpoint_queue = tasks
            else:
                checkpoint_queue = self.checkpointable_tasks
                self.checkpointable_tasks = []

            checkpoint_dir = '{0}/checkpoint'.format(self.run_dir)
            checkpoint_tasks = checkpoint_dir + '/tasks.pkl'

            if not os.path.exists(checkpoint_dir):
                os.makedirs(checkpoint_dir, exist_ok=True)

            count = 0

            with open(checkpoint_tasks, 'ab') as f:
                for task_record in checkpoint_queue:
                    task_id = task_record['id']

                    app_fu = task_record['app_fu']

                    if app_fu.done() and app_fu.exception() is None:
                        hashsum = task_record['hashsum']
                        if not hashsum:
                            continue
                        t = {'hash': hashsum, 'exception': None, 'result': app_fu.result()}

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

    @staticmethod
    def _log_std_streams(task_record: TaskRecord) -> None:
        tid = task_record['id']

        def log_std_stream(name: str, target) -> None:
            if target is None:
                logger.info(f"{name} for task {tid} will not be redirected.")
            elif isinstance(target, str):
                logger.info(f"{name} for task {tid} will be redirected to {target}")
            elif isinstance(target, os.PathLike):
                logger.info(f"{name} for task {tid} will be redirected to {os.fspath(target)}")
            elif isinstance(target, tuple) and len(target) == 2 and isinstance(target[0], str):
                logger.info(f"{name} for task {tid} will be redirected to {target[0]} with mode {target[1]}")
            elif isinstance(target, tuple) and len(target) == 2 and isinstance(target[0], os.PathLike):
                logger.info(f"{name} for task {tid} will be redirected to {os.fspath(target[0])} with mode {target[1]}")
            elif isinstance(target, DataFuture):
                logger.info(f"{name} for task {tid} will staged to {target.file_obj.url}")
            else:
                logger.error(f"{name} for task {tid} has unknown specification: {target!r}")

        log_std_stream("Standard out", task_record['app_fu'].stdout)
        log_std_stream("Standard error", task_record['app_fu'].stderr)

    def default_std_autopath(self, taskrecord, kw):
        label = taskrecord['kwargs'].get('label')
        task_id = taskrecord['id']
        return os.path.join(
            self.run_dir,
            'task_logs',
            str(int(task_id / 10000)).zfill(4),  # limit logs to 10k entries per directory
            'task_{}_{}{}.{}'.format(
                str(task_id).zfill(4),
                taskrecord['func_name'],
                '' if label is None else '_{}'.format(label),
                kw))

    def render_future_description(self, dep: Future) -> str:
        """Renders a description of the future in the context of the
        current DFK.
        """
        if isinstance(dep, AppFuture) and dep.task_record['dfk'] == self:
            tid = "task " + repr(dep.task_record['id'])
        elif isinstance(dep, DataFuture):
            tid = "DataFuture from task " + repr(dep.tid)
        else:
            tid = repr(dep)
        return tid


class DataFlowKernelLoader:
    """Manage which DataFlowKernel is active.

    This is a singleton class containing only class methods. You should not
    need to instantiate this class.
    """

    _dfk: Optional[DataFlowKernel] = None

    @classmethod
    def clear(cls) -> None:
        """Clear the active DataFlowKernel so that a new one can be loaded."""
        cls._dfk = None

    @classmethod
    @typeguard.typechecked
    def load(cls, config: Optional[Config] = None) -> DataFlowKernel:
        """Load a DataFlowKernel.

        Args:
            - config (Config) : Configuration to load. This config will be passed to a
              new DataFlowKernel instantiation which will be set as the active DataFlowKernel.
        Returns:
            - DataFlowKernel : The loaded DataFlowKernel object.
        """
        if cls._dfk is not None:
            raise ConfigurationError('Config has already been loaded')

        if config is None:
            cls._dfk = DataFlowKernel(Config())
        else:
            cls._dfk = DataFlowKernel(config)

        return cls._dfk

    @classmethod
    def wait_for_current_tasks(cls) -> None:
        """Waits for all tasks in the task list to be completed, by waiting for their
        AppFuture to be completed. This method will not necessarily wait for any tasks
        added after cleanup has started such as data stageout.
        """
        cls.dfk().wait_for_current_tasks()

    @classmethod
    def dfk(cls) -> DataFlowKernel:
        """Return the currently-loaded DataFlowKernel."""
        if cls._dfk is None:
            raise NoDataFlowKernelError('Must first load config')
        return cls._dfk
