import os
import logging
import atexit
import random
import pickle
import threading

from concurrent.futures import Future
from functools import partial

import parsl
import libsubmit
from parsl.dataflow.error import *
from parsl.dataflow.states import States
from parsl.dataflow.futures import AppFuture
from parsl.dataflow.rundirs import make_rundir
from parsl.dataflow.flow_control import FlowControl, FlowNoControl, Timer
from parsl.dataflow.usage_tracking.usage import UsageTracker
from parsl.dataflow.memoization import Memoizer
from parsl.dataflow.config_defaults import update_config
from parsl.data_provider.data_manager import DataManager
from parsl.execution_provider.provider_factory import ExecProviderFactory as EPF
from parsl.utils import get_version
from parsl.app.errors import RemoteException

# from parsl.dataflow.start_controller import Controller
# Exceptions

logger = logging.getLogger(__name__)


class DataFlowKernel(object):
    """The DataFlowKernel adds dependency awareness to an existing executor.

    It is responsible for managing futures, such that when dependencies are resolved,
    pending tasks move to the runnable state.

    Here's a simplified diagram of what happens internally::

         User             |        DFK         |    Executor
        ----------------------------------------------------------
                          |                    |
               Task-------+> +Submit           |
             App_Fu<------+--|                 |
                          |  Dependencies met  |
                          |         task-------+--> +Submit
                          |        Ex_Fu<------+----|

    """

    def __init__(self, config=None, executors=None, lazyErrors=True, appCache=True,
                 rundir=None, retries=0, checkpointFiles=None, checkpointMode=None,
                 data_manager=None):
        """ Initialize the DataFlowKernel.

        Please note that keyword args passed to the DFK here will always override
        options passed in via the config.

        KWargs:
            - config (dict): A single data object encapsulating all config attributes
            - executors (list of Executor objs): Optional, kept for (somewhat) backward compatibility with 0.2.0
            - lazyErrors(bool): Default=True, allow workflow to continue on app failures.
            - appCache (bool): Enable caching of apps
            - rundir (str): Path to run directory. Defaults to ./runinfo/runNNN
            - retries(int): Default=0, Set the number of retry attempts in case of failure
            - checkpointFiles (list of str): List of filepaths to checkpoint files
            - checkpointMode (None, 'dfk_exit', 'task_exit', 'periodic'): Method to use.
            - data_manager (DataManager): User created DataManager
        Returns:
            DataFlowKernel object
        """
        # Create run dirs for this run
        self.rundir = make_rundir(config=config, path=rundir)
        parsl.set_file_logger("{}/parsl.log".format(self.rundir),
                              level=logging.DEBUG)

        logger.info("Parsl version: {}".format(get_version()))
        logger.info("Libsubmit version: {}".format(libsubmit.__version__))

        # Update config with defaults
        self._config = update_config(config, self.rundir)

        # Set the data manager
        if data_manager:
            self.data_manager = data_manager
        else:
            self.data_manager = DataManager(config=self._config)

        # Start the anonymized usage tracker and send init msg
        self.usage_tracker = UsageTracker(self)
        self.usage_tracker.send_message()

        # Load Memoizer with checkpoints before we start the run.
        if checkpointFiles:
            checkpoint_src = checkpointFiles
        elif self._config and self._config["globals"]["checkpointFiles"]:
            checkpoint_src = self._config["globals"]["checkpointFiles"]
        else:
            checkpoint_src = None

        cpts = self.load_checkpoints(checkpoint_src)
        # Initialize the memoizer
        self.memoizer = Memoizer(self, memoize=appCache, checkpoint=cpts)
        self.checkpointed_tasks = 0
        self._checkpoint_timer = None

        if self._config:
            self._executors_managed = True
            # Create the executors
            epf = EPF()
            self.executors = epf.make(self.rundir, self._config)

            # set global vars from config
            self.lazy_fail = self._config["globals"].get("lazyErrors", lazyErrors)
            self.fail_retries = self._config["globals"].get("retries", retries)
            self.flowcontrol = FlowControl(self, self._config)
            self.checkpoint_mode = self._config["globals"].get("checkpointMode",
                                                               checkpointMode)
            if self.checkpoint_mode == "periodic":
                period = self._config["globals"].get("checkpointPeriod",
                                                     "00:30:00")
                try:
                    h, m, s = map(int, period.split(':'))
                    checkpoint_period = (h * 3600) + (m * 60) + s
                    self._checkpoint_timer = Timer(self.checkpoint, interval=checkpoint_period)
                except Exception as e:
                    logger.error("invalid checkpointPeriod provided:{0} expected HH:MM:SS".format(period))
                    self._checkpoint_timer = Timer(self.checkpoint, interval=(30 * 60))

        else:
            self._executors_managed = False
            self.fail_retries = retries
            self.lazy_fail = lazyErrors
            self.executors = {i: x for i, x in enumerate(executors)}
            self.flowcontrol = FlowNoControl(self, None)
            self.checkpoint_mode = checkpointMode

        self.task_count = 0
        self.fut_task_lookup = {}
        self.tasks = {}
        self.task_launch_lock = threading.Lock()

        logger.debug("Using executors: {0}".format(self.executors))
        atexit.register(self.cleanup)

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

            if not self.lazy_fail:
                logger.debug("Eager fail, skipping retry logic")
                raise e

            if self.tasks[task_id]['fail_count'] <= self.fail_retries:
                logger.debug("Task {} marked for retry".format(task_id))
                self.tasks[task_id]['status'] = States.pending

            else:
                logger.info("Task {} failed after {} retry attempts".format(task_id,
                                                                            self.fail_retries))
                self.tasks[task_id]['status'] = States.failed
                final_state_flag = True

        else:
            logger.info("Task {} completed".format(task_id))
            self.tasks[task_id]['status'] = States.done
            final_state_flag = True

        if not memo_cbk and final_state_flag is True:
            # Update the memoizer with the new result if this is not a
            # result from a memo lookup and the task has reached a terminal state.
            self.memoizer.update_memo(task_id, self.tasks[task_id], future)

            if self.checkpoint_mode is 'task_exit':
                self.checkpoint(tasks=[task_id])

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

        If the app task has the sites attributes not set (default=='all')
        the task is launched on a randomly selected executor from the
        list of executors. This behavior could later be updates to support
        binding to sites based on user specified criteria.

        If the app task specifies a particular set of sites, it will be
        targetted at those specific sites.

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

        target_sites = self.tasks[task_id]["sites"]
        executor = None
        if isinstance(target_sites, str) and target_sites.lower() == 'all':
            # Pick a random site from the list
            site, executor = random.choice(list(self.executors.items()))

        elif isinstance(target_sites, list):
            # Pick a random site from user specified list
            try:
                site = random.choice(target_sites)
                executor = self.executors[site]

            except Exception as e:
                logger.error("Task {}: requests invalid site [{}]".format(task_id,
                                                                          target_sites))
        else:
            logger.error("App {} specifies invalid site option, expects str|list".format(
                self.tasks[task_id]['func'].__name__))

        exec_fu = executor.submit(executable, *args, **kwargs)
        self.tasks[task_id]['status'] = States.running
        exec_fu.retries_left = self.fail_retries - \
            self.tasks[task_id]['fail_count']
        exec_fu.add_done_callback(partial(self.handle_update, task_id))
        logger.info("Task {} launched on site {}".format(task_id, site))
        return exec_fu

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

    def submit(self, func, *args, parsl_sites='all', fn_hash=None, cache=False, **kwargs):
        """Add task to the dataflow system.

        >>> IF all deps are met:
        >>>   send to the runnable queue and launch the task
        >>> ELSE:
        >>>   post the task in the pending queue

        Args:
            - func : A function object
            - *args : Args to the function

        KWargs :
            - parsl_sites (List|String) : List of sites this call could go to.
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

        # Get the dep count and a list of dependencies for the task
        dep_cnt, depends = self._count_all_deps(task_id, args, kwargs)

        task_def = {'depends': depends,
                    'sites': parsl_sites,
                    'func': func,
                    'func_name': func.__name__,
                    'args': args,
                    'kwargs': kwargs,
                    'fn_hash': fn_hash,
                    'memoize': cache,
                    'callback': None,
                    'dep_cnt': dep_cnt,
                    'exec_fu': None,
                    'checkpoint': None,
                    'fail_count': 0,
                    'fail_history': [],
                    'env': None,
                    'status': States.unsched,
                    'app_fu': None}

        if task_id in self.tasks:
            raise DuplicateTaskError(
                "Task {0} in pending list".format(task_id))
        else:
            self.tasks[task_id] = task_def

        # Extract stdout and stderr to pass to AppFuture:
        task_stdout = kwargs.get('stdout', None)
        task_stderr = kwargs.get('stderr', None)

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
                logger.debug("Task {} launched with AppFut:{}".format(task_id,
                                                                      task_def['app_fu']))

            else:
                self.tasks[task_id]['exec_fu'] = None
                app_fu = AppFuture(self.tasks[task_id]['exec_fu'],
                                   tid=task_id,
                                   stdout=task_stdout,
                                   stderr=task_stderr)
                app_fu.set_exception(DependencyError(exceptions,
                                                     "Failures in input dependencies",
                                                     None))
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
            logger.debug("Task {} launched with AppFut:{}".format(task_id,
                                                                  task_def['app_fu']))

        return task_def['app_fu']

    def cleanup(self):
        """DataFlowKernel cleanup.

        This involves killing resources explicitly and sending die messages to IPP workers.

        If the executors are managed, i.e created by the DFK
            then : we scale_in each of the executors and call executor.shutdown
            else : we do nothing. Executor cleanup is left to the user.

        """
        logger.info("DFK cleanup initiated")

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
        # We do not need to cleanup if the executors are managed outside
        # the DFK
        if not self._executors_managed:
            return

        logger.info("Terminating flow_control and strategy threads")
        self.flowcontrol.close()

        for executor in self.executors.values():
            if executor.scaling_enabled:
                job_ids = executor.execution_provider.resources.keys()
                executor.scale_in(len(job_ids))

            # We are not doing shutdown here because even with block=False this blocks.
            executor.shutdown()

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

        checkpoint_queue = None
        if tasks:
            checkpoint_queue = tasks
        else:
            checkpoint_queue = self.tasks

        checkpoint_dir = '{0}/checkpoint'.format(self.rundir)
        checkpoint_dfk = checkpoint_dir + '/dfk.pkl'
        checkpoint_tasks = checkpoint_dir + '/tasks.pkl'

        if not os.path.exists(checkpoint_dir):
            try:
                os.makedirs(checkpoint_dir)
            except FileExistsError as e:
                pass

        with open(checkpoint_dfk, 'wb') as f:
            state = {'config': self.config,
                     'rundir': self.rundir,
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
                logger.warn("No tasks checkpointed, please ensure caching is enabled")
            else:
                logger.debug("No tasks checkpointed")
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
    def load(cls, config):
        """Load a DataFlowKernel.

        Args:
            - config (dict) : Configuration to load. This config will be passed to a
              new DataFlowKernel instantiation which will be set as the active DataFlowKernel.
        Returns:
            - DataFlowKernel : The loaded DataFlowKernel object.
        """
        if cls._dfk is not None:
            raise RuntimeError('Config has already been loaded')
        cls._dfk = DataFlowKernel(config=config)

        return cls._dfk

    @classmethod
    def dfk(cls):
        """Return the currently-loaded DataFlowKernel."""
        if cls._dfk is None:
            raise RuntimeError('Must first load config')
        return cls._dfk
