from __future__ import annotations

import hashlib
import logging
import os
import pickle
import threading
import types
from abc import ABCMeta, abstractmethod
from concurrent.futures import Future
from functools import lru_cache, singledispatch
from typing import Any, Dict, List, Literal, Optional, Sequence

import typeguard

from parsl.dataflow.errors import BadCheckpoint
from parsl.dataflow.taskrecord import TaskRecord
from parsl.errors import ConfigurationError
from parsl.utils import Timer, get_all_checkpoints

logger = logging.getLogger(__name__)


class CheckpointCommand:
    def __init__(self, task_record: TaskRecord, *, result: Optional[object] = None, exception: Optional[BaseException] = None):
        """Create a checkpoint command. This specifies a checkpoint entry of
        either a result or an exception. If exception is set, then this object
        specifies an exception checkpoint. Otherwise, it specifies a result.
        This is almost: one of exception or result must be non-None, but not
        quite because this data model also needs to accomodate a valid result
        of None."""

        assert result is None or exception is None, "CheckpointCommand cannot specify both a result and exception"

        self._task_record = task_record
        self._result = result
        self._exception = exception

    @property
    def task_record(self) -> TaskRecord:
        return self._task_record

    @property
    def result(self) -> Optional[object]:
        return self._result

    @property
    def exception(self) -> Optional[BaseException]:
        return self._exception


@singledispatch
def id_for_memo(obj: object, output_ref: bool = False) -> bytes:
    """This should return a byte sequence which identifies the supplied
    value for memoization purposes: for any two calls of id_for_memo,
    the byte sequence should be the same when the "same" value is supplied,
    and different otherwise.

    "same" is in quotes about because sameness is not as straightforward as
    serialising out the content.

    For example, for two dicts x, y:

      x = {"a":3, "b":4}
      y = {"b":4, "a":3}

    then: x == y, but their serialization is not equal, and some other
    functions on x and y are not equal: list(x.keys()) != list(y.keys())


    id_for_memo is invoked with output_ref=True when the parameter is an
    output reference (a value in the outputs=[] parameter of an app
    invocation).

    Memo hashing might be different for such parameters: for example, a
    user might choose to hash input File content so that changing the
    content of an input file invalidates memoization. This does not make
    sense to do for output files: there is no meaningful content stored
    where an output filename points at memoization time.
    """
    logger.error("id_for_memo attempted on unknown type {}".format(type(obj)))
    raise ValueError("unknown type for memoization: {}".format(type(obj)))


@id_for_memo.register(str)
@id_for_memo.register(int)
@id_for_memo.register(float)
@id_for_memo.register(type(None))
def id_for_memo_pickle(obj: object, output_ref: bool = False) -> bytes:
    return pickle.dumps(obj)


@id_for_memo.register(list)
def id_for_memo_list(denormalized_list: list, output_ref: bool = False) -> bytes:
    if type(denormalized_list) is not list:
        raise ValueError("id_for_memo_list cannot work on subclasses of list")

    normalized_list = []

    for e in denormalized_list:
        normalized_list.append(id_for_memo(e, output_ref=output_ref))

    return pickle.dumps(normalized_list)


@id_for_memo.register(tuple)
def id_for_memo_tuple(denormalized_tuple: tuple, output_ref: bool = False) -> bytes:
    if type(denormalized_tuple) is not tuple:
        raise ValueError("id_for_memo_tuple cannot work on subclasses of tuple")

    normalized_list = []

    for e in denormalized_tuple:
        normalized_list.append(id_for_memo(e, output_ref=output_ref))

    return pickle.dumps(normalized_list)


@id_for_memo.register(dict)
def id_for_memo_dict(denormalized_dict: dict, output_ref: bool = False) -> bytes:
    """This normalises the keys and values of the supplied dictionary.

    When output_ref=True, the values are normalised as output refs, but
    the keys are not.
    """
    if type(denormalized_dict) is not dict:
        raise ValueError("id_for_memo_dict cannot work on subclasses of dict")

    keys = sorted(denormalized_dict)

    normalized_list = []
    for k in keys:
        normalized_list.append(id_for_memo(k))
        normalized_list.append(id_for_memo(denormalized_dict[k], output_ref=output_ref))
    return pickle.dumps(normalized_list)


# the LRU cache decorator must be applied closer to the id_for_memo_function call
# that the .register() call, so that the cache-decorated version is registered.
@id_for_memo.register(types.FunctionType)
@lru_cache()
def id_for_memo_function(f: types.FunctionType, output_ref: bool = False) -> bytes:
    """This will checkpoint a function based only on its name and module name.
    This means that changing source code (other than the function name) will
    not cause a checkpoint invalidation.
    """
    return pickle.dumps(["types.FunctionType", f.__name__, f.__module__])


def make_hash(task: TaskRecord) -> str:
    """Create a hash of the task inputs.

    Args:
        - task (dict) : Task dictionary from dfk.tasks

    Returns:
        - hash (str) : A unique hash string
    """

    t: List[bytes] = []

    # if kwargs contains an outputs parameter, that parameter is removed
    # and normalised differently - with output_ref set to True.
    # kwargs listed in ignore_for_cache will also be removed

    filtered_kw = task['kwargs'].copy()

    ignore_list = task['ignore_for_cache']

    logger.debug("Ignoring these kwargs for checkpointing: %s", ignore_list)
    for k in ignore_list:
        logger.debug("Ignoring kwarg %s", k)
        del filtered_kw[k]

    if 'outputs' in task['kwargs']:
        outputs = task['kwargs']['outputs']
        del filtered_kw['outputs']
        t.append(id_for_memo(outputs, output_ref=True))

    t.extend(map(id_for_memo, (filtered_kw, task['func'], task['args'])))

    x = b''.join(t)
    return hashlib.md5(x).hexdigest()


class Memoizer(metaclass=ABCMeta):
    """Defines the interface for the DFK to talk to the memoization/checkpoint system.

    The DFK will invoke these methods on an instance of a Memoizer at suitable points
    in the lifecycle of a task. These methods are not intended for users to invoke
    directly.
    """

    @abstractmethod
    def update_memo_exception(self, task: TaskRecord, e: BaseException) -> None:
        """Called by the DFK when a task completes with an exception.

        On every task completion, either this method or `update_memo_result`
        will be called, but not both.

        This is an opportunity for the memoization/checkpoint system to record
        the outcome of a task for later discovery by a call to check_memo.
        """
        raise NotImplementedError

    @abstractmethod
    def update_memo_result(self, task: TaskRecord, r: Any) -> None:
        """Called by the DFK when a task completes with a successful result.

        On every task completion, either this method or `update_memo_exception`
        will be called, but not both.

        This is an opportunity for the memoization/checkpoint system to record
        the outcome of a task for later discovery by a call to check_memo.
        """
        raise NotImplementedError

    @abstractmethod
    def start(self, *, run_dir: str) -> None:
        """Called by the DFK when it starts up.

        This is an opportunity for the memoization/checkpoint system to
        initialize itself.

        The path to the base run directory is passed as a parameter.
        """
        raise NotImplementedError

    @abstractmethod
    def check_memo(self, task: TaskRecord) -> Optional[Future[Any]]:
        """Asks the checkpoint system for a result recorded for the described
        task. ``check_memo`` should return a `Future` that will be used as
        an executor future, in place of sending the task to an executor for
        execution. That future should be populated with a result or exception.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """Called at DFK shutdown. This gives the checkpoint system an
        opportunity for graceful shutdown.
        """
        raise NotImplementedError


class BasicMemoizer(Memoizer):
    """Memoizer is responsible for ensuring that identical work is not repeated.

    When a task is repeated, i.e., the same function is called with the same exact arguments, the
    result from a previous execution is reused. `wiki <https://en.wikipedia.org/wiki/Memoization>`_

    The memoizer implementation here does not collapse duplicate calls
    at call time, but works **only** when the result of a previous
    call is available at the time the duplicate call is made.

    For instance::

       No advantage from                 Memoization helps
       memoization here:                 here:

        TaskA                            TaskB
          |   TaskA                        |
          |     |   TaskA                done  (TaskB)
          |     |     |                                (TaskB)
        done    |     |
              done    |
                    done

    The memoizer creates a lookup table by hashing the function name
    and its inputs, and storing the results of the function.

    When a task is ready for launch, i.e., all of its arguments
    have resolved, we add its hash to the task datastructure.

    """

    run_dir: str

    def __init__(self, *,
                 checkpoint_files: Sequence[str] | None = None,
                 checkpoint_period: Optional[str] = None,
                 checkpoint_mode: Literal['task_exit', 'periodic', 'dfk_exit', 'manual'] | None = None,
                 memoize: bool = True):
        """Initialize the memoizer.

        KWargs:

            - checkpoint_files : sequence of str, optional
                  List of paths to checkpoint files. See :func:`parsl.utils.get_all_checkpoints` and
                  :func:`parsl.utils.get_last_checkpoint` for helpers. Default is None.
            - checkpoint_period : str, optional
                  Time interval (in "HH:MM:SS") at which to checkpoint completed tasks. Only has an effect if
                  ``checkpoint_mode='periodic'``.
            - checkpoint_mode : str, optional
                  Checkpoint mode to use, can be ``'dfk_exit'``, ``'task_exit'``, ``'periodic'`` or ``'manual'``.
                  If set to `None`, checkpointing will be disabled. Default is None.
            - memoize : str, enable memoization or not.

        """
        self.checkpointed_tasks = 0

        # this lock must be held when:
        # * writing to any checkpoint files
        # * interacting with self.checkpointable_tasks
        self._checkpoint_lock = threading.Lock()

        self.checkpoint_files = checkpoint_files
        self.checkpoint_mode = checkpoint_mode
        self.checkpoint_period = checkpoint_period

        self.checkpointable_tasks: List[CheckpointCommand] = []

        self._checkpoint_timer: Timer | None = None
        self.memoize = memoize

    def start(self, *, run_dir: str) -> None:

        self.run_dir = run_dir

        if self.checkpoint_files is not None:
            checkpoint_files = self.checkpoint_files
        elif self.checkpoint_files is None and self.checkpoint_mode is not None:
            checkpoint_files = get_all_checkpoints(self.run_dir)
        else:
            checkpoint_files = []

        checkpoint = self.load_checkpoints(checkpoint_files)

        if self.memoize:
            logger.info("App caching initialized")
            self.memo_lookup_table = checkpoint
        else:
            logger.info("App caching disabled for all apps")
            self.memo_lookup_table = {}

        if self.checkpoint_mode == "periodic":
            if self.checkpoint_period is None:
                raise ConfigurationError("Checkpoint period must be specified with periodic checkpoint mode")
            else:
                try:
                    h, m, s = map(int, self.checkpoint_period.split(':'))
                except Exception:
                    raise ConfigurationError("invalid checkpoint_period provided: {0} expected HH:MM:SS".format(self.checkpoint_period))
                checkpoint_period = (h * 3600) + (m * 60) + s
                self._checkpoint_timer = Timer(self.checkpoint_queue, interval=checkpoint_period, name="Checkpoint")

    def close(self) -> None:
        if self.checkpoint_mode is not None:
            logger.info("Making final checkpoint")
            self.checkpoint_queue()

        if self._checkpoint_timer:
            logger.info("Stopping checkpoint timer")
            self._checkpoint_timer.close()

    def check_memo(self, task: TaskRecord) -> Optional[Future[Any]]:
        """Create a hash of the task and its inputs and check the lookup table for this hash.

        If present, the results are returned.

        Args:
            - task(task) : task from the dfk.tasks table

        Returns:
            - Result of the function if present in table, wrapped in a Future

        This call will also set task['hashsum'] to the unique hashsum for the func+inputs.
        """

        task_id = task['id']

        if not self.memoize or not task['memoize']:
            task['hashsum'] = None
            logger.debug("Task {} will not be memoized".format(task_id))
            return None

        hashsum = make_hash(task)
        logger.debug("Task {} has memoization hash {}".format(task_id, hashsum))
        result = None
        if hashsum in self.memo_lookup_table:
            result = self.memo_lookup_table[hashsum]
            logger.info("Task %s using result from cache", task_id)
        else:
            logger.info("Task %s had no result in cache", task_id)

        task['hashsum'] = hashsum

        assert isinstance(result, Future) or result is None
        return result

    def update_memo_result(self, task: TaskRecord, r: Any) -> None:
        self._update_memo(task)
        if self.checkpoint_mode is not None:
            self._update_checkpoint(CheckpointCommand(task, result=r))

    def update_memo_exception(self, task: TaskRecord, e: BaseException) -> None:
        self._update_memo(task)
        if self.checkpoint_mode is not None:
            self._update_checkpoint(CheckpointCommand(task, exception=e))

    def _update_memo(self, task: TaskRecord) -> None:
        """Updates the memoization lookup table with the result from a task.
        This doesn't move any values around but associates the memoization
        hashsum with the completed (by success or failure) AppFuture.

        Args:
             - task (TaskRecord) : A task record from dfk.tasks
        """
        task_id = task['id']

        if not self.memoize or not task['memoize'] or 'hashsum' not in task:
            return

        if not isinstance(task['hashsum'], str):
            logger.error("Attempting to update app cache entry but hashsum is not a string key")
            return

        if task['hashsum'] in self.memo_lookup_table:
            logger.info(f"Replacing app cache entry {task['hashsum']} with result from task {task_id}")
        else:
            logger.debug(f"Storing app cache entry {task['hashsum']} with result from task {task_id}")
        self.memo_lookup_table[task['hashsum']] = task['app_fu']

    def _load_checkpoints(self, checkpointDirs: Sequence[str]) -> Dict[str, Future[Any]]:
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
                            memo_fu: Future = Future()
                            assert data['exception'] is None
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

    @typeguard.typechecked
    def load_checkpoints(self, checkpointDirs: Optional[Sequence[str]]) -> Dict[str, Future]:
        """Load checkpoints from the checkpoint files into a dictionary.

        The results are used to pre-populate the memoizer's lookup_table

        Kwargs:
             - checkpointDirs (list) : List of run folder to use as checkpoints
               Eg. ['runinfo/001', 'runinfo/002']

        Returns:
             - dict containing, hashed -> future mappings
        """
        if checkpointDirs:
            return self._load_checkpoints(checkpointDirs)
        else:
            return {}

    def _update_checkpoint(self, command: CheckpointCommand) -> None:
        if self.checkpoint_mode == 'task_exit':
            self.checkpoint_one(command)
        elif self.checkpoint_mode in ('manual', 'periodic', 'dfk_exit'):
            with self._checkpoint_lock:
                self.checkpointable_tasks.append(command)
        elif self.checkpoint_mode is None:
            pass
        else:
            assert False, "Invalid checkpoint mode {self.checkpoint_mode} - should have been validated at initialization"

    def checkpoint_one(self, cc: CheckpointCommand) -> None:
        """Checkpoint a single task to a checkpoint file.

        By default the checkpoints are written to the RUNDIR of the current
        run under RUNDIR/checkpoints/tasks.pkl

        Kwargs:
            - task : A task to checkpoint.

        .. note::
            Checkpointing only works if memoization is enabled

        """
        with self._checkpoint_lock:
            self._checkpoint_these_tasks([cc])

    def checkpoint_queue(self) -> None:
        """Checkpoint all tasks registered in self.checkpointable_tasks.

        By default the checkpoints are written to the RUNDIR of the current
        run under RUNDIR/checkpoints/tasks.pkl

        .. note::
            Checkpointing only works if memoization is enabled

        """
        with self._checkpoint_lock:
            self._checkpoint_these_tasks(self.checkpointable_tasks)
            self.checkpointable_tasks = []

    def checkpoint(self) -> None:
        """This is the user-facing interface to manual checkpointing.
        """
        self.checkpoint_queue()

    def _checkpoint_these_tasks(self, checkpoint_queue: List[CheckpointCommand]) -> None:
        """Play a sequence of CheckpointCommands into a checkpoint file.

        The checkpoint lock must be held when invoking this method.
        """

        # This checks that the lock is held, at least, but does not check that
        # it is held by the current thread - threading.Lock does not have a
        # concept of locking thread for threading.Lock.
        assert self._checkpoint_lock.locked(), "checkpoint system should be locked"

        checkpoint_dir = '{0}/checkpoint'.format(self.run_dir)
        checkpoint_tasks = checkpoint_dir + '/tasks.pkl'

        if not os.path.exists(checkpoint_dir):
            os.makedirs(checkpoint_dir, exist_ok=True)

        count = 0

        with open(checkpoint_tasks, 'ab') as f:
            for cc in checkpoint_queue:
                if cc.exception is None:
                    hashsum = cc.task_record['hashsum']
                    if not hashsum:
                        continue
                    t = {'hash': hashsum, 'exception': None, 'result': cc.result}

                    # We are using pickle here since pickle dumps to a file in 'ab'
                    # mode behave like a incremental log.
                    pickle.dump(t, f)
                    count += 1
                    logger.debug("Task {cc.task_record['id']} checkpointed")

        self.checkpointed_tasks += count

        if count == 0:
            if self.checkpointed_tasks == 0:
                logger.warning("No tasks checkpointed so far in this run. Please ensure caching is enabled")
            else:
                logger.debug("No tasks checkpointed in this pass.")
        else:
            logger.info("Done checkpointing {} tasks".format(count))
