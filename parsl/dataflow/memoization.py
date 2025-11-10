from __future__ import annotations

import hashlib
import logging
import os
import pickle
import threading
import types
from concurrent.futures import Future
from functools import lru_cache, singledispatch
from typing import Any, Dict, List, Literal, Optional, Sequence

import typeguard

from parsl.dataflow.errors import BadCheckpoint
from parsl.dataflow.taskrecord import TaskRecord
from parsl.errors import ConfigurationError, InternalConsistencyError
from parsl.utils import Timer, get_all_checkpoints

logger = logging.getLogger(__name__)


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


class Memoizer:
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
                 memoize: bool = True,
                 checkpoint_files: Sequence[str] | None,
                 checkpoint_period: Optional[str],
                 checkpoint_mode: Literal['task_exit', 'periodic', 'dfk_exit', 'manual'] | None):
        """Initialize the memoizer.

        KWargs:
            - memoize (Bool): enable memoization or not.
            - checkpoint (Dict): A checkpoint loaded as a dict.
        """
        self.memoize = memoize

        self.checkpointed_tasks = 0

        self.checkpoint_lock = threading.Lock()

        self.checkpoint_files = checkpoint_files
        self.checkpoint_mode = checkpoint_mode
        self.checkpoint_period = checkpoint_period

        self.checkpointable_tasks: List[TaskRecord] = []

        self._checkpoint_timer: Timer | None = None

    def start(self) -> None:
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

    def make_hash(self, task: TaskRecord) -> str:
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

        hashsum = self.make_hash(task)
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

    def update_memo_exception(self, task: TaskRecord, e: BaseException) -> None:
        self._update_memo(task)

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

    def update_checkpoint(self, task_record: TaskRecord) -> None:
        if self.checkpoint_mode == 'task_exit':
            self.checkpoint_one(task=task_record)
        elif self.checkpoint_mode in ('manual', 'periodic', 'dfk_exit'):
            with self.checkpoint_lock:
                self.checkpointable_tasks.append(task_record)
        elif self.checkpoint_mode is None:
            pass
        else:
            raise InternalConsistencyError(f"Invalid checkpoint mode {self.checkpoint_mode}")

    def checkpoint_one(self, *, task: TaskRecord) -> None:
        """Checkpoint a single task to a checkpoint file.

        By default the checkpoints are written to the RUNDIR of the current
        run under RUNDIR/checkpoints/tasks.pkl

        Kwargs:
            - task : A task to checkpoint.

        .. note::
            Checkpointing only works if memoization is enabled

        """
        with self.checkpoint_lock:
            self._checkpoint_these_tasks([task])

    def checkpoint_queue(self) -> None:
        """Checkpoint all tasks registered in self.checkpointable_tasks.

        By default the checkpoints are written to the RUNDIR of the current
        run under RUNDIR/checkpoints/tasks.pkl

        .. note::
            Checkpointing only works if memoization is enabled

        """
        with self.checkpoint_lock:
            self._checkpoint_these_tasks(self.checkpointable_tasks)
            self.checkpointable_tasks = []

    def _checkpoint_these_tasks(self, checkpoint_queue: List[TaskRecord]) -> None:
        """Checkpoint a list of task records.

        The checkpoint lock must be held when invoking this method.
        """
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
