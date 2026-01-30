import logging
import pickle
import sqlite3
import threading
from concurrent.futures import Future
from pathlib import Path
from typing import Any, Optional

from parsl.dataflow.memoization import Memoizer, make_hash
from parsl.dataflow.taskrecord import TaskRecord

logger = logging.getLogger(__name__)


class SQLiteMemoizer(Memoizer):
    """Memoize out of memory into an sqlite3 database.
    """

    def __init__(self, *, checkpoint_dir: str | None = None):
        self.checkpoint_dir = checkpoint_dir
        self._db_lock = threading.Lock()

    def start(self, *, run_dir: str, config_run_dir: str) -> None:
        self.run_dir = run_dir
        self.config_run_dir = config_run_dir

        dir = self.checkpoint_dir if self.checkpoint_dir is not None else self.config_run_dir

        self.db_path = Path(dir) / "checkpoint.sqlite3"
        logger.debug("starting with db_path %r", self.db_path)

        # check_same_thread should be safe if this assertion
        # passes.
        assert sqlite3.threadsafety == 3, "sqlite3 threadsafety violation"
        self._connection = sqlite3.connect(self.db_path, check_same_thread=False, autocommit=True)
        self._cursor = self._connection.cursor()

        with self._db_lock:
            self._cursor.execute("CREATE TABLE IF NOT EXISTS checkpoints(key PRIMARY KEY, result)")

        logger.debug("checkpoint table created")

    def close(self):
        logger.debug("closing sqlite3 connection")
        self._connection.close()

    def check_memo(self, task: TaskRecord) -> Optional[Future]:
        """TODO: document this: check_memo is required to set the task hashsum,
        if that's how we're going to key checkpoints in update_memo. (that's not
        a requirement though: other equalities are available."""
        logger.debug("check memo start")
        task_id = task['id']

        if not task['memoize']:
            task['hashsum'] = None
            logger.debug("Task %s will not be memoized", task_id)
            return None

        hashsum = make_hash(task)
        logger.debug("Task {} has memoization hash {}".format(task_id, hashsum))
        task['hashsum'] = hashsum

        logger.debug("checking memo")
        # connection = sqlite3.connect(self.db_path)
        # cursor = connection.cursor()
        with self._db_lock:
            self._cursor.execute("SELECT result FROM checkpoints WHERE key = ?", (hashsum, ))
            r = self._cursor.fetchone()

        logger.debug("checked memo")
        if r is None:
            return None
        else:
            data = pickle.loads(r[0])

            memo_fu: Future = Future()

            if data['exception'] is None:
                memo_fu.set_result(data['result'])
            else:
                assert data['result'] is None
                memo_fu.set_exception(data['exception'])

            return memo_fu

    def update_memo_result(self, task: TaskRecord, result: Any) -> None:
        logger.debug("updating memo")

        if not task['memoize'] or 'hashsum' not in task:
            logger.debug("preconditions for memo not satisfied")
            return

        if not isinstance(task['hashsum'], str):
            logger.error(f"Attempting to update app cache entry but hashsum is not a string key: {task['hashsum']}")
            return

        hashsum = task['hashsum']

        # this comes from the original concatenation-based checkpoint code:
        # assert app_fu.done(), "assumption: update_memo is called after future has a result"
        t = {'hash': hashsum, 'exception': None, 'result': result}
        # else:
        #    t = {'hash': hashsum, 'exception': app_fu.exception(), 'result': None}

        value = pickle.dumps(t)

        # connection = sqlite3.connect(self.db_path)
        # cursor = connection.cursor()

        with self._db_lock:
            self._cursor.execute("INSERT OR IGNORE INTO checkpoints VALUES(?, ?)", (hashsum, value))

        logger.debug("updated memo")

    def update_memo_exception(self, task: TaskRecord, exception: BaseException) -> None:
        logger.debug("updating memo")

        if not task['memoize'] or 'hashsum' not in task:
            logger.debug("preconditions for memo not satisfied")
            return

        if not isinstance(task['hashsum'], str):
            logger.error(f"Attempting to update app cache entry but hashsum is not a string key: {task['hashsum']}")
            return

        hashsum = task['hashsum']

        # this comes from the original concatenation-based checkpoint code:
        # assert app_fu.done(), "assumption: update_memo is called after future has a result"
        # t = {'hash': hashsum, 'exception': None, 'result': app_fu.result()}
        # else:
        t = {'hash': hashsum, 'exception': exception, 'result': None}

        value = pickle.dumps(t)

        # connection = sqlite3.connect(self.db_path)
        # cursor = connection.cursor()

        logger.debug("running sql")

        with self._db_lock:
            self._cursor.execute("INSERT INTO checkpoints VALUES(?, ?)", (hashsum, value))

        logger.debug("updating memo - finished")
