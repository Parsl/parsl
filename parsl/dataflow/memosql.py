import logging
import pickle
import sqlite3
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

    def start(self, *, run_dir: str) -> None:
        """TODO: run_dir is the per-workflow run dir, but we need a broader checkpoint context... one level up
        by default... get_all_checkpoints uses "runinfo/" as a relative path for that by default so replicating
        that choice would do here. likewise I think for monitoring."""

        self.run_dir = run_dir

        dir = self.checkpoint_dir if self.checkpoint_dir is not None else self.run_dir

        self.db_path = Path(dir) / "checkpoint.sqlite3"
        logger.debug("starting with db_path %r", self.db_path)

        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()

        cursor.execute("CREATE TABLE IF NOT EXISTS checkpoints(key, result)")
        # probably want some index on key because that's what we're doing all the access via.

        connection.commit()
        connection.close()
        logger.debug("checkpoint table created")

    def close(self):
        """TODO: probably going to need some kind of shutdown now, to close the sqlite3 connection."""
        pass

    def check_memo(self, task: TaskRecord) -> Optional[Future]:
        """TODO: document this: check_memo is required to set the task hashsum,
        if that's how we're going to key checkpoints in update_memo. (that's not
        a requirement though: other equalities are available."""
        task_id = task['id']

        if not task['memoize']:
            task['hashsum'] = None
            logger.debug("Task %s will not be memoized", task_id)
            return None

        hashsum = make_hash(task)
        logger.debug("Task {} has memoization hash {}".format(task_id, hashsum))
        task['hashsum'] = hashsum

        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        cursor.execute("SELECT result FROM checkpoints WHERE key = ?", (hashsum, ))
        r = cursor.fetchone()

        if r is None:
            connection.close()
            return None
        else:
            data = pickle.loads(r[0])
            connection.close()

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

        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()

        cursor.execute("INSERT INTO checkpoints VALUES(?, ?)", (hashsum, value))

        connection.commit()
        connection.close()

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

        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()

        cursor.execute("INSERT INTO checkpoints VALUES(?, ?)", (hashsum, value))

        connection.commit()
        connection.close()
