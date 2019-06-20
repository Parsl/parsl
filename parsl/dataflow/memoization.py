import hashlib
import logging
from parsl.executors.serialize.serialize import serialize_object

logger = logging.getLogger(__name__)


class Memoizer(object):
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

    def __init__(self, dfk, memoize=True, checkpoint={}):
        """Initialize the memoizer.

        Args:
            - dfk (DFK obj): The DFK object

        KWargs:
            - memoize (Bool): enable memoization or not.
            - checkpoint (Dict): A checkpoint loaded as a dict.
        """
        self.dfk = dfk
        self.memoize = memoize

        if self.memoize:
            logger.info("App caching initialized")
            self.memo_lookup_table = checkpoint
        else:
            logger.info("App caching disabled for all apps")
            self.memo_lookup_table = {}

    def make_hash(self, task):
        """Create a hash of the task inputs.

        This uses a serialization library borrowed from ipyparallel.
        If this fails here, then all ipp calls are also likely to fail due to failure
        at serialization.

        Args:
            - task (dict) : Task dictionary from dfk.tasks

        Returns:
            - hash (str) : A unique hash string
        """
        # Function name TODO: Add fn body later
        t = [serialize_object(task['func_name'])[0],
             serialize_object(task['fn_hash'])[0],
             serialize_object(task['args'])[0],
             serialize_object(task['kwargs'])[0],
             serialize_object(task['env'])[0]]
        x = b''.join(t)
        hashedsum = hashlib.md5(x).hexdigest()
        return hashedsum

    def check_memo(self, task_id, task):
        """Create a hash of the task and its inputs and check the lookup table for this hash.

        If present, the results are returned. The result is a tuple indicating whether a memo
        exists and the result, since a None result is possible and could be confusing.
        This seems like a reasonable option without relying on a cache_miss exception.

        Args:
            - task(task) : task from the dfk.tasks table

        Returns:
            Tuple of the following:
            - present (Bool): Is this present in the memo_lookup_table
            - Result (Py Obj): Result of the function if present in table

        This call will also set task['hashsum'] to the unique hashsum for the func+inputs.
        """
        if not self.memoize or not task['memoize']:
            task['hashsum'] = None
            return False, None

        hashsum = self.make_hash(task)
        present = False
        result = None
        if hashsum in self.memo_lookup_table:
            present = True
            result = self.memo_lookup_table[hashsum]
            logger.info("Task %s using result from cache", task_id)

        task['hashsum'] = hashsum
        return present, result

    def hash_lookup(self, hashsum):
        """Lookup a hash in the memoization table.

        Args:
            - hashsum (str): The same hashes used to uniquely identify apps+inputs

        Returns:
            - Lookup result

        Raises:
            - KeyError: if hash not in table
        """
        return self.memo_lookup_table[hashsum]

    def update_memo(self, task_id, task, r):
        """Updates the memoization lookup table with the result from a task.

        Args:
             - task_id (int): Integer task id
             - task (dict) : A task dict from dfk.tasks
             - r (Result future): Result future

        A warning is issued when a hash collision occurs during the update.
        This is not likely.
        """
        if not self.memoize or not task['memoize']:
            return

        if task['hashsum'] in self.memo_lookup_table:
            logger.info('Updating appCache entry with latest %s:%s call' %
                        (task['func_name'], task_id))
            self.memo_lookup_table[task['hashsum']] = r
        else:
            self.memo_lookup_table[task['hashsum']] = r
