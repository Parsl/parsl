import hashlib
from functools import singledispatch
import logging
from parsl.executors.serialize.serialize import serialize_object
import types

logger = logging.getLogger(__name__)


@singledispatch
def id_for_memo(obj, output_ref=False):
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
@id_for_memo.register(types.FunctionType)
@id_for_memo.register(type(None))
def id_for_memo_serialize(obj, output_ref=False):
    return serialize_object(obj)[0]


@id_for_memo.register(list)
def id_for_memo_list(denormalized_list, output_ref=False):
    if type(denormalized_list) != list:
        raise ValueError("id_for_memo_list cannot work on subclasses of list")

    normalized_list = []

    for e in denormalized_list:
        normalized_list.append(id_for_memo(e, output_ref=output_ref))

    return serialize_object(normalized_list)[0]


@id_for_memo.register(dict)
def id_for_memo_dict(denormalized_dict, output_ref=False):
    """This normalises the keys and values of the supplied dictionary.

    When output_ref=True, the values are normalised as output refs, but
    the keys are not.
    """
    if type(denormalized_dict) != dict:
        raise ValueError("id_for_memo_dict cannot work on subclasses of dict")

    keys = sorted(denormalized_dict)

    normalized_list = []
    for k in keys:
        normalized_list.append(id_for_memo(k))
        normalized_list.append(id_for_memo(denormalized_dict[k], output_ref=output_ref))
    return serialize_object(normalized_list)[0]


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

        t = []

        # if kwargs contains an outputs parameter, that parameter is removed
        # and normalised differently - with output_ref set to True.
        # kwargs listed in ignore_for_cache will also be removed

        filtered_kw = task['kwargs'].copy()

        ignore_list = task['ignore_for_cache']

        logger.debug("Ignoring these kwargs for checkpointing: {}".format(ignore_list))
        for k in ignore_list:
            logger.debug("Ignoring kwarg {}".format(k))
            del filtered_kw[k]

        if 'outputs' in task['kwargs']:
            outputs = task['kwargs']['outputs']
            del filtered_kw['outputs']
            t = t + [id_for_memo(outputs, output_ref=True)]   # TODO: use append?

        t = t + [id_for_memo(filtered_kw)]

        t = t + [id_for_memo(task['func_name']),
                 id_for_memo(task['fn_hash']),
                 id_for_memo(task['args'])]

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
            - Result (Future): A completed future containing the memoized result

        This call will also set task['hashsum'] to the unique hashsum for the func+inputs.
        """
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

        return result

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
        if not self.memoize or not task['memoize'] or 'hashsum' not in task:
            return

        if task['hashsum'] in self.memo_lookup_table:
            logger.info('Updating app cache entry with latest %s:%s call' %
                        (task['func_name'], task_id))
            self.memo_lookup_table[task['hashsum']] = r
        else:
            self.memo_lookup_table[task['hashsum']] = r
