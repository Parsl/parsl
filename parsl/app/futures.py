"""This module implements DataFutures.

We have two basic types of futures:
    1. DataFutures which represent data objects
    2. AppFutures which represent the futures on App/Leaf tasks.
"""
import os
import logging
from concurrent.futures import Future

from parsl.dataflow.futures import AppFuture, _STATE_TO_DESCRIPTION_MAP, FINISHED
from parsl.app.errors import *
from parsl.data_provider.files import File

logger = logging.getLogger(__name__)


class DataFuture(Future):
    """A datafuture points at an AppFuture.

    We are simply wrapping a AppFuture, and adding the specific case where, if
    the future is resolved i.e file exists, then the DataFuture is assumed to be
    resolved.

    The super.result of the future is set to parent_fu.result. TODO: WHY?
    The results of calling self.result() will give different behaviour though
      - fileobj. But add_done_callback will pass the parent result.
    TODO: a consistency unit test:   self.result() should return the same as
        a callback added with add_done_callback(). I believe (prior to this
        commit) that this is violated because of the above.
    """

    def parent_callback(self, parent_fu):
        """Callback from executor future to update the parent.

        Args:
            - parent_fu (Future): Future returned by the executor along with callback

        Returns:
            - None

        Updates the super() with the result() or exception()
        """

        # if parent_fu.done() is True: # TODO: would it ever not be in here?
        e = parent_fu._exception
        if e:
            super().set_exception(e)
        else:
            super().set_result(self.file_obj)

    def __init__(self, fut, file_obj, parent=None, tid=None):
        """Construct the DataFuture object.

        If the file_obj is a string (but not a File) convert to a File.

        Args:
            - fut (AppFuture) : AppFuture that this DataFuture will track
            - file_obj (string/File obj) : Something representing file(s)

        Kwargs:
            - parent ()
            - tid (task_id) : Task id that this DataFuture tracks
        """
        super().__init__()
        self._tid = tid

        # this is a weird test that comes from file being a 'str' subclass
        # which it almost definitely shouldn't be
        if isinstance(file_obj, str) and not isinstance(file_obj, File):
            self.file_obj = File(file_obj)
        else:
            self.file_obj = file_obj
        self.parent = parent

        if fut is None:
            logger.debug("Setting result to filepath immediately since no parent future was passed")
            self.set_result(self.file_obj)

        else:
            if isinstance(fut, Future):
                self.parent = fut
                self.parent.add_done_callback(self.parent_callback)
            else:
                raise NotFutureError("DataFuture can be created only with a FunctionFuture on None")

        logger.debug("Creating DataFuture with parent: %s", parent)
        logger.debug("Filepath: %s", self.filepath)

    @property
    def tid(self):
        """Returns the task_id of the task that will resolve this DataFuture."""
        return self._tid

    @property
    def filepath(self):
        """Filepath of the File object this datafuture represents."""
        return self.file_obj.filepath

    @property
    def filename(self):
        """Filepath of the File object this datafuture represents."""
        return self.filepath

    def result(self, timeout=None):
        """A blocking call that returns either the result or raises an exception.

        Assumptions : A DataFuture always has a parent AppFuture. The AppFuture does callbacks when
        setup.

        Kwargs:
            - timeout (int): Timeout in seconds

        Returns:
            - If App completed successfully returns the filepath.

        Raises:
            - Exception raised by app if failed.

        """
        if self.parent:
            if self.parent.done():
                # This explicit call to raise exceptions might be redundant.
                # the result() call *should* raise an exception if there's one
                e = self.parent._exception
                if e:
                    raise e
                else:
                    self.parent.result(timeout=timeout)
            else:
                self.parent.result(timeout=timeout)

        return self.file_obj

    def cancel(self):
        raise ValueError("cancel() not supported for data futures")

    def cancelled(self):
        raise ValueError("cancelled() not supported for data futures")

    def running(self):
        if self.parent:
            return self.parent.running()
        else:
            return False

    # def done(self):
    # this should be handled by super.done

    # def exception(self, timeout=None):
    # this should be handled by super.exception

    # def add_done_callback(self, fn):
    # this is removed because it should be handled by super.add_done_callback with no special casing here

    def __repr__(self):

        # The DataFuture could be wrapping an AppFuture whose parent is a Future
        # check to find the top level parent
        if isinstance(self.parent, AppFuture):
            parent = self.parent.parent
        else:
            parent = self.parent

        if parent:
            with parent._condition:
                if parent._state == FINISHED:
                    if parent._exception:
                        return '<%s at %#x state=%s raised %s>' % (
                            self.__class__.__name__,
                            id(self),
                            _STATE_TO_DESCRIPTION_MAP[parent._state],
                            parent._exception.__class__.__name__)
                    else:
                        return '<%s at %#x state=%s returned %s>' % (
                            self.__class__.__name__,
                            id(self),
                            _STATE_TO_DESCRIPTION_MAP[parent._state],
                            self.filepath)
                return '<%s at %#x state=%s>' % (
                    self.__class__.__name__,
                    id(self),
                    _STATE_TO_DESCRIPTION_MAP[parent._state])

        else:
            return '<%s at %#x state=%s>' % (
                self.__class__.__name__,
                id(self),
                _STATE_TO_DESCRIPTION_MAP[self._state])


def testing_nonfuture():
    fpath = '~/shuffled.txt'
    df = DataFuture(None, fpath)
    print(df)
    print("Result: ", df.filepath)
    assert df.filepath == os.path.abspath(os.path.expanduser(fpath))


if __name__ == "__main__":
    # logging.basicConfig(filename='futures.testing.log',level=logging.DEBUG)
    import sys
    import random
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logger.debug("Begin Testing")

    with open('shuffled.txt', 'w') as testfile:
        nums = list(range(0, 10000))
        random.shuffle(nums)
        for item in nums:
            testfile.write("{0}\n".format(item))

    foo = Future()
    df = DataFuture(foo, './shuffled.txt')
    dx = DataFuture(foo, '~/shuffled.txt')

    print(foo.done())
    print(df.done())

    testing_nonfuture()
