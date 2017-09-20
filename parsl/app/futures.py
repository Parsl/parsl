""" We have two basic types of futures:
    1. DataFutures which represent data objects
    2. AppFutures which represent the futures on App/Leaf tasks.
    This module implements the DataFutures.
"""
import os
import logging
from concurrent.futures import Future
from parsl.data_provider.files import File

logger = logging.getLogger(__name__)

# Possible future states (for internal use by the futures package).
PENDING = 'PENDING'
RUNNING = 'RUNNING'
# The future was cancelled by the user...
CANCELLED = 'CANCELLED'
# ...and _Waiter.add_cancelled() was called by a worker.
CANCELLED_AND_NOTIFIED = 'CANCELLED_AND_NOTIFIED'
FINISHED = 'FINISHED'

_STATE_TO_DESCRIPTION_MAP = {
    PENDING: "pending",
    RUNNING: "running",
    CANCELLED: "cancelled",
    CANCELLED_AND_NOTIFIED: "cancelled",
    FINISHED: "finished"
}

class DataFuture(Future):
    """ A datafuture points at an AppFuture

    We are simply wrapping a AppFuture, and adding the specific case where, if the future
    is resolved i.e file exists, then the DataFuture is assumed to be resolved.
    """

    def parent_callback(self, parent_fu):
        ''' Callback from executor future to update the parent.

        Args:
            - executor_fu (Future): Future returned by the executor along with callback

        Returns:
            - None

        Updates the super() with the result() or exception()
        '''

        if parent_fu.done() is True:
            e = parent_fu._exception
            if e:
                super().set_exception(e)
            else:
                super().set_result(parent_fu.result())
        return

    def __init__(self, fut, file_obj, parent=None, tid=None):
        ''' Construct the DataFuture object. If the file_obj is a string convert
        to a File.

        Args:
            - fut (AppFuture) : AppFuture that this DataFuture will track
            - file_obj (string/File obj) : Something representing file(s)

        Kwargs:
            - parent ()
            - tid (task_id) : Task id that this DataFuture tracks
        '''
        super().__init__()
        self._tid = tid
        if isinstance(file_obj, str):
            self.file_obj = File(file_obj)
        else:
            self.file_obj = file_obj
        self.parent = parent
        self._exception = None

        if fut == None:
            logger.debug("Setting result to filepath since no future was passed")
            self.set_result = self.file_obj

        else:
            if isinstance(fut, Future):
                self.parent = fut
                self.parent.add_done_callback(self.parent_callback)
            else:
                raise NotFutureError("DataFuture can be created only with a FunctionFuture on None")

        logger.debug("Creating DataFuture with parent : %s", parent)
        logger.debug("Filepath : %s", self.filepath)

    @property
    def tid(self):
        ''' Returns the task_id of the task that will resolve this DataFuture
        '''
        return self._tid

    @property
    def filepath(self):
        ''' Filepath of the File object this datafuture represents
        '''
        return self.file_obj.filepath

    @property
    def filename(self):
        ''' Filepath of the File object this datafuture represents
        '''
        return self.filepath

    def result(self, timeout=None):
        ''' A blocking call that returns either the result or raises an exception.
        Assumptions : A DataFuture always has a parent AppFuture. The AppFuture does callbacks when
        setup.

        Kwargs:
            - timeout (int): Timeout in seconds

        Returns:
            - If App completed successfully returns the filepath.

        Raises:
            - Exception raised by app if failed.

        '''

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

        return self.file_obj.filepath

    def cancel(self):
        ''' Cancel the task that this DataFuture is tracking.

            Note: This may not work
        '''
        if self.parent:
            return self.parent.cancel
        else:
            return False

    def cancelled(self):
        if self.parent:
            return self.parent.cancelled()
        else:
            return False

    def running(self):
        if self.parent:
            return self.parent.running()
        else:
            return False

    def done(self):
        if self.parent:
            return self.parent.done()
        else:
            return True

    def exception(self, timeout=None):
        if self.parent:
            return self.parent.exception(timeout=timeout)
        else:
            return True

    def add_done_callback(self, fn):
        if self.parent:
            return self.parent.add_done_callback(fn)
        else:
            return None

    def __repr__(self):

        if self.parent:
            with self.parent._condition:
                if self.parent._state == FINISHED:
                    if self.parent._exception:
                        return '<%s at %#x state=%s raised %s>' % (
                            self.__class__.__name__,
                            id(self),
                            _STATE_TO_DESCRIPTION_MAP[self.parent._state],
                            self.parent._exception.__class__.__name__)
                    else:
                        return '<%s at %#x state=%s returned %s>' % (
                            self.__class__.__name__,
                            id(self),
                            _STATE_TO_DESCRIPTION_MAP[self.parent._state],
                            self.filepath + '_file')
                return '<%s at %#x state=%s>' % (
                    self.__class__.__name__,
                    id(self),
                    _STATE_TO_DESCRIPTION_MAP[self.parent._state])

        else:
            return '<%s at %#x state=%s>' % (
                self.__class__.__name__,
                id(self),
                _STATE_TO_DESCRIPTION_MAP[self._state])




def testing_nonfuture():
    fpath = '~/shuffled.txt'
    df = DataFuture(None, fpath)
    print(df)
    print("Result : ", df.filepath)
    assert df.filepath == os.path.abspath(os.path.expanduser(fpath))

if __name__ == "__main__":
    #logging.basicConfig(filename='futures.testing.log',level=logging.DEBUG)
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
