""" We have two basic types of futures:
    1. DataFutures which represent data objects
    2. AppFutures which represent the futures on App/Leaf tasks.
    This module implements the DataFutures.
"""
from concurrent.futures import Future
import logging
import os
import parsl.app.errors

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

    def __init__ (self, fut, filepath, parent=None, filetype='local'):
        super().__init__()
        self.filetype = filetype
        self.filepath = os.path.abspath(os.path.expanduser(filepath))
        self.parent   = parent

        if fut == None:
            logger.debug("Setting result to filepath since no future was passed")
            self.set_result = self.filepath

        else:
            if isinstance(fut, Future):
                self.parent = fut
            else:
                raise NotFutureError("DataFuture can be created only with a FunctionFuture on None")

        logger.debug("Filepath : %s", self.filepath)

    def result(self, timeout=None):
        if not self.parent :
            return self.parent.result(timeout=timeout)
        else:
            return super().result(timeout=timeout)

    def cancel(self):
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
                            self.filetype + '_file' )
                return '<%s at %#x state=%s>' % (
                    self.__class__.__name__,
                    id(self),
                    _STATE_TO_DESCRIPTION_MAP[self.parent._state])

        else:
            return '<%s at %#x state=%s>' % (
                self.__class__.__name__,
                id(self),
                _STATE_TO_DESCRIPTION_MAP[self._state])




def AppFuture(Future):

    def __init__ (self, fut, filepath):

        super().__init__()



def testing_nonfuture():
    fpath = '~/shuffled.txt'
    df = DataFuture(None, fpath)
    print (df)
    print ("Result : ", df.filepath)
    assert df.filepath == os.path.abspath(os.path.expanduser(fpath))

if __name__ == "__main__":
    #logging.basicConfig(filename='futures.testing.log',level=logging.DEBUG)
    import sys
    import random
    logging.basicConfig(stream=sys.stdout,level=logging.DEBUG)
    logger.debug("Begin Testing")

    with open('shuffled.txt', 'w') as testfile:
        nums = list(range(0,10000))
        random.shuffle(nums)
        for item in nums:
            testfile.write("{0}\n".format(item))

    foo = Future()
    df  = DataFuture(foo, './shuffled.txt')
    dx  = DataFuture(foo, '~/shuffled.txt')

    print(foo.done())
    print(df.done())


    testing_nonfuture()
