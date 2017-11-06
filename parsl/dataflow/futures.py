""" AppFuture

    We have two basic types of futures:
    1. DataFutures which represent data objects
    2. AppFutures which represent the futures on App/Leaf tasks.
    This module implements the AppFutures

"""

from concurrent.futures import Future
import logging
from parsl.dataflow.error import *

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

class AppFuture(Future):
    """ An AppFuture points at a Future returned from an Executor

    We are simply wrapping a AppFuture, and adding the specific case where, if the future
    is resolved i.e file exists, then the DataFuture is assumed to be resolved.

    """
    def parent_callback(self, executor_fu):
        ''' Callback from executor future to update the parent.

        Args:
            - executor_fu (Future): Future returned by the executor along with callback

        Returns:
            - None

        Updates the super() with the result() or exception()
        '''
        if executor_fu.done() == True:
            try :
                super().set_result(executor_fu.result())
            except Exception as e:
                super().set_exception(e)


    def __init__ (self, parent, tid=None):
        self._tid = tid
        super().__init__()
        self.parent   = parent
        #if self.parent:
        #    parent.add_done_callback(self.parent_callback)
        self._outputs = []

    @property
    def tid(self):
        return self._tid

    def update_parent(self, fut):
        ''' Handle the case where the user has called result on the AppFuture
        before the parent exists. Add a callback to the parent to update the
        state
        '''
        self.parent = fut
        fut.add_done_callback(self.parent_callback)

    def result(self, timeout=None):

        if self.parent :
            x = self.parent._exception
            if x :
                raise x
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
        ''' Check if the future is done.
        If a parent is set, we return the status of the parent.
        else, there is no parent assigned, meaning the status is False.

        Returns:
              - True : If the future has successfully resolved.
              - False : Pending resolution
        '''

        if self.parent:
            return self.parent.done()
        else:
            return False

    def exception(self, timeout=None):
        if self.parent:
            return self.parent.exception(timeout=timeout)
        else:
            return False

    def add_done_callback(self, fn):
        if self.parent:
            return self.parent.add_done_callback(fn)
        else:
            return None

    @property
    def outputs(self):
        return self._outputs

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
                            self.parent._result.__class__.__name__ )
                return '<%s at %#x state=%s>' % (
                    self.__class__.__name__,
                    id(self),
                    _STATE_TO_DESCRIPTION_MAP[self.parent._state])
        else:
            return '<%s at %#x state=%s>' % (
                self.__class__.__name__,
                id(self),
                _STATE_TO_DESCRIPTION_MAP[self._state])
