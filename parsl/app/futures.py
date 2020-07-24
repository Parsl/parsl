"""This module implements DataFutures.
"""
import logging
from concurrent.futures import Future

from parsl.dataflow.futures import _STATE_TO_DESCRIPTION_MAP, FINISHED
from parsl.app.errors import NotFutureError
from parsl.data_provider.files import File

logger = logging.getLogger(__name__)


class DataFuture(Future):
    """A datafuture points at an AppFuture.

    We are simply wrapping a AppFuture, and adding the specific case where, if
    the future is resolved i.e file exists, then the DataFuture is assumed to be
    resolved.
    """

    def parent_callback(self, parent_fu):
        """Callback from executor future to update the parent.

        Updates the future with the result (the File object) or the parent future's
        exception.

        Args:
            - parent_fu (Future): Future returned by the executor along with callback

        Returns:
            - None
        """

        e = parent_fu._exception
        if e:
            self.set_exception(e)
        else:
            self.set_result(self.file_obj)

    def __init__(self, fut, file_obj, tid=None):
        """Construct the DataFuture object.

        If the file_obj is a string convert to a File.

        Args:
            - fut (AppFuture) : AppFuture that this DataFuture will track
            - file_obj (string/File obj) : Something representing file(s)

        Kwargs:
            - tid (task_id) : Task id that this DataFuture tracks
        """
        super().__init__()
        self._tid = tid
        if isinstance(file_obj, str):
            raise ValueError("DataFuture constructed with a string, not a File. This is no longer supported.")
        elif isinstance(file_obj, File):
            self.file_obj = file_obj
        else:
            raise ValueError("DataFuture must be initialized with a File")
        self.parent = fut

        if fut is None:
            logger.debug("Setting result to filepath immediately since no parent future was passed")
            self.set_result(self.file_obj)
        elif isinstance(fut, Future):
            self.parent.add_done_callback(self.parent_callback)
        else:
            raise NotFutureError("DataFuture parent must be either another Future or None")

        logger.debug("Creating DataFuture with parent: %s and file: %s", self.parent, repr(self.file_obj))

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

    def cancel(self):
        raise NotImplementedError("Cancel not implemented")

    def cancelled(self):
        return False

    def running(self):
        if self.parent:
            return self.parent.running()
        else:
            return False

    def __repr__(self):

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
                        return '<%s at %#x state=%s with file %s>' % (
                            self.__class__.__name__,
                            id(self),
                            _STATE_TO_DESCRIPTION_MAP[parent._state],
                            repr(self.file_obj))
                return '<%s at %#x state=%s>' % (
                    self.__class__.__name__,
                    id(self),
                    _STATE_TO_DESCRIPTION_MAP[parent._state])

        else:
            return '<%s at %#x state=%s>' % (
                self.__class__.__name__,
                id(self),
                _STATE_TO_DESCRIPTION_MAP[self._state])
