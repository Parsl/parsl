"""This module implements DataFutures.
"""
import logging
from concurrent.futures import Future

import typeguard

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

    @typeguard.typechecked
    def __init__(self, fut: Future, file_obj: File, tid: int) -> None:
        """Construct the DataFuture object.

        If the file_obj is a string convert to a File.

        Args:
            - fut (Future) : Future that this DataFuture will track.
                             Completion of ``fut`` indicates that the data is
                             ready.
            - file_obj (File) : File that this DataFuture represents the availability of

        Kwargs:
            - tid (task_id) : Task id that this DataFuture tracks
        """
        super().__init__()
        self._tid = tid
        self.file_obj = file_obj
        self.parent = fut

        self.parent.add_done_callback(self.parent_callback)

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

    def __repr__(self) -> str:
        type_ = type(self)
        module = type_.__module__
        qualname = type_.__qualname__
        if self.done():
            done = "done"
        else:
            done = "not done"
        return f"<{module}.{qualname} object at {hex(id(self))} representing {repr(self.file_obj)} {done}>"
