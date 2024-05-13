"""This module implements DataFutures.
"""
import logging
import os.path
from concurrent.futures import Future
from datetime import datetime, timezone
from hashlib import md5
from os import stat
from typing import Optional

import typeguard

from parsl.data_provider.files import File

logger = logging.getLogger(__name__)


class DataFuture(Future):
    """A datafuture points at an AppFuture.

    We are simply wrapping a AppFuture, and adding the specific case where, if
    the future is resolved i.e. file exists, then the DataFuture is assumed to be
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
            self.update_file_provenance()

    @typeguard.typechecked
    def __init__(self, fut: Future, file_obj: File, tid: int, track_provenance: Optional[bool] = False) -> None:
        """Construct the DataFuture object.

        If the file_obj is a string convert to a File.

        Args:
            - fut (Future) : Future that this DataFuture will track.
                             Completion of ``fut`` indicates that the data is
                             ready.
            - file_obj (File) : File that this DataFuture represents the availability of
            - tid (task_id) : Task id that this DataFuture tracks
        Kwargs:
            - track_provenance (bool) : If True then track the underlying file's provenance. Default is False.
        """
        super().__init__()
        self._tid = tid
        self.file_obj = file_obj
        self.parent = fut
        self.track_provenance = track_provenance
        self.parent.add_done_callback(self.parent_callback)
        # only capture this if needed
        if self.track_provenance and self.file_obj.scheme == 'file' and os.path.exists(file_obj.path):
            file_stat = os.stat(file_obj.path)
            self.file_obj.timestamp = datetime.fromtimestamp(file_stat.st_ctime, tz=timezone.utc)
            self.file_obj.size = file_stat.st_size
            self.file_obj.md5sum = md5(open(self.file_obj, 'rb').read()).hexdigest()

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

    @property
    def uu_id(self):
        """UUID of the File object this datafuture represents."""
        return self.file_obj.uu_id

    @property
    def timestamp(self):
        """Timestamp when the future was marked done."""
        return self.file_obj.timestamp

    @timestamp.setter
    def timestamp(self, value: Optional[datetime]) -> None:
        self.file_obj.timestamp = value

    @property
    def size(self):
        """Size of the file."""
        return self.file_obj.size

    @property
    def md5sum(self):
        """MD5 sum of the file."""
        return self.file_obj.md5sum

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

    def update_file_provenance(self):
        """ Update any file provenance information, but only if the file object if it is a File
        """
        if self.track_provenance and self.file_obj.scheme == 'file' and os.path.isfile(self.file_obj.filepath):
            if not self.file_obj.timestamp:
                self.file_obj.timestamp = datetime.fromtimestamp(stat(self.file_obj.filepath).st_ctime, tz=timezone.utc)
            if not self.file_obj.size:
                self.file_obj.size = stat(self.file_obj.filepath).st_size
            if not self.file_obj.md5sum:
                self.file_obj.md5sum = md5(open(self.file_obj, 'rb').read()).hexdigest()
