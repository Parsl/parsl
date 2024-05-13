"""This module implements the DynamicFileList class and DynamicFile subclass.

The DynaicFile class is a drop in replacement/wrapper for the File and DataFuture classes. See
the XXXXXX documentation for specifics.

The DynamicFileList class is intended to replace the list of Files for app `outputs`. It acts like a
traditional Python `list`, but is also a Future. This allows for Files to be appended to the output list
and have these Files properly treated by Parsl.
"""
from __future__ import annotations
from concurrent.futures import Future
from typing import List, Optional, Union, Callable

import typeguard
import logging

from parsl.data_provider.files import File
from parsl.app.futures import DataFuture

logger = logging.getLogger(__name__)


class DynamicFileList(Future, list):
    """A list of files that is also a Future.

    This is used to represent the list of files that an app will produce.
    """

    class DynamicFile(Future):
        """A wrapper for a File or DataFuture


        """
        def parent_callback(self, parent_fu: Future):
            """Callback from executor future to update the parent.

            Updates the future with the result (the File object) or the parent future's
            exception.

            Args:
                - parent_fu (Future): Future returned by the executor along with callback

            Returns:
                - None
            """
            e = parent_fu.exception()
            if e:
                self.set_exception(e)
            else:
                self.set_result(self.file_obj)

        def __init__(self, fut: DynamicFileList, file_obj: Optional[Union[File, DataFuture]] = None):
            """Construct a DynamicFile instance

            If the file_obj is None, create an emptry instance, otherwise wrap file_obj.

            Args:
                - fut (AppFuture) : AppFuture that this DynamicFile will track
                - file_obj (File/DataFuture obj) : Something representing file(s)
            """
            super().__init__()
            self._is_df = isinstance(file_obj, DataFuture)
            self.parent = fut
            self.file_obj = file_obj
            self.parent.add_done_callback(self.parent_callback)
            self._empty = file_obj is None       #: Tracks whether this wrapper is empty

        @property
        def empty(self):
            """Return whether this is an empty wrapper."""
            return self._empty

        @typeguard.typechecked
        def set(self, file_obj: Union[File, DataFuture, 'DynamicFileList.DynamicFile']):
            """Set the file_obj for this instance.

            Args:
                - file_obj (File/DataFuture) : File or DataFuture to set
            """
            if isinstance(file_obj, type(self)):
                self.file_obj = file_obj.file_obj
            self.file_obj = file_obj
            self._empty = False
            self._is_df = isinstance(self.file_obj, DataFuture)
            self.parent.add_done_func(self.file_obj.filename, self.done)

        def done(self) -> bool:
            """Return whether the file_obj state is `done`.

            Returns:
                - bool : True if the file_obj is `done`, False otherwise
            """
            if self._is_df:
                return self.file_obj.done()
            return True  # Files are always done

        @property
        def tid(self):
            """Returns the task_id of the task that will resolve this DataFuture."""
            if self._is_df:
                return self.file_obj.tid

        @property
        def filepath(self):
            """Filepath of the File object this datafuture represents."""
            return self.file_obj.filepath

        @property
        def filename(self):
            """Filename of the File object this datafuture represents."""
            if self.file_obj is None:
                return None
            return self.file_obj.filepath

        def cancel(self):
            """Not implemented"""
            raise NotImplementedError("Cancel not implemented")

        def cancelled(self) -> bool:
            """Return False"""
            return False

        def running(self) -> bool:
            """Return whether the parent future is running"""
            if self.parent is not None:
                return self.parent.running()
            else:
                return False

        def exception(self, timeout=None):
            """Return None"""
            return None

        def __repr__(self) -> str:
            return self.file_obj.__repr__()

    def parent_callback(self, parent_fu):
        """Callback from executor future to update the parent.

        Updates the future with the result (the File object) or the parent future's
        exception.

        Args:
            - parent_fu (Future): Future returned by the executor along with callback

        Returns:
            - None
        """
        def _stub():
            return
        e = parent_fu.exception()
        if e:
            self.set_exception(e)
        else:
            for idx, f in enumerate(self):
                if isinstance(f, File): ### TODO: and not self.dataflow.check_staging_inhibited()
                    f_copy = f.cleancopy()
                    logger.debug("Submitting stage out for output file {}".format(repr(f)))
                    stageout_fut = self.dataflow.data_manager.stage_out(f_copy, self.executor, parent_fu)
                    if stageout_fut:
                        logger.debug("Adding a dependency on stageout future for {}".format(repr(f)))
                        parent_fu._outputs.append(DataFuture(stageout_fut, f, tid=parent_fu.tid))
                    else:
                        logger.debug("No stageout dependency for {}".format(repr(f)))
                        parent_fu._outputs.append(DataFuture(parent_fu, f, tid=parent_fu.tid))

                    # this is a hook for post-task stageout
                    # note that nothing depends on the output - which is maybe a bug
                    # in the not-very-tested stageout system?
                    func = self.dataflow.data_manager.replace_task_stage_out(f_copy, _stub, self.executor)
                    func()
                elif isinstance(f, DataFuture):
                    logger.debug("Not performing output staging for: {}".format(repr(f)))
                    parent_fu._outputs.append(DataFuture(parent_fu, f, tid=parent_fu.tid))

            self.set_result(self)

    '''''
    def file_callback(self, file_fu: Future):
        """Callback from executor future to update the file.

        Updates the future with the result (the File object) or the parent future's
        exception.

        Args:
            - file_fu (Future): Future returned by the executor along with callback

        Returns:
            - None
        """

        e = file_fu.exception()
        if e:
            self.files_done[file_fu.filename] = False
        else:
            self.files_done[file_fu.filename] = file_fu.done()
    '''
    @typeguard.typechecked
    def __init__(self, files: Optional[List[Union[File, DataFuture, DynamicFile]]] = None, fut: Optional[Future] = None):
        """Construct a DynamicFileList instance

        Args:
            - files (List[File/DataFuture]) : List of files to initialize the DynamicFileList with
            - fut (Future) : Future to set as the parent
        """
        super().__init__()
        self.files_done = {}      #: dict mapping file names to their "done" status True/False
        self._last_idx = -1
        self.executor = None
        self.parent = fut
        self.dataflow = None
        self._sub_callbacks = []
        self._in_callback = False
        if files is not None:
            self.extend(files)
        if fut is not None:
            self.parent.add_done_callback(self.parent_callback)

    def add_done_func(self, name: str, func: Callable):
        """ Add a function to the files_done dict, specifically for when an empty DynamicFile
        is updated to contain a real File.

        Args:
            - name (str) : Name of the file to add the function for
            - func (Callable) : Function to add
        """
        self.files_done[name] = func

    def wrap(self, file_obj: Union[File, DataFuture, None]):
        """ Wrap a file object in a DynamicFile

        Args:
            - file_obj (File/DataFuture) : File or DataFuture to wrap
        """
        return self.DynamicFile(self, file_obj)

    def set_dataflow(self, dataflow, executor: str):
        """ Set the dataflow and executor for this instance

        Args:
            - dataflow (DataFlowKernel) : Dataflow kernel that this instance is associated with
            - executor (str) : Executor that this instance is associated with
        """
        self.executor = executor
        self.dataflow = dataflow

    def set_parent(self, fut: Future):
        """ Set the parent future for this instance

        Args:
            - fut (Future) : Future to set as the parent
        """
        if self.parent is not None:
            raise ValueError("Parent future already set")
        self.parent = fut
        self.parent.add_done_callback(self.parent_callback)

    def cancel(self):
        """ Not implemented """
        raise NotImplementedError("Cancel not implemented")

    def cancelled(self):
        """ Not implemented """
        return False

    def running(self):
        """ Returns True if the parent future is running """
        if self.parent is not None:
            return self.parent.running()
        else:
            return False

    def result(self, timeout=None):
        """ Return self, which is the results of the file list """
        return self

    def exception(self, timeout=None):
        """ No-op"""
        return None

    def done(self):
        """ Return True if all files are done """
        for element in self.files_done.values():
            if not element():
                return False
        return True

    @typeguard.typechecked
    def append(self, __object: Union[File, DataFuture, 'DynamicFileList.DynamicFile']):
        """ Append a file to the list and update the files_done dict

        Args:
            - __object (File/DataFuture) : File or DataFuture to append
        """
        if not isinstance(__object, DynamicFileList.DynamicFile):
            __object = self.wrap(__object)
        if self._last_idx == len(self) - 1:
            super().append(__object)
        else:
            # must assume the object is empty, but exists
            super().__getitem__(self._last_idx + 1).set(__object)
        self.files_done[__object.filename] = super().__getitem__(self._last_idx + 1).done
        self._last_idx += 1
        self._call_callbacks()

    def extend(self, __iterable):
        """ Extend the list with the contents of the iterable and update the files_done dict

        Args:
            - __iterable (Iterable) : Iterable to extend the list with
        """
        items = []
        for f in __iterable:
            if not isinstance(f, (DynamicFileList.DynamicFile, File, DataFuture)):
                raise ValueError("DynamicFileList can only contain Files or DataFutures")
            if not isinstance(f, DynamicFileList.DynamicFile):
                f = self.wrap(f)
            self.files_done[f.filename] = f.done
            items.append(f)
        if self._last_idx == len(self) - 1:
            super().extend(items)
            self._last_idx += len(items)
            self._call_callbacks()
            return
        diff = len(self) - 1 - self._last_idx - len(items)
        if diff < 0:
            super().extend([self.wrap(None)] * abs(diff))
        for item in items:
            self._last_idx += 1
            self[self._last_idx].set(item)
            self.files_done[item.filename] = super().__getitem__(self._last_idx).done
        self._call_callbacks()

    def insert(self, __index: int, __object: Union[File, DataFuture, DynamicFile]):
        """ Insert a file into the list at the given index and update the files_done dict

        Args:
            - __index (int) : Index to insert the file at
            - __object (File/DataFuture) : File or DataFuture to insert
        """
        if __index > self._last_idx:
            raise ValueError("Cannot insert at index greater than the last index")
        if not isinstance(__object, self.DynamicFile):
            __object = self.wrap(__object)
        self.files_done[__object.filename] = __object.done
        super().insert(__index, __object)
        self._last_idx += 1
        self._call_callbacks()

    def remove(self, __value):
        """ Remove a file from the list and update the files_done dict

        Args:
            - __value (File/DataFuture) : File or DataFuture to remove
        """
        del self.files_done[__value.filename]
        super().remove(__value)
        self._last_idx -= 1
        self._call_callbacks()

    def pop(self, __index: int = -1) -> Union[File, DataFuture]:
        """ Pop a file from the list and update the files_done dict

        Args:
            - __index (int) : Index to pop the file at

        Returns:
            - File/DataFuture : File or DataFuture that was popped
        """
        if __index == -1:
            value = super().pop(self._last_idx)
        elif __index <= self._last_idx:
            value = super().pop(__index)
        else:
            raise IndexError("Index out of range")
        del self.files_done[value.filename]
        self._last_idx -= 1
        self._call_callbacks()
        return value.file_obj

    def clear(self):
        """ Clear the list and the files_done dict """
        self.files_done.clear()
        self._last_idx = -1
        super().clear()
        # detach all the callbacks so that sublists can still be used
        self._sub_callbacks.clear()

    def _call_callbacks(self):
        """ Call the callbacks for the sublists """
        if self._in_callback:
            return
        self._in_callback = True
        for cb in self._sub_callbacks:
            cb()
        self._in_callback = False

    def _expand(self, idx):
        for _ in range(idx - len(self) + 1):
            super().append(self.wrap(None))

    @typeguard.typechecked
    def __setitem__(self, key: int, value: Union[File, DataFuture, 'DynamicFileList.DynamicFile']):
        if self[key].filename in self.files_done:
            del self.files_done[self[key].filename]
        if super().__getitem__(key).empty:
            super().__getitem__(key).set(value)
            self.files_done[super().__getitem__(key).filename] = super().__getitem__(key).done
            self._last_idx = max(self._last_idx, key)
        else:
            if not isinstance(value, self.DynamicFile):
                value = self.wrap(value)
            super().__setitem__(key, value)
            self.files_done[value.filename] = value.done
        self._call_callbacks()

    def __getitem__(self, key):
        # make sure the list will be long enough when it is filled, so we can return a future
        if isinstance(key, slice):
            if key.start is None:
                pass
            elif key.start >= len(self):
                for i in range(len(self), key.start + 1):
                    self.append(self.wrap(None))
            if key.stop is not None and key.stop > len(self):
                for i in range(len(self), key.stop):
                    self.append(self.wrap(None))
            ret = DynamicFileSubList(key, super().__getitem__(key), self)
            self._sub_callbacks.append(ret.callback)
            return ret
        else:
            if key >= len(self):
                self._expand(key)
            return super().__getitem__(key)

    def get_update(self, key: slice):
        """Get an updated slice for the sublist.

        Args:
            - key (slice) : Slice to update

        Returns:
            - List[DynamicFile] : Updated slice
        """
        return super().__getitem__(key)

    def __delitem__(self, key):
        del self.files_done[self[key].filename]
        super().__delitem__(key)
        self._call_callbacks()

    def __repr__(self):
        type_ = type(self)
        module = type_.__module__
        qualname = type_.__qualname__
        if self.done():
            done = "done"
        else:
            done = "not done"
        return f"<{module}.{qualname} object at {hex(id(self))} containing {len(self)} objects {done}>"


class DynamicFileSubList(DynamicFileList):
    @typeguard.typechecked
    def __init__(self, key: slice, files: Optional[List[DynamicFileList.DynamicFile]], parent: DynamicFileList):
        super().__init__(files=files)
        self.parent = parent
        self.slice = key
        self.fixed_size = key.stop is not None and key.start is not None

    def callback(self):
        """Callback for updating the sublist when the parent list is updated."""
        self.clear()
        self.extend(self.parent.get_update(self.slice))
