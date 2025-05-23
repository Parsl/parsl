"""This module implements the DynamicFileList class and DynamicFile subclass.

The DynamicFile class is a drop in replacement/wrapper for the File and DataFuture classes.

The DynamicFileList class is intended to replace the list of Files for app `outputs`. It acts like a
traditional Python `list`, but is also a Future. This allows for Files to be appended to the output list
and have these Files properly treated by Parsl.
"""
from __future__ import annotations

import logging
import sys
from concurrent.futures import Future
from copy import deepcopy
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Union

import typeguard

from parsl.app.futures import DataFuture
from parsl.data_provider.files import File
from parsl.dataflow.futures import AppFuture
from parsl.dataflow.taskrecord import deepcopy as trcopy

logger = logging.getLogger(__name__)


class DynamicFileList(Future):
    """A list of files that is also a Future.

    The DynamicFileList class is intended to replace the list of Files for app `outputs`. It acts like a
    traditional Python `list`, but is also a Future. This allows for Files to be appended to the output list
    and have these Files properly treated by Parsl.
    """

    class DynamicFile(Future):
        """A wrapper for a File or DataFuture

           Should not be instantiated from outside the DynamicFileList class.
        """
        def parent_callback(self, parent_fu: Future) -> None:
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
                self.file_obj.timestamp = datetime.now(timezone.utc)
                self.parent.dfk.register_as_output(self.file_obj, self.parent.task_record)
                if self._is_df:
                    self.set_result(self.file_obj.file_obj)
                else:
                    self.set_result(self.file_obj)

        def __init__(self, fut: DynamicFileList,
                     file_obj: Optional[Union[File, DataFuture]] = None):
            """Construct a DynamicFile instance

            If the file_obj is None, create an empty instance, otherwise wrap file_obj.

            Args:
                - fut (AppFuture) : AppFuture that this DynamicFile will track
                - file_obj (File/DataFuture obj) : Something representing file(s)
            """  # TODO need to be able to link output and input dynamic file objects and update when the output changes
            super().__init__()
            self._is_df = isinstance(file_obj, DataFuture)
            self.parent = fut
            self.file_obj = file_obj
            self.parent.add_done_callback(self.parent_callback)
            self._empty = file_obj is None       #: Tracks whether this wrapper is empty
            self._staged_out = False

        @property
        def staged(self) -> bool:
            """Return whether this file has been staged out."""
            return self._staged_out

        @property
        def scheme(self) -> Union[str, None]:
            """Return the scheme for the wrapped file object."""
            if self.empty:
                return None
            if self._is_df:
                return self.file_obj.file_obj.scheme
            return self.file_obj.scheme

        @property
        def empty(self) -> bool:
            """Return whether this is an empty wrapper."""
            return self._empty

        @property
        def uuid(self) -> Union[str, None]:
            """Return the uuid of the file object this data-future represents."""
            if self._empty:
                return None
            return self.file_obj.uuid

        @property
        def timestamp(self) -> Union[datetime, None]:
            """Return the timestamp of the file object this data-future represents."""
            if self._empty:
                return None
            return self.file_obj.timestamp

        @timestamp.setter
        def timestamp(self, timestamp: Optional[datetime]) -> None:
            """Set the timestamp"""
            self.file_obj.timestamp = timestamp

        @typeguard.typechecked
        def set_file(self, file_obj: Union[File, DataFuture, 'DynamicFileList.DynamicFile']):
            """Set the file_obj for this instance.

            Args:
                - file_obj (File/DataFuture) : File or DataFuture to set
            """
            if isinstance(file_obj, type(self)):
                self.file_obj = file_obj.file_obj
            else:
                self.file_obj = file_obj
            self._empty = self.file_obj is None
            self._is_df = isinstance(self.file_obj, DataFuture)
            if self.file_obj is not None:
                self.parent.add_done_func(self.file_obj.filename, self.done)

        def cleancopy(self) -> File:
            """Create a clean copy of the file_obj."""
            if self._is_df:
                return self.file_obj.file_obj.cleancopy()
            return self.file_obj.cleancopy()

        def convert_to_df(self, dfk) -> None:
            """Convert the file_obj to a DataFuture.

            Parameters
            ----------
            dfk : DataFlowKernel
                The dataflow kernel that this instance is associated with
            """
            if not self._is_df:
                self.file_obj = DataFuture(self.parent, self.file_obj, tid=self.parent._output_task_id,
                                           dfk=dfk)
                self._is_df = True

        def done(self) -> bool:
            """Return whether the file_obj state is `done`.

            Returns:
                - bool : True if the file_obj is `done`, False otherwise
            """
            if self._is_df:
                return self.file_obj.done()
            if self._empty:
                return False
            return True  # Files are always done

        @property
        def is_df(self) -> bool:
            """Return whether this instance wraps a DataFuture."""
            return self._is_df

        @property
        def tid(self) -> Union[int, None]:
            """Returns the task_id of the task that will resolve this DataFuture."""
            if self._is_df:
                return self.file_obj.tid

        @property
        def filepath(self) -> Union[str, None]:
            """Filepath of the File object this data-future represents."""
            if self.file_obj is None:
                return None
            return self.file_obj.filepath

        @property
        def filename(self) -> Union[str, None]:
            """Filename of the File object this data-future represents."""
            if self.file_obj is None:
                return None
            return self.file_obj.filepath

        @property
        def size(self) -> Union[int, None]:
            """Size of the file."""
            if self._empty:
                return None
            if self._is_df:
                return self.file_obj.file_obj.size
            return self.file_obj.size

        @size.setter
        def size(self, f_size: int):
            """ Set the size of the file """
            if self.file_obj:
                if self._is_df:
                    self.file_obj.file_obj.size = f_size
                else:
                    self.file_obj.size = f_size

        @property
        def md5sum(self) -> Union[str, None]:
            """MD5 sum of the file."""
            if self._empty:
                return None
            if self._is_df:
                return self.file_obj.file_obj.md5sum
            return self.file_obj.md5sum

        @md5sum.setter
        def md5sum(self, sum: str):
            """ Set MD5 sum for file """
            if self.file_obj:
                if self._is_df:
                    self.file_obj.file_obj.md5sum = sum
                else:
                    self.file_obj.md5sum = sum

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

        def __eq__(self, other: DynamicFileList.DynamicFile) -> bool:
            return self.uuid == other.uuid

        def __ne__(self, other: DynamicFileList.DynamicFile) -> bool:
            return self.uuid != other.uuid

        def __gt__(self, other: DynamicFileList.DynamicFile) -> bool:
            if self.file_obj is not None and other.file_obj is not None:
                return self.filepath > other.filepath
            if self.file_obj is None:
                return False
            return True

        def __lt__(self, other: DynamicFileList.DynamicFile) -> bool:
            if self.file_obj is not None and other.file_obj is not None:
                return self.filepath < other.filepath
            if self.file_obj is None:
                return True
            return False

        def __ge__(self, other: DynamicFileList.DynamicFile) -> bool:
            return self.__gt__(other) or self.__eq__(other)

        def __le__(self, other: DynamicFileList.DynamicFile) -> bool:
            return self.__lt__(other) or self.__eq__(other)

    def parent_callback(self, parent_fu):
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
            if self._watcher is not None:
                self._watcher()
                self._watcher = None
            self._is_done = True
            self.parent._outputs = self
            self.set_result(self)

    @typeguard.typechecked
    def __init__(self, files: Optional[List[Union[File, DataFuture, 'self.DynamicFile']]] = None):
        """Construct a DynamicFileList instance

        Args:
            - files (List[File/DataFuture]) : List of files to initialize the DynamicFileList with
            - fut (Future) : Future to set as the parent
        """
        super().__init__()
        self.files_done: Dict[str, Callable] = {}      #: dict mapping file names to their "done" status True/False
        self._last_idx = -1
        self.executor: str = ''
        self.parent: Union[AppFuture, None] = None
        self.dfk = None
        self._sub_callbacks: List[Callable] = []
        self._in_callback = False
        self._staging_inhibited = False
        self._output_task_id = None
        self.task_record = None
        self._files: List[DynamicFileList.DynamicFile] = []
        self._is_done = False
        if files is not None:
            self.extend(files)
        self._watcher = None

    def add_watcher_callback(self, func: Callable):
        """ Add a function to call when a bash_watch app completes, to collect the files.

        Args:
            - func (Callable) : function to use
        """
        self._watcher = func

    def add_done_func(self, name: str, func: Callable):
        """ Add a function to the files_done dict, specifically for when an empty DynamicFile
        is updated to contain a real File.

        Args:
            - name (str) : Name of the file to add the function for
            - func (Callable) : Function to add
        """
        self.files_done[name] = func

    def stage_file(self, idx: int):
        """ Stage a file at the given index, we do this now because so that the app and dataflow
        can act accordingly when the app finishes.

        Args:
            - idx (int) : Index of the file to stage
        """
        if self.dfk is None:
            return
        out_file = self[idx]
        if out_file.empty or out_file.staged:
            return
        if self.parent is None or not out_file.is_df:
            return
        if self._staging_inhibited:
            logger.debug("Not performing output staging for: {}".format(repr(out_file.file_obj)))
        else:
            f_copy = out_file.file_obj.file_obj.cleancopy()
            self[idx].file_obj.file_obj = f_copy
            logger.debug("Submitting stage out for output file {}".format(repr(out_file.file_obj)))
            stageout_fut = self.dfk.data_manager.stage_out(f_copy, self.executor, self.parent)
            if stageout_fut:
                logger.debug("Adding a dependency on stageout future for {}".format(repr(out_file)))
                self[idx].file_obj.parent = stageout_fut
                self[idx].file_obj._tid = self.parent.tid
            else:
                logger.debug("No stageout dependency for {}".format(repr(f_copy)))
                # self.parent._outputs.append(DataFuture(self.parent, out_file.file_obj.file_obj, tid=self.parent.tid))
            func = self.dfk.tasks[self._output_task_id]['func']
            # this is a hook for post-task stage-out
            # note that nothing depends on the output - which is maybe a bug
            # in the not-very-tested stage-out system?
            func = self.dfk.data_manager.replace_task_stage_out(f_copy, func, self.executor)
            self.dfk.tasks[self._output_task_id]['func'] = func
        self.parent._outputs = self
        self._call_callbacks()
        # TODO dfk._gather_all_deps

    def count(self, item) -> int:
        """ Return the count of the given item in the list"""
        return self._files.count(item)

    def wrap(self, file_obj: Union[File, DataFuture, None]) -> DynamicFile:
        """ Wrap a file object in a DynamicFile

        Args:
            - file_obj (File/DataFuture) : File or DataFuture to wrap
        """
        return self.DynamicFile(self, file_obj)

    def set_dataflow(self, dataflow, executor: str, st_inhibited: bool, task_id: int, task_record: dict):
        """ Set the dataflow and executor for this instance

        Args:
            - dataflow (DataFlowKernel) : Dataflow kernel that this instance is associated with
            - executor (str) : Executor that this instance is associated with
            - st_inhibited (bool) : Whether staging is inhibited
        """
        self.executor = executor
        self.dfk = dataflow
        self._staging_inhibited = st_inhibited
        self._output_task_id = task_id
        self.task_record = task_record
        for idx in range(self._last_idx + 1):
            self.stage_file(idx)

    def set_parent(self, fut: AppFuture):
        """ Set the parent future for this instance

        Args:
            - fut (Future) : Future to set as the parent
        """
        if self.parent is not None:
            raise ValueError("Parent future already set")
        self.parent = fut
        self.parent.add_done_callback(self.parent_callback)
        for idx in range(self._last_idx + 1):
            self[idx].convert_to_df(self.dfk)
            self.stage_file(idx)
        self._call_callbacks()

    def cancel(self):
        """ Not implemented """
        raise NotImplementedError("Cancel not implemented")

    def cancelled(self) -> bool:
        """ Not implemented """
        return False

    def running(self) -> bool:
        """ Returns True if the parent future is running """
        if self.parent is not None:
            return self.parent.running()
        else:
            return False

    #def result(self, timeout=None) -> 'DynamicFileList':
    #    """ Return self, which is the results of the file list """
    #    return self

    def exception(self, timeout=None) -> None:
        """ No-op"""
        return None

    def done(self, ) -> bool:
        """ Return True if all files are done """
        if not self._is_done:
            return False
        for element in self.files_done.values():
            if not element():
                return False
        return True

    def __len__(self) -> int:
        return len(self._files)

    def __eq__(self, other: DynamicFileList) -> bool:
        return self._files == other._files

    def __ne__(self, other: DynamicFileList) -> bool:
        return self._files != other._files

    def __lt__(self, other: DynamicFileList) -> bool:
        return self._files < other._files

    def __le__(self, other: DynamicFileList) -> bool:
        return self._files <= other._files

    def __gt__(self, other: DynamicFileList) -> bool:
        return self._files > other._files

    def __ge__(self, other: DynamicFileList) -> bool:
        return self._files >= other._files

    @typeguard.typechecked
    def append(self, __object: Union[File, DataFuture, 'DynamicFileList.DynamicFile']):
        """ Append a file to the list and update the files_done dict

        Args:
            - __object (File/DataFuture) : File or DataFuture to append
        """
        if not isinstance(__object, DynamicFileList.DynamicFile):
            if self.parent is not None and isinstance(__object, File):
                __object = DataFuture(self.parent, __object, tid=self._output_task_id,
                                      dfk=self.dfk)
            __object = self.wrap(__object)
        if self._last_idx == len(self) - 1:
            self._files.append(__object)
        else:
            # must assume the object is empty, but exists
            self._files[self._last_idx + 1].set_file(__object)
        self.files_done[__object.filename] = self._files[self._last_idx + 1].done
        self._last_idx += 1
        self.stage_file(self._last_idx)
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
                if self.parent is not None and isinstance(f, File):
                    f = DataFuture(self.parent, f, tid=self._output_task_id,
                                   dfk=self.dfk)
                f = self.wrap(f)
            self.files_done[f.filename] = f.done
            items.append(f)
        if self._last_idx == len(self) - 1:
            self._files.extend(items)
            for i in range(len(items)):
                self._last_idx += 1
                self.stage_file(self._last_idx)
            self._call_callbacks()
            return
        diff = len(self) - 1 - self._last_idx - len(items)
        if diff < 0:
            self._files.extend([self.wrap(None)] * abs(diff))
        for item in items:
            self._last_idx += 1
            self[self._last_idx].set_file(item)
            self.files_done[item.filename] = self._files[self._last_idx].done
            self.stage_file(self._last_idx)
        self._call_callbacks()

    def index(self,
              item: Union[File, DataFuture, DynamicFile],
              start: Optional[int] = 0,
              stop: Optional[int] = sys.maxsize) -> int:
        """ Return the index of the first instance of the given item in the list.
        Raises a ValueError if the item is not found. Note that this method looks
        for the base File object which is wrapped by the DataFuture or DynamicFile.

        Args:
            - item (File/DataFuture/DynamicFile) : File, DataFuture, or DynamicFile to find
            - start (int) : Index to start searching from
            - stop (int) : Index to stop searching at
        """
        for i in range(start, min(stop, len(self))):
            if not self[i].empty:
                if self[i].uuid == item.uuid:
                    return i
        raise ValueError("Item not found")

    def insert(self, __index: int, __object: Union[File, DataFuture, DynamicFile]):
        """ Insert a file into the list at the given index and update the files_done dict

        Args:
            - __index (int) : Index to insert the file at
            - __object (File/DataFuture) : File or DataFuture to insert
        """
        if __index > self._last_idx:
            raise ValueError("Cannot insert at index greater than the last index")
        if not isinstance(__object, self.DynamicFile):
            if self.parent is not None and isinstance(__object, File):
                __object = DataFuture(self.parent, __object, tid=self._output_task_id,
                                      dfk=self.dfk)
            __object = self.wrap(__object)
        self.files_done[__object.filename] = __object.done
        self._files.insert(__index, __object)
        self.stage_file(__index)
        self._last_idx += 1
        self._call_callbacks()

    def remove(self, __value):
        """ Remove a file from the list and update the files_done dict

        Args:
            - __value (File/DataFuture) : File or DataFuture to remove
        """
        if __value.filename in self.files_done:
            del self.files_done[__value.filename]
        idx = self.index(__value)
        del self._files[idx]
        self._last_idx -= 1
        self._call_callbacks()

    def pop(self, __index: int = -1) -> DataFuture:
        """ Pop a file from the list and update the files_done dict

        Args:
            - __index (int) : Index to pop the file at

        Returns:
            - File/DataFuture : File or DataFuture that was popped
        """
        if __index == -1:
            value = self._files.pop(self._last_idx)
        elif __index <= self._last_idx:
            value = self._files.pop(__index)
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
        self._files.clear()
        # detach all the callbacks so that sub-lists can still be used
        self._sub_callbacks.clear()

    def resize(self, idx: int) -> None:
        """ Resize the list to the given length

        Args:
            - idx (int) : Length to resize the list to
        """
        if idx == len(self):
            return
        if idx < len(self):
            for i in range(len(self) - 1, idx - 1, -1):
                del self[i]
        else:
            self._expand(idx)

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
            self._files.append(self.wrap(None))

    @typeguard.typechecked
    def __setitem__(self, key: int, value: Union[File, DataFuture, 'DynamicFileList.DynamicFile']):
        if self[key].filename in self.files_done:
            del self.files_done[self[key].filename]
        if self.__getitem__(key).empty:
            if self.parent is not None and isinstance(value, File):
                value = DataFuture(self.parent, value, tid=self._output_task_id,
                                   dfk=self.dfk)
            self._files[key].set_file(value)
            self.files_done[self._files[key].filename] = self._files[key].done
            self._last_idx = max(self._last_idx, key)
            self._call_callbacks()
            self.stage_file(key)
        elif value.uuid == self._files[key].uuid:
            if isinstance(value, DynamicFileList.DynamicFile):
                self._files[key].set_file(value.file_obj)
            else:
                self._files[key].set_file(value)
        else:
            raise ValueError("Cannot set a value that is not empty")
            # if not isinstance(value, self.DynamicFile):
            #    if isinstance(value, File):
            #        value = DataFuture(self.parent, value, tid=self._output_task_id)
            #    value = self.wrap(value)
            # super().__setitem__(key, value)
            # self.files_done[value.filename] = value.done

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
            ret = DynamicFileSubList(key, self._files[key], self)
            self._sub_callbacks.append(ret.callback)
            return ret
        else:
            if key >= len(self):
                self._expand(key)
            return self._files[key]

    def get_update(self, key: slice):
        """Get an updated slice for the sublist.

        Args:
            - key (slice) : Slice to update

        Returns:
            - List[DynamicFile] : Updated slice
        """
        return self._files[key]

    def __contains__(self, item):
        return item in self._files

    def __bool__(self):
        return bool(self._files)

    def __iter__(self):
        yield from self._files

    def __hash__(self):
        return hash(self._files)

    def __delitem__(self, key):
        raise Exception("Cannot delete from a DynamicFileList")
        # del self.files_done[self[key].filename]
        # super().__delitem__(key)
        # self._call_callbacks()

    def __repr__(self):
        type_ = type(self)
        module = type_.__module__
        qualname = type_.__qualname__
        if self.done():
            done = "done"
        else:
            done = "not done"
        return f"<{module}.{qualname} object at {hex(id(self))} containing {len(self)} objects {done}>"

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['dfk']
        del state['_condition']
        if self.parent is not None:
            #state['parent'] = self.parent.__getstate__()
            if state['parent'].task_record is not None:
                state['parent'].task_record = trcopy(self.parent.task_record)
        if self.task_record is not None:
            state['task_record'] = trcopy(self.task_record)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __reduce__(self):
        return self.__reduce_ex__(None)

    def __reduce_ex__(self, proto):
        if self.task_record is not None:
            tr = trcopy(self.task_record)
            if 'dfk' in tr:
                del tr['dfk']
        else:
            tr = None
        if self.parent is not None:
            par = deepcopy(self.parent)
            if 'dfk' in par.task_record:
                del par.task_record['dfk']
        else:
            par = None
        data = {}
        data["files_done"] = self.files_done
        data["_last_idx"] = self._last_idx
        data["executor"] = self.executor
        data["parent"] = par
        data["_sub_callbacks"] = self._sub_callbacks
        data["_in_callback"] = self._in_callback
        data["_staging_inhibited"] = self._staging_inhibited
        data["_output_task_id"] = self._output_task_id
        data["task_record"] = tr
        data["_is_done"] = self._is_done
        return (self.__class__, (self._files,), data)

        '''
        return (self.__class__, (self._files, ), {"files_done": self.files_done,
                                                  "_last_idx": self._last_idx,
                                                  "executor": self.executor,
                                                  "parent": par,
                                                  "_sub_callbacks": self._sub_callbacks,
                                                  "_in_callback": self._in_callback,
                                                  "_staging_inhibited": self._staging_inhibited,
                                                  "_output_task_id": self._output_task_id,
                                                  "task_record": tr,
                                                  "_is_done": self._is_done})'''

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
