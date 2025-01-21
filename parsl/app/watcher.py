"""This module provides the bash_watch_app function. This function wraps a bash_app and captures
any files created by the bash_app. It can be used with or without the file provenance feature.

"""
from typing import List, Union, Callable

from parsl.app.app import python_app
from parsl.data_provider.dynamic_files import DynamicFileList


@python_app
def bash_watch(func: Callable,
               outputs: DynamicFileList,
               paths: Union[List[str], str] = ".",
               *args,
               **kwargs) -> int:
    """This function wraps a bash_app and captures any files created by the bash_app. This is done
    by using the watchdog library to watch the specified paths for file creation events. It is most
    useful when using the file provenance framework.

    Parameters
    ----------
    func : callable
        The bash_app to run.
    outputs : DynamicFileList
        The list, partial, or complete, of files to watch for. Can also be empty. When the function
        completes it will hold the list of files that were created by the bash_app, regardless of
        what was specified here.
    paths: string or list
        The path or paths to watch, recursively, for file creation events. Default is ".".
    *args : list
        Arbitrary arguments to pass to the bash_app.
    **kwargs : dict
        Arbitrary keyword arguments to pass to the bash_app.

    Returns
    -------
    The return code of the wrapped `bash_app`
    """
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer

    from parsl.data_provider.dynamic_files import DynamicFileList
    from parsl.data_provider.files import File

    class BashWatcher(FileSystemEventHandler):
        """A class to watch for file creation events"""
        def __init__(self):
            super().__init__()
            self.added_files = []

        def on_created(self, event) -> None:
            if event.is_directory:
                return
            self.added_files.append(event.src_path)

        def on_deleted(self, event) -> None:
            """ If a file is deleted, and we recoded its creation, remove it from our list as well"""
            if event.is_directory:
                return
            if event.src_path in self.added_files:
                self.added_files.remove(event.src_path)

        def on_moved(self, event) -> None:
            """ If a file is moved, and we recorded its creation, change its list entry"""
            if event.is_directory:
                return
            if event.src_path in self.added_files:
                self.added_files.remove(event.src_path)
                self.added_files.append(event.dest_path)
    # do some type checking (can't use typeguard here)
    if not isinstance(outputs, DynamicFileList):
        raise ValueError("outputs must be a DynamicFileList")
    if isinstance(paths, str):
        paths = [paths]
    # create an internal list to avoid issues with parentage
    if len(outputs) == 0:
        i_outs = DynamicFileList()
    else:
        i_outs = DynamicFileList([DynamicFileList.DynamicFile(o.filename) for o in outputs])
    watcher = BashWatcher()
    observer = Observer()
    # tell the observer what paths to monitor
    for pth in paths:
        observer.schedule(watcher, pth, recursive=True)
    observer.start()
    # run the bash app and wait for it to complete
    _f = func(outputs=i_outs, *args, **kwargs)
    res = _f.result()
    observer.stop()
    observer.join()
    outputs.clear()
    # now put any new files that were detected into the outputs
    for added in watcher.added_files:
        outputs.append(File(added))
    return res
