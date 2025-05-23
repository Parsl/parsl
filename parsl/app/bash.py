import logging
from functools import partial
from inspect import Parameter, signature
from pathlib import Path
import glob
import os
import subprocess

from parsl.app.app import AppBase
from parsl.app.errors import wrap_error
from parsl.data_provider.files import File
from parsl.dataflow.dflow import DataFlowKernelLoader
from parsl.data_provider.dynamic_files import DynamicFileList

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

logger = logging.getLogger(__name__)


def remote_side_bash_executor(func, *args, **kwargs):
    """Executes the supplied function with *args and **kwargs to get a
    command-line to run, and then run that command-line using bash.
    """
    import os
    import subprocess

    import parsl.app.errors as pe
    from parsl.utils import get_std_fname_mode

    if hasattr(func, '__name__'):
        func_name = func.__name__
    else:
        logger.warning('No name for the function. Potentially a result of parsl#2233')
        func_name = 'bash_app'

    executable = None

    # Try to run the func to compose the commandline
    try:
        # Execute the func to get the commandline
        executable = func(*args, **kwargs)

        if not isinstance(executable, str):
            raise ValueError(f"Expected a str for bash_app commandline, got {type(executable)}")

    except AttributeError as e:
        if executable is not None:
            raise pe.AppBadFormatting("App formatting failed for app '{}' with AttributeError: {}".format(func_name, e))
        else:
            raise pe.BashAppNoReturn("Bash app '{}' did not return a value, or returned None - with this exception: {}".format(func_name, e))

    except IndexError as e:
        raise pe.AppBadFormatting("App formatting failed for app '{}' with IndexError: {}".format(func_name, e))
    except Exception as e:
        raise e

    # Updating stdout, stderr if values passed at call time.

    def open_std_fd(fdname):
        # fdname is 'stdout' or 'stderr'
        stdfspec = kwargs.get(fdname)  # spec is str name or tuple (name, mode)
        if stdfspec is None:
            return None

        if isinstance(stdfspec, File):
            # a File is an os.PathLike and so we can use it directly for
            # the subsequent file operations
            fname = stdfspec
            mode = "w"
        else:
            fname, mode = get_std_fname_mode(fdname, stdfspec)

        try:
            if os.path.dirname(fname):
                os.makedirs(os.path.dirname(fname), exist_ok=True)
            fd = open(fname, mode)
        except Exception as e:
            raise pe.BadStdStreamFile(str(fname)) from e
        return fd

    std_out = open_std_fd('stdout')
    std_err = open_std_fd('stderr')
    timeout = kwargs.get('walltime')

    if std_err is not None:
        print('--> executable follows <--\n{}\n--> end executable <--'.format(executable), file=std_err, flush=True)

    returncode = None
    try:
        proc = subprocess.Popen(executable, stdout=std_out, stderr=std_err, shell=True, executable='/bin/bash', close_fds=False)
        proc.wait(timeout=timeout)
        returncode = proc.returncode

    except subprocess.TimeoutExpired:
        raise pe.AppTimeout(f"App {func_name} exceeded walltime: {timeout} seconds")

    except Exception as e:
        raise pe.AppException(f"App {func_name} caught exception with returncode: {returncode}", e)

    if returncode != 0:
        raise pe.BashExitFailure(func_name, proc.returncode)

    # TODO : Add support for globs here

    missing = []
    for outputfile in kwargs.get('outputs', []):
        fpath = outputfile.filepath

        if not os.path.exists(fpath):
            missing.extend([outputfile])

    if missing:
        raise pe.MissingOutputs(f"Missing outputs from app {func_name}", missing)

    return returncode


class BashApp(AppBase):

    def __init__(self, func, data_flow_kernel=None, cache=False, executors='all', ignore_for_cache=None):
        super().__init__(func, data_flow_kernel=data_flow_kernel, executors=executors, cache=cache, ignore_for_cache=ignore_for_cache)
        self.kwargs = {}

        # We duplicate the extraction of parameter defaults
        # to self.kwargs to ensure availability at point of
        # command string format. Refer: #349
        sig = signature(func)

        for s in sig.parameters:
            if sig.parameters[s].default is not Parameter.empty:
                self.kwargs[s] = sig.parameters[s].default

        # partial is used to attach the first arg the "func" to the remote_side_bash_executor
        # this is done to avoid passing a function type in the args which parsl.serializer
        # doesn't support
        remote_fn = partial(remote_side_bash_executor, self.func)
        remote_fn.__name__ = self.func.__name__
        self.wrapped_remote_function = wrap_error(remote_fn)

    def __call__(self, *args, **kwargs):
        """Handle the call to a Bash app.

        Args:
             - Arbitrary

        Kwargs:
             - Arbitrary

        Returns:
                   App_fut

        """
        invocation_kwargs = {}
        invocation_kwargs.update(self.kwargs)
        invocation_kwargs.update(kwargs)

        if self.data_flow_kernel is None:
            dfk = DataFlowKernelLoader.dfk()
        else:
            dfk = self.data_flow_kernel

        app_fut = dfk.submit(self.wrapped_remote_function,
                             app_args=args,
                             executors=self.executors,
                             cache=self.cache,
                             ignore_for_cache=self.ignore_for_cache,
                             app_kwargs=invocation_kwargs)

        return app_fut


class BashWatcher(FileSystemEventHandler):
    """A class to watch for file creation events on local file systems"""

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


class BashTracker:
    """A class to watch for file creations on networked file systems. This is done by snapshotting the monitored
    directories before and after and recording the difference"""

    def __init__(self):
        self.paths = []
        self.files = []
        self.added_files = []

    def add_path(self, path: str) -> None:
        """Add a directory to be monitored

        Parameters
        ----------
        path: str
            The path to be monitored
        """
        self.paths.append(path)
        self.files.extend(glob.glob(os.path.join(path, '**', '*'), recursive=True))

    def update(self) -> None:
        """Generate the list of new files based on the initial snapshot and the current state of the paths to be
         monitored."""
        temp = []
        for pth in self.paths:
            temp.extend(glob.glob(os.path.join(pth, '**', '*'), recursive=True))
        updated = set(temp)
        self.added_files = list(updated - set(self.files))


class BashObserver(Observer):
    """Class to monitor directories for new files. Sets up two different watchers: 'inotify' type watcher for local
    file systems and one which snapshots the directories to be monitored for network file systems"""

    def __init__(self):
        super().__init__()
        self.watchers = [BashWatcher(), BashTracker()]
        self.f_sys = {}
        mounts = subprocess.check_output(["df", "-T"], text=True).split("\n")
        for m in mounts:
            if m.startswith('df:') or m.startswith('Filesystem'):
                continue
            parts = m.split()

            if not parts:
                continue
            if parts[6] == '/':
                self.n_root = parts[1] not in ['ext4', 'ext2', 'ext3', 'xfs', 'tmpfs', 'f2fs', 'reiserfs', 'zfs',
                                               'jfs', 'btrfs', 'reiser4']
                continue
            self.f_sys[Path(parts[6])] = parts[1] not in ['ext4', 'ext2', 'ext3', 'xfs', 'tmpfs', 'f2fs', 'reiserfs', 'zfs',
                                                          'jfs', 'btrfs', 'reiser4']

    def schedule(self, path):
        """Schedule a directory to be monitored by one of the internal watchers.

        Parameters
        ----------
        path: str
            The directory to be monitored
        """
        try:
            Path(path).mkdir(parents=True, exist_ok=True)
            r_path = Path(path).resolve(True)
        except FileExistsError:
            print(f"Observer can only monitor directories. {path} is a file.")
            raise
        for p, n in self.f_sys.items():
            if r_path.is_relative_to(p):
                if n:
                    self.watchers[1].add_path(str(r_path))
                else:
                    super().schedule(self.watchers[0], str(r_path), recursive=True)
                return
        if self.n_root:
            self.watchers[1].add_path(str(r_path))
        else:
            super().schedule(self.watchers[0], str(r_path), recursive=True)

    def stop(self):
        """Signal the watchers to stop watching the directories."""
        self.watchers[1].update()
        super().stop()

    def get_new_files(self) -> list[str]:
        """ Get a list of all files created in the watched paths

        Returns
        -------
        list of strings, one for each new file
        """
        return self.watchers[0].added_files + self.watchers[1].added_files


class BashWatch(AppBase):
    def __init__(self, func, data_flow_kernel=None, cache=False, executors='all', ignore_for_cache=None):
        super().__init__(func, data_flow_kernel=data_flow_kernel, executors=executors, cache=cache,
                         ignore_for_cache=ignore_for_cache)
        self.kwargs = {}
        # We duplicate the extraction of parameter defaults
        # to self.kwargs to ensure availability at point of
        # command string format. Refer: #349
        sig = signature(func)

        for s in sig.parameters:
            if sig.parameters[s].default is not Parameter.empty:
                self.kwargs[s] = sig.parameters[s].default

        # partial is used to attach the first arg the "func" to the remote_side_bash_executor
        # this is done to avoid passing a function type in the args which parsl.serializer
        # doesn't support
        remote_fn = partial(remote_side_bash_executor, self.func)
        remote_fn.__name__ = self.func.__name__
        self.wrapped_remote_function = wrap_error(remote_fn)

        self.observer = BashObserver()
        self.outputs = None

    def gather(self) -> None:
        """ Gather any files that were detected. """
        self.observer.stop()
        self.observer.join()
        for added in self.observer.get_new_files():
            self.outputs.append(File(added))

    def __call__(self, outputs, *args, paths=".", **kwargs):
        """Handle the call to a Bash app.

        Args:
             - Arbitrary

        Kwargs:
             - Arbitrary

        Returns:
                   App_fut

        """
        invocation_kwargs = {}
        invocation_kwargs.update(self.kwargs)
        invocation_kwargs.update(kwargs)
        if not isinstance(outputs, DynamicFileList):
            raise ValueError("outputs must be a DynamicFileList.")
        outputs.clear()
        invocation_kwargs['outputs'] = outputs
        self.outputs = outputs

        if isinstance(paths, str):
            paths = [paths]
        elif isinstance(paths, list):
            pass
        else:
            raise ValueError("paths must be a string or list of strings.")

        if self.data_flow_kernel is None:
            dfk = DataFlowKernelLoader.dfk()
        else:
            dfk = self.data_flow_kernel

        for pth in paths:
            self.observer.schedule(pth)
        self.observer.start()
        self.outputs.add_watcher_callback(self.gather)
        app_fut = dfk.submit(self.wrapped_remote_function,
                             app_args=args,
                             executors=self.executors,
                             cache=self.cache,
                             ignore_for_cache=self.ignore_for_cache,
                             app_kwargs=invocation_kwargs)
        return app_fut
