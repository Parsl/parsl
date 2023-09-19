import inspect
import logging
import os
import shlex
import subprocess
import threading
import time
from contextlib import contextmanager
from types import TracebackType
from typing import Any, Callable, List, Tuple, Union, Generator, IO, AnyStr, Dict, Optional

import typeguard
from typing_extensions import Type

import parsl
from parsl.version import VERSION


try:
    import setproctitle as setproctitle_module
except ImportError:
    _setproctitle_enabled = False
else:
    _setproctitle_enabled = True


logger = logging.getLogger(__name__)


@typeguard.typechecked
def get_version() -> str:
    version = parsl.__version__  # type: str
    work_tree = os.path.dirname(os.path.dirname(__file__))
    git_dir = os.path.join(work_tree, '.git')
    if os.path.exists(git_dir):
        env = {'GIT_WORK_TREE': work_tree, 'GIT_DIR': git_dir}
        try:
            cmd = shlex.split('git rev-parse --short HEAD')
            head = subprocess.check_output(cmd, env=env).strip().decode('utf-8')
            diff = subprocess.check_output(shlex.split('git diff HEAD'), env=env)
            status = 'dirty' if diff else 'clean'
            version = f'{VERSION}-{head}-{status}'
        except Exception:
            pass

    return version


@typeguard.typechecked
def get_all_checkpoints(rundir: str = "runinfo") -> List[str]:
    """Finds the checkpoints from all runs in the rundir.

    Kwargs:
       - rundir(str) : Path to the runinfo directory

    Returns:
       - a list suitable for the checkpoint_files parameter of `Config`

    """

    if not os.path.isdir(rundir):
        return []

    dirs = sorted(os.listdir(rundir))

    checkpoints = []

    for runid in dirs:

        checkpoint = os.path.abspath(f'{rundir}/{runid}/checkpoint')

        if os.path.isdir(checkpoint):
            checkpoints.append(checkpoint)

    return checkpoints


@typeguard.typechecked
def get_last_checkpoint(rundir: str = "runinfo") -> List[str]:
    """Finds the checkpoint from the last run, if one exists.

    Note that checkpoints are incremental, and this helper will not find
    previous checkpoints from earlier than the most recent run. If you
    want that behaviour, see `get_all_checkpoints`.

    Kwargs:
       - rundir(str) : Path to the runinfo directory

    Returns:
     - a list suitable for the checkpoint_files parameter of `Config`,
       with 0 or 1 elements

    """
    if not os.path.isdir(rundir):
        return []

    dirs = sorted(os.listdir(rundir))

    if len(dirs) == 0:
        return []

    last_runid = dirs[-1]
    last_checkpoint = os.path.abspath(f'{rundir}/{last_runid}/checkpoint')

    if not os.path.isdir(last_checkpoint):
        return []

    return [last_checkpoint]


@typeguard.typechecked
def get_std_fname_mode(
    fdname: str,
    stdfspec: Union[os.PathLike, str, Tuple[str, str], Tuple[os.PathLike, str]]
) -> Tuple[str, str]:
    import parsl.app.errors as pe
    if isinstance(stdfspec, (str, os.PathLike)):
        fname = stdfspec
        mode = 'a+'
    elif isinstance(stdfspec, tuple):
        if len(stdfspec) != 2:
            msg = (f"std descriptor {fdname} has incorrect tuple length "
                   f"{len(stdfspec)}")
            raise pe.BadStdStreamFile(msg, TypeError('Bad Tuple Length'))
        fname, mode = stdfspec
    return str(fname), mode


@contextmanager
def wait_for_file(path: str, seconds: int = 10) -> Generator[None, None, None]:
    for i in range(0, int(seconds * 100)):
        time.sleep(seconds / 100.)
        if os.path.exists(path):
            break
    yield


@contextmanager
def time_limited_open(path: str, mode: str, seconds: int = 1) -> Generator[IO[AnyStr], None, None]:
    with wait_for_file(path, seconds):
        logger.debug("wait_for_file yielded")
    f = open(path, mode)
    yield f
    f.close()


def wtime_to_minutes(time_string: str) -> int:
    ''' wtime_to_minutes

    Convert standard wallclock time string to minutes.

    Args:
        - Time_string in HH:MM:SS format

    Returns:
        (int) minutes

    '''
    hours, mins, seconds = time_string.split(':')
    total_mins = int(hours) * 60 + int(mins)
    if total_mins < 1:
        msg = (f"Time string '{time_string}' parsed to {total_mins} minutes, "
               f"less than 1")
        logger.warning(msg)
    return total_mins


class RepresentationMixin:
    """A mixin class for adding a __repr__ method.

    The __repr__ method will return a string equivalent to the code used to instantiate
    the child class, with any defaults included explicitly. The __max_width__ class variable
    controls the maximum width of the representation string. If this width is exceeded,
    the representation string will be split up, with one argument or keyword argument per line.

    Any arguments or keyword arguments in the constructor must be defined as attributes, or
    an AttributeError will be raised.

    Examples
    --------
    >>> from parsl.utils import RepresentationMixin
    >>> class Foo(RepresentationMixin):
            def __init__(self, first, second, third='three', fourth='fourth'):
                self.first = first
                self.second = second
                self.third = third
                self.fourth = fourth
    >>> bar = Foo(1, 'two', fourth='baz')
    >>> bar
    Foo(1, 'two', third='three', fourth='baz')
    """
    __max_width__ = 80

    def __repr__(self) -> str:
        init = self.__init__  # type: ignore[misc]

        # This test looks for a single layer of wrapping performed by
        # functools.update_wrapper, commonly used in decorators. This will
        # allow RepresentationMixin to see through a single such decorator
        # applied to the __init__ method of a class, and find the underlying
        # arguments. It will not see through multiple layers of such
        # decorators, or cope with other decorators which do not use
        # functools.update_wrapper.

        if hasattr(init, '__wrapped__'):
            init = init.__wrapped__

        argspec = inspect.getfullargspec(init)
        if len(argspec.args) > 1 and argspec.defaults is not None:
            defaults = dict(zip(reversed(argspec.args), reversed(argspec.defaults)))
        else:
            defaults = {}

        for arg in argspec.args[1:]:
            if not hasattr(self, arg):
                template = (f'class {self.__class__.__name__} uses {arg} in the'
                            f' constructor, but does not define it as an '
                            f'attribute')
                raise AttributeError(template)

        if len(defaults) != 0:
            args = [getattr(self, a) for a in argspec.args[1:-len(defaults)]]
        else:
            args = [getattr(self, a) for a in argspec.args[1:]]
        kwargs = {key: getattr(self, key) for key in defaults}

        def assemble_multiline(args: List[str], kwargs: Dict[str, object]) -> str:
            def indent(text: str) -> str:
                lines = text.splitlines()
                if len(lines) <= 1:
                    return text
                return "\n".join("    " + line for line in lines).strip()
            args = [f"\n    {indent(repr(a))}," for a in args]
            kwargsl = [f"\n    {k}={indent(repr(v))}" for k, v in
                       sorted(kwargs.items())]

            info = "".join(args) + ", ".join(kwargsl)
            return self.__class__.__name__ + f"({info}\n)"

        def assemble_line(args: List[str], kwargs: Dict[str, object]) -> str:
            kwargsl = [f'{k}={repr(v)}' for k, v in sorted(kwargs.items())]

            info = ", ".join([repr(a) for a in args] + kwargsl)
            return self.__class__.__name__ + f"({info})"

        if len(assemble_line(args, kwargs)) <= self.__class__.__max_width__:
            return assemble_line(args, kwargs)
        else:
            return assemble_multiline(args, kwargs)


class AtomicIDCounter:
    """A class to allocate counter-style IDs, in a thread-safe way.
    """

    def __init__(self) -> None:
        self.count = 0
        self.lock = threading.Lock()

    def get_id(self) -> int:
        with self.lock:
            new_id = self.count
            self.count += 1
            return new_id


def setproctitle(title: str) -> None:
    if _setproctitle_enabled:
        setproctitle_module.setproctitle(title)
    else:
        logger.warn(f"setproctitle not enabled for process {title}")


class Timer:
    """This class will make a callback periodically, with a period
    specified by the interval parameter.

    This is based on the following logic :

    .. code-block:: none


        BEGIN (INTERVAL, THRESHOLD, callback) :
            start = current_time()

            while (current_time()-start < INTERVAL) :
                 wait()
                 break

            callback()

    """

    def __init__(self, callback: Callable, *args: Any, interval: int = 5, name: Optional[str] = None) -> None:
        """Initialize the Timer object.
        We start the timer thread here

        KWargs:
             - interval (int) : number of seconds between callback events
             - name (str) : a base name to use when naming the started thread
        """

        self.interval = interval
        self.cb_args = args
        self.callback = callback
        self._wake_up_time = time.time() + 1

        self._kill_event = threading.Event()
        if name is None:
            name = "Timer-Thread-{}".format(id(self))
        else:
            name = "{}-Timer-Thread-{}".format(name, id(self))
        self._thread = threading.Thread(target=self._wake_up_timer, args=(self._kill_event,), name=name)
        self._thread.daemon = True
        self._thread.start()

    def _wake_up_timer(self, kill_event: threading.Event) -> None:
        """Internal. This is the function that the thread will execute.
        waits on an event so that the thread can make a quick exit when close() is called

        Args:
            - kill_event (threading.Event) : Event to wait on
        """

        # Sleep till time to wake up
        while True:
            prev = self._wake_up_time

            # Waiting for the event returns True only when the event
            # is set, usually by the parent thread
            time_to_die = kill_event.wait(float(max(prev - time.time(), 0)))

            if time_to_die:
                return

            if prev == self._wake_up_time:
                self.make_callback()
            else:
                print("Sleeping a bit more")

    def make_callback(self) -> None:
        """Makes the callback and resets the timer.
        """
        self._wake_up_time = time.time() + self.interval

        try:
            self.callback(*self.cb_args)
        except Exception:
            logger.error("Callback threw an exception - logging and proceeding anyway", exc_info=True)

    def close(self) -> None:
        """Merge the threads and terminate.
        """
        self._kill_event.set()
        self._thread.join()


class AutoCancelTimer(threading.Timer):
    """
    Extend threading.Timer for use as a context manager

    Example:

        with AutoCancelTimer(delay, your_callback):
            some_func()

    If `some_func()` returns before the delay is up, the timer will
    be cancelled.
    """
    def __enter__(self) -> "AutoCancelTimer":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType]
    ) -> None:
        self.cancel()
