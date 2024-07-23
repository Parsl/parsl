"""Defines the FluxExecutor class."""

import collections
import concurrent.futures as cf
import functools
import itertools
import os
import queue
import shutil
import sys
import threading
import uuid
import weakref
from collections.abc import Callable, Mapping
from socket import gethostname
from typing import Any, Dict, Optional

import zmq

from parsl.app.errors import AppException
from parsl.executors.base import ParslExecutor
from parsl.executors.errors import ScalingFailed
from parsl.executors.flux.execute_parsl_task import __file__ as _WORKER_PATH
from parsl.executors.flux.flux_instance_manager import __file__ as _MANAGER_PATH
from parsl.providers import LocalProvider
from parsl.providers.base import ExecutionProvider
from parsl.serialize import deserialize, pack_res_spec_apply_message
from parsl.serialize.errors import SerializationError
from parsl.utils import RepresentationMixin

_WORKER_PATH = os.path.realpath(_WORKER_PATH)
_MANAGER_PATH = os.path.realpath(_MANAGER_PATH)


_FluxJobInfo = collections.namedtuple(
    "_FluxJobInfo", ("future", "task_id", "infile", "outfile", "resource_spec")
)


class FluxFutureWrapper(cf.Future):
    """Wrapper class around a ``flux.job.FluxExecutorFuture.``

    Forwards methods onto the underlying FluxExecutorFuture.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._flux_future = None
        self._cancellation_lock = threading.Lock()

    def cancel(self):
        # potential race condition: executor sets _flux_future after check
        # that _flux_future is None. Protect the check and cancel() call with a lock.
        with self._cancellation_lock:
            if self._flux_future is None:
                return super().cancel()
        if self._flux_future.cancelled():
            # due to a bug, flux futures can only have ``cancel`` called once
            return True
        if self._flux_future.cancel():  # cancel underlying future and then self
            if not super().cancel():
                raise RuntimeError("Unexpected state")
            self.set_running_or_notify_cancel()  # also should be called only once
            return True
        return False

    cancel.__doc__ = cf.Future.cancel.__doc__

    def running(self):
        if self._flux_future is None:
            return False  # no race condition since ``running()`` just advisory
        return self._flux_future.running()

    running.__doc__ = cf.Future.running.__doc__


def _complete_future(
    expected_file: str, future_wrapper: FluxFutureWrapper, flux_future: Any
):
    """Callback triggered when a FluxExecutorFuture completes.

    When the FluxExecutorFuture completes, check for the Parsl task's
    output file, and assign the result to the FluxWrapperFuture future.

    Parameters
    ----------
    expected_file : str
        The path to the Parsl task's output file, storing the result of the task.
    future_wrapper : FluxFutureWrapper
        The user-facing future.
    flux_future : FluxExecutorFuture
        The future wrapped by ``future_wrapper``. Also accessible via
        ``future_wrapper``, but the flux_future must be accepted as an argument
        due to how ``concurrent.futures.add_done_callback`` works.
    """
    if flux_future.cancelled():  # if underlying future was cancelled, return
        return  # no need to set a result on the wrapper future
    try:
        returncode = flux_future.result()
    except Exception as unknown_err:
        future_wrapper.set_exception(unknown_err)
        return
    if returncode == 0:
        try:  # look for the output file
            with open(expected_file, "rb") as file_handle:
                task_result = deserialize(file_handle.read())
        except FileNotFoundError:
            future_wrapper.set_exception(
                FileNotFoundError(
                    f"No result found for Parsl task, expected {expected_file}"
                )
            )
        except Exception as unknown_err:
            future_wrapper.set_exception(unknown_err)
        else:  # task package deserialized successfully
            if task_result.exception is not None:
                future_wrapper.set_exception(task_result.exception)
            else:
                future_wrapper.set_result(task_result.returnval)
    else:  # the job exited abnormally
        future_wrapper.set_exception(
            AppException(f"Parsl task exited abnormally: returned {returncode}")
        )


class FluxExecutor(ParslExecutor, RepresentationMixin):
    """Executor that uses Flux to schedule and run jobs.

    Every callable submitted to the executor is wrapped into a Flux job.

    This executor requires that there be a Flux installation available
    locally, and that it can be located either in PATH or through the
    ``flux_path`` argument.

    Flux jobs are fairly heavyweight. As of Flux v0.25, a single Flux
    instance is (on many systems) capped at 50 jobs per second. As such,
    this executor is not a good fit for use-cases consisting of large numbers
    of small, fast jobs.

    However, Flux is great at handling jobs with large resource requirements,
    and collections of jobs with varying resource requirements.

    Note that due to vendor-specific extensions, on certain Cray machines like
    ALCF's Theta or LANL's Trinitite/Trinity, Flux cannot run applications
    that use the default MPI library. Generally the only workaround is to
    recompile with another MPI library like OpenMPI.

    This executor acts as a sort of wrapper around a ``flux.job.FluxExecutor``,
    which can be confusing since both wrapped and wrapper classes share the same name.
    Whenever possible, the underlying executor is referred by its fully qualified name,
    ``flux.job.FluxExecutor``.

    Parameters
    ----------
    working_dir: str
        Directory in which the executor should place its files, possibly overwriting
        existing files. If ``None``, generate a unique directory.
    label: str
        Label for this executor instance.
    flux_handle_args: collections.abc.Sequence
        Positional arguments to ``flux.Flux()`` instance, if any.
        The first positional argument, ``url``, is provided by this executor.
    flux_executor_kwargs: collections.abc.Mapping
        Keyword arguments to pass to the underlying ``flux.job.FluxExecutor()``
        instance, if any. Note that the ``handle_args`` keyword
        argument is provided by this executor,
        in order to supply the URL of a remote Flux instance.
    flux_path: str
        Path to flux installation to use, or None to search PATH for flux.
    launch_cmd: str
        The command to use when launching the executor's backend. The default
        command is available as the ``DEFAULT_LAUNCH_COMMAND`` attribute. The
        default command starts a new Flux instance, which may not be desirable
        if a Flux instance will already be provisioned (this is not likely).
    """

    DEFAULT_LAUNCH_CMD = "{flux} start {python} {manager} {protocol} {hostname} {port}"

    def __init__(
        self,
        provider: Optional[ExecutionProvider] = None,
        working_dir: Optional[str] = None,
        label: str = "FluxExecutor",
        flux_executor_kwargs: Mapping = {},
        flux_path: Optional[str] = None,
        launch_cmd: Optional[str] = None,
    ):
        super().__init__()
        if provider is None:
            provider = LocalProvider()
        self.provider = provider
        self.label = label
        if working_dir is None:
            working_dir = self.label + "_" + str(uuid.uuid4())
        self.working_dir = os.path.abspath(working_dir)
        # check that flux_path is an executable, or look for flux in PATH
        if flux_path is None:
            flux_path = shutil.which("flux")
            if flux_path is None:
                raise EnvironmentError("Cannot find Flux installation in PATH")
        self.flux_path = os.path.abspath(flux_path)
        self._task_id_counter = itertools.count()
        # Assumes a launch command cannot be None or empty
        self.launch_cmd = launch_cmd or self.DEFAULT_LAUNCH_CMD
        self._submission_queue: queue.Queue = queue.Queue()
        self._stop_event = threading.Event()
        # lock to protect self._task_id_counter and also submission/shutdown race
        self._submission_lock = threading.Lock()
        self.flux_executor_kwargs = flux_executor_kwargs
        self._submission_thread = threading.Thread(
            target=_submit_wrapper,
            args=(
                self._submission_queue,
                self._stop_event,
                self.working_dir,
                self.flux_executor_kwargs,
                self.provider,
                self,
                self.flux_path,
                self.launch_cmd,
            ),
            daemon=True,
        )
        # add a ``weakref.finalize()`` function for joining the executor thread
        weakref.finalize(
            self,
            lambda x, y: x.set() or y.join(),
            self._stop_event,
            self._submission_thread,
        )

    def start(self):
        """Called when DFK starts the executor when the config is loaded."""
        os.makedirs(self.working_dir, exist_ok=True)
        self._submission_thread.start()

    def shutdown(self, wait=True):
        """Shut down the executor, causing further calls to ``submit`` to fail.

        Parameters
        ----------
        wait
            If ``True``, do not return until all submitted Futures are done.
        """
        with self._submission_lock:
            self._stop_event.set()
        if wait:
            self._submission_thread.join()

    def submit(
        self,
        func: Callable,
        resource_specification: Dict[str, Any],
        *args: Any,
        **kwargs: Any,
    ):
        """Wrap a callable in a Flux job and submit it to Flux.

        :param func: The callable to submit as a job to Flux

        :param resource_specification: A mapping defining the resources to allocate to the Flux job.

            Only the following keys are checked for:

            -  num_tasks: the number of tasks to launch (MPI ranks for an MPI job), default 1
            -  cores_per_task: cores per task, default 1
            -  gpus_per_task: gpus per task, default 1
            -  num_nodes: if > 0, evenly distribute the allocated cores/gpus
               across the given number of nodes. Does *not* give the job exclusive
               access to those nodes; this option only affects distribution.

        :param args: positional arguments for the callable

        :param kwargs: keyword arguments for the callable
        """
        # protect self._task_id_counter and shutdown/submit race
        with self._submission_lock:
            if self._stop_event.is_set():
                raise RuntimeError("`shutdown()` already called")
            task_id = str(next(self._task_id_counter))
            infile = os.path.join(self.working_dir, f"{task_id}_in{os.extsep}pkl")
            outfile = os.path.join(self.working_dir, f"{task_id}_out{os.extsep}pkl")
            try:
                fn_buf = pack_res_spec_apply_message(
                    func, args, kwargs,
                    resource_specification={},
                    buffer_threshold=1024 * 1024
                )
            except TypeError:
                raise SerializationError(func.__name__)
            with open(infile, "wb") as infile_handle:
                infile_handle.write(fn_buf)
            future = FluxFutureWrapper()
            self._submission_queue.put(
                _FluxJobInfo(future, task_id, infile, outfile, resource_specification)
            )
            return future


def _submit_wrapper(
    submission_queue: queue.Queue, stop_event: threading.Event, *args, **kwargs
):
    """Wrap the ``_submit_flux_jobs`` function in a try/except.

    If an exception is thrown, error out all submitted tasks.
    """
    with zmq.Context() as ctx:
        with ctx.socket(zmq.REP) as socket:
            try:
                _submit_flux_jobs(submission_queue, stop_event, socket, *args, **kwargs)
            except Exception as exc:
                _error_out_jobs(submission_queue, stop_event, exc)
                raise


def _error_out_jobs(
    submission_queue: queue.Queue, stop_event: threading.Event, exc: Exception
):
    """Clear out ``submission_queue``, setting errors on all futures."""
    while not stop_event.is_set() or not submission_queue.empty():
        try:
            jobinfo = submission_queue.get(timeout=0.05)
        except queue.Empty:
            pass
        else:
            jobinfo.future.set_exception(exc)


def _submit_flux_jobs(
    submission_queue: queue.Queue,
    stop_event: threading.Event,
    socket: zmq.Socket,
    working_dir: str,
    flux_executor_kwargs: Mapping,
    provider: ExecutionProvider,
    executor: FluxExecutor,
    flux_path: str,
    launch_cmd: str,
):
    """Function to be run in a separate thread by executor.

    Pull ``_FluxJobInfo`` job packages from a queue and submit them to Flux.
    """
    provider.script_dir = working_dir
    job_id = provider.submit(
        launch_cmd.format(
            port=socket.bind_to_random_port("tcp://*"),
            protocol="tcp",
            hostname=gethostname(),
            python=sys.executable,
            flux=flux_path,
            manager=_MANAGER_PATH,
        ),
        1,
    )
    if not job_id:
        raise ScalingFailed(
            executor, "Attempt to provision nodes via provider has failed",
        )
    # wait for the flux package path to be sent
    _check_provider_job(socket, provider, job_id)
    # receive path to the ``flux`` package from the ZMQ socket
    flux_pkg_path = socket.recv().decode()
    # load the package. Unfortunately the only good way to do this is to
    # modify sys.path
    if flux_pkg_path not in sys.path:
        sys.path.append(flux_pkg_path)
    import flux.job

    socket.send(b"ack")  # dummy message
    # receive the URI of the Flux instance launched by provider
    _check_provider_job(socket, provider, job_id)
    flux_instance_uri = socket.recv()
    # create a ``flux.job.FluxExecutor`` connected to remote Flux instance
    with flux.job.FluxExecutor(
        handle_args=(flux_instance_uri,), **flux_executor_kwargs
    ) as flux_executor:
        # need to ensure that no jobs submitted after stop_event set
        # exit loop when event is set and queue is drained
        while not stop_event.is_set() or not submission_queue.empty():
            try:
                jobinfo = submission_queue.get(timeout=0.05)
            except queue.Empty:
                pass
            else:
                _submit_single_job(flux_executor, working_dir, jobinfo)
    socket.send(b"shutdown")


def _check_provider_job(socket: zmq.Socket, provider: ExecutionProvider, job_id: Any):
    """Poll for messages, checking that the provider's allocation is alive."""
    while not socket.poll(1000, zmq.POLLIN):
        if provider.status([job_id])[0].terminal:
            raise RuntimeError("Provider job has terminated")


def _submit_single_job(flux_executor: Any, working_dir: str, jobinfo: _FluxJobInfo):
    """Submit a single job to Flux. Link the Flux future with a user-facing future."""
    import flux.job

    jobspec = flux.job.JobspecV1.from_command(
        command=[
            sys.executable,
            _WORKER_PATH,
            "-i",
            jobinfo.infile,
            "-o",
            jobinfo.outfile,
        ],
        num_tasks=jobinfo.resource_spec.get("num_tasks", 1),
        num_nodes=jobinfo.resource_spec.get("num_nodes"),
        cores_per_task=jobinfo.resource_spec.get("cores_per_task", 1),
        gpus_per_task=jobinfo.resource_spec.get("gpus_per_task"),
    )
    jobspec.cwd = os.getcwd()
    jobspec.environment = dict(os.environ)
    jobspec.stdout = os.path.abspath(
        os.path.join(working_dir, f"{jobinfo.task_id}_stdout{os.extsep}txt")
    )
    jobspec.stderr = os.path.abspath(
        os.path.join(working_dir, f"{jobinfo.task_id}_stderr{os.extsep}txt")
    )
    # need to shield user future from cancellation while setting its underlying future
    with jobinfo.future._cancellation_lock:
        if jobinfo.future.cancelled():
            return
        try:
            # flux_executor.submit() raises if the executor is broken for any reason
            # most importantly, it raises if the remote flux instance dies
            flux_future = flux_executor.submit(jobspec)
        except Exception as exc:
            jobinfo.future.set_exception(exc)
            return
        jobinfo.future._flux_future = flux_future
    # Trigger the user-facing wrapper future to complete when the
    # wrapped ``flux.job.FluxExecutor`` future completes.
    flux_future.add_done_callback(
        functools.partial(_complete_future, jobinfo.outfile, jobinfo.future)
    )
