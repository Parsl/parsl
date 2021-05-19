"""Defines the FluxExecutor class."""

import concurrent.futures as cf
import functools
import os
import sys
import uuid
import threading
import itertools
import shutil
import importlib.util
from socket import gethostname
from collections.abc import Sequence, Mapping, Callable
from typing import Optional, Any

import zmq

from parsl.utils import RepresentationMixin
from parsl.executors.status_handling import NoStatusHandlingExecutor
from parsl.executors.flux.execute_parsl_task import __file__ as _WORKER_PATH
from parsl.executors.flux.flux_instance_manager import __file__ as _MANAGER_PATH
from parsl.executors.errors import SerializationError, ScalingFailed
from parsl.providers import LocalProvider
from parsl.providers.provider_base import ExecutionProvider
from parsl.serialize import pack_apply_message, deserialize
from parsl.app.errors import AppException


_WORKER_PATH = os.path.realpath(_WORKER_PATH)
_MANAGER_PATH = os.path.realpath(_MANAGER_PATH)


class FluxFutureWrapper(cf.Future):
    """Wrapper class around a ``flux.job.FluxExecutorFuture.``

    Forwards methods onto the underlying FluxExecutorFuture.
    """

    def __init__(self, flux_future, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._flux_future = flux_future

    def cancel(self):
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


class FluxExecutor(NoStatusHandlingExecutor, RepresentationMixin):
    """Executor that uses Flux to schedule and run jobs.

    Every callable submitted to the executor is wrapped into a Flux job.

    This executor requires that Flux's Python bindings, the ``flux`` package,
    be available upon startup, and that there be a running Flux instance
    available.

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

    Parameters
    ----------
    managed : bool
        If this executor is managed by the DFK or externally handled.
    working_dir : str
        Directory in which the executor should place its files, possibly overwriting
        existing files. If ``None``, generate a unique directory.
    label : str
        Label for this executor instance.
    flux_handle_args: collections.abc.Sequence
        Positional arguments to ``flux.Flux()`` instance, if any.
        The first positional argument, ``url``, is provided by this executor.
    flux_handle_kwargs: collections.abc.Mapping
        Keyword arguments to pass to ``flux.Flux()`` instance, if any.
        The ``url`` argument is provided by this executor.
    flux_path: str
        Path to flux installation to use, or None to search PATH for flux.
    launch_cmd: str
        The command to use when launching the executor's backend. The default
        command is available as ``FluxExecutor.DEFAULT_LAUNCH_COMMAND``.
    """

    DEFAULT_LAUNCH_CMD = "{flux} start {python} {manager} {protocol} {hostname} {port}"

    def __init__(
        self,
        provider: Optional[ExecutionProvider] = None,
        managed: bool = True,
        working_dir: Optional[str] = None,
        label: str = "FluxExecutor",
        flux_handle_args: Sequence = (),
        flux_handle_kwargs: Mapping = {},
        flux_path: Optional[str] = None,
        launch_cmd: Optional[str] = None,
    ):
        super().__init__()
        if provider is None:
            provider = LocalProvider()
        self._provider = provider
        self.label = label
        if working_dir is None:
            working_dir = self.label + "_" + str(uuid.uuid4())
        self.working_dir = os.path.abspath(working_dir)
        self.managed = managed
        self._flux_executor = None
        self._flux_job_module = None
        self.flux_handle_args = flux_handle_args
        self.flux_handle_kwargs = flux_handle_kwargs
        # check that flux_path is an executable, or look for flux in PATH
        if flux_path is None:
            flux_path = shutil.which("flux")
            if flux_path is None:
                raise EnvironmentError("Cannot find Flux installation in PATH")
        self.flux_path = os.path.abspath(flux_path)
        self._task_id_lock = threading.Lock()  # protects self._task_id_counter
        self._task_id_counter = itertools.count()
        self._socket = zmq.Context().socket(zmq.REP)
        if launch_cmd is None:
            self.launch_cmd = self.DEFAULT_LAUNCH_CMD

    def start(self):
        """Called when DFK starts the executor when the config is loaded."""
        os.makedirs(self.working_dir, exist_ok=True)
        port = self._socket.bind_to_random_port("tcp://*")
        self.provider.script_dir = self.working_dir
        job_id = self.provider.submit(
            self.launch_cmd.format(
                port=port,
                protocol="tcp",
                hostname=gethostname(),
                python=sys.executable,
                flux=self.flux_path,
                manager=_MANAGER_PATH,
            ),
            1,
        )
        if not job_id:
            raise (
                ScalingFailed(
                    self.provider.label,
                    "Attempts to provision nodes via provider has failed",
                )
            )
        # receive path to the ``flux.job`` package from ``flux_instance_manager.py``
        flux_job_path = self._socket.recv().decode()
        spec = importlib.util.spec_from_file_location("flux.job", flux_job_path)
        self._flux_job_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self._flux_job_module)
        self._socket.send(b"ack")
        # receive the URI of the Flux instance launched by provider
        flux_instance_uri = self._socket.recv()
        handle_args = [flux_instance_uri]
        handle_args.extend(self.flux_handle_args)
        self._flux_executor = self._flux_job_module.FluxExecutor(
            handle_args=handle_args, handle_kwargs=self.flux_handle_kwargs
        )

    def shutdown(self, wait=True):
        """Shut down the executor, causing further calls to ``submit`` to fail.

        Parameters
        ----------
        wait
            If ``True``, do not return until all submitted Futures are done.
        """
        if self._flux_executor is not None:
            self._flux_executor.shutdown(wait=wait)
            self._socket.send(b"shutdown")

    def submit(self, func: Callable, resource_spec: Mapping, *args, **kwargs):
        """Wrap a callable in a Flux job and submit it to Flux.

        Parameters
        ----------
        func: callable
            The callable to submit as a job to Flux
        resource_spec: collections.abc.Mapping
            A mapping defining the resources to allocate to the Flux job.
            Only the following keys are checked for:
            * num_tasks: the number of tasks to launch (MPI ranks for an MPI job),
              default 1
            * cores_per_task: cores per task, default 1
            * gpus_per_task: gpus per task, default 1
            * num_nodes: if > 0, evenly distribute the allocated cores/gpus
              across the given number of nodes. Does *not* give the job exclusive
              access to those nodes; this option only affects distribution.
        *args:
            positional arguments for the callable
        **kwargs:
            keyword arguments for the callable
        """
        with self._task_id_lock:
            task_id = str(next(self._task_id_counter))
        infile = os.path.join(self.working_dir, f"{task_id}_in{os.extsep}pkl")
        outfile = os.path.join(self.working_dir, f"{task_id}_out{os.extsep}pkl")
        try:
            fn_buf = pack_apply_message(
                func, args, kwargs, buffer_threshold=1024 * 1024
            )
        except TypeError:
            raise SerializationError(func.__name__)
        with open(infile, "wb") as infile_handle:
            infile_handle.write(fn_buf)
        jobspec = self._flux_job_module.JobspecV1.from_command(
            command=[sys.executable, _WORKER_PATH, "-i", infile, "-o", outfile],
            num_tasks=resource_spec.get("num_tasks", 1),
            num_nodes=resource_spec.get("num_nodes"),
            cores_per_task=resource_spec.get("cores_per_task", 1),
            gpus_per_task=resource_spec.get("gpus_per_task"),
        )
        jobspec.cwd = os.getcwd()
        jobspec.environment = dict(os.environ)
        jobspec.stdout = os.path.abspath(
            os.path.join(self.working_dir, f"{task_id}_stdout{os.extsep}txt")
        )
        jobspec.stderr = os.path.abspath(
            os.path.join(self.working_dir, f"{task_id}_stderr{os.extsep}txt")
        )
        flux_future = self._flux_executor.submit(jobspec)
        # wrap the underlying executor's future. The underlying future's result
        # is the returncode of the Flux job, but we want the task's resulting Python
        # object.
        user_future = FluxFutureWrapper(flux_future)
        # Trigger the wrapper to complete when the wrapped future completes.
        flux_future.add_done_callback(
            functools.partial(_complete_future, outfile, user_future)
        )
        return user_future

    def scale_in(self, *args, **kwargs):
        pass

    def scale_out(self):
        pass

    def scaling_enabled(self):
        return False
