"""Defines the FluxExecutor class."""

import concurrent.futures as cf
import functools
import os
import sys
import uuid
from collections.abc import Sequence, Mapping, Callable
from typing import Optional, Any

try:
    import flux.job
except ImportError:
    _FLUX_AVAIL = False
else:
    _FLUX_AVAIL = True

from parsl.utils import RepresentationMixin
from parsl.executors.status_handling import NoStatusHandlingExecutor
from parsl.executors.flux.execute_parsl_task import __file__ as _worker_path
from parsl.executors.errors import SerializationError
from parsl.errors import OptionalModuleMissing
from parsl.serialize import pack_apply_message, deserialize
from parsl.providers import LocalProvider
from parsl.app.errors import AppException


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
        Working dir to be used by the executor.
    label : str
        Label for this executor instance.
    flux_handle_args: collections.abc.Sequence
        Positional arguments to ``flux.Flux()`` instance, if any.
    flux_handle_kwargs: collections.abc.Mapping
        Keyword arguments to pass to ``flux.Flux()`` instance, if any.
        For instance, the ``url`` argument to ``flux.Flux`` can be used to connect to a
        remote Flux instance at the given URL.
    """

    def __init__(
        self,
        managed: bool = True,
        working_dir: Optional[str] = "flux_workdir",
        label: str = "FluxExecutor",
        flux_handle_args: Sequence = (),
        flux_handle_kwargs: Mapping = {},
    ):
        super().__init__()
        self.label = label
        self.working_dir = os.path.abspath(working_dir) if working_dir else os.getcwd()
        self.managed = managed
        self._flux_executor = None
        self.flux_handle_args = flux_handle_args
        self.flux_handle_kwargs = flux_handle_kwargs
        self._provider = LocalProvider()

    def start(self):
        """Called when DFK starts the executor when the config is loaded.

        Raises
        ------
        OptionalModuleMissing
            If ``flux`` package cannot be imported.
        """
        if not _FLUX_AVAIL:
            raise OptionalModuleMissing(
                "flux", "Cannot initialize FluxExecutor without flux module"
            )
        self._flux_executor = flux.job.FluxExecutor(
            handle_args=self.flux_handle_args, handle_kwargs=self.flux_handle_kwargs
        )
        os.makedirs(self.working_dir, exist_ok=True)

    def shutdown(self, wait=True):
        """Shut down the executor, causing further calls to ``submit`` to fail.

        Parameters
        ----------
        wait
            If ``True``, do not return until all submitted Futures are done.
        """
        self._flux_executor.shutdown(wait=wait)

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
        tid = str(uuid.uuid4())
        infile = os.path.join(self.working_dir, tid + ".pkl")
        outfile = os.path.join(self.working_dir, tid + ".out.pkl")
        try:
            fn_buf = pack_apply_message(
                func, args, kwargs, buffer_threshold=1024 * 1024
            )
        except TypeError:
            raise SerializationError(func.__name__)
        with open(infile, "wb") as infile_handle:
            infile_handle.write(fn_buf)
        jobspec = flux.job.JobspecV1.from_command(
            command=[sys.executable, _worker_path, "-i", infile, "-o", outfile],
            num_tasks=resource_spec.get("num_tasks", 1),
            num_nodes=resource_spec.get("num_nodes"),
            cores_per_task=resource_spec.get("cores_per_task", 1),
            gpus_per_task=resource_spec.get("gpus_per_task"),
        )
        jobspec.cwd = os.getcwd()  # should be self.working_dir?
        jobspec.environment = dict(os.environ)
        jobspec.stdout = os.path.abspath(
            os.path.join(self.working_dir, tid + "_stdout_stderr.txt")
        )
        jobspec.stderr = jobspec.stdout
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
