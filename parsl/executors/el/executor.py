import logging
import os
import uuid
from concurrent.futures import Future
from typing import Any, Callable

import typeguard

from parsl.errors import OptionalModuleMissing
from parsl.executors.base import ParslExecutor
from parsl.executors.errors import InvalidResourceSpecification

try:
    from ensemble_launcher import EnsembleLauncher
    from ensemble_launcher.config import LauncherConfig, PolicyConfig, SystemConfig
    from ensemble_launcher.config.mpi_config import MPIConfig
    from ensemble_launcher.ensemble import Task
    from ensemble_launcher.helper_functions import get_nodes
    from ensemble_launcher.orchestrator import ClusterClient
except ImportError:
    _el_enabled = False
else:
    _el_enabled = True


logger = logging.getLogger(__name__)


def _is_int(value: Any) -> bool:
    """Return ``True`` for a real int; ``bool`` is rejected.

    ``bool`` subclasses ``int`` in Python, but is never a meaningful
    count or device index here, so ``True`` must not pass as ``1``.
    """
    return isinstance(value, int) and not isinstance(value, bool)


def _check_positive_int(value: Any) -> str | None:
    if not _is_int(value) or value < 1:
        return "a positive int"
    return None


def _check_ngpus_per_process(value: Any) -> str | None:
    if not (_is_int(value) or isinstance(value, float)) or value < 0:
        return "a non-negative int or float"
    return None


def _check_cpu_affinity(value: Any) -> str | None:
    if not isinstance(value, list) or not all(_is_int(v) for v in value):
        return "a list of int"
    return None


def _check_gpu_affinity(value: Any) -> str | None:
    if not isinstance(value, list) or not all(
        _is_int(v) or isinstance(v, str) for v in value
    ):
        return "a list of int or str"
    return None


def _check_env(value: Any) -> str | None:
    if not isinstance(value, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in value.items()
    ):
        return "a dict mapping str to str"
    return None


def _check_run_dir(value: Any) -> str | None:
    if not isinstance(value, (str, os.PathLike)):
        return "a str or os.PathLike"
    return None


# Maps each accepted resource specification key to a checker. A checker
# returns None when the value is acceptable, or a description of what
# was expected. The accepted types mirror the corresponding fields of
# ``ensemble_launcher.ensemble.Task``.
_RESOURCE_SPEC_VALIDATORS: dict[str, Callable[[Any], str | None]] = {
    "ppn": _check_positive_int,
    "nnodes": _check_positive_int,
    "ngpus_per_process": _check_ngpus_per_process,
    "cpu_affinity": _check_cpu_affinity,
    "gpu_affinity": _check_gpu_affinity,
    "env": _check_env,
    "run_dir": _check_run_dir,
}


class EnsembleExecutor(ParslExecutor):
    """Executor that delegates task execution to an EnsembleLauncher cluster.

    EnsembleExecutor wraps the ``ensemble_launcher`` package to provide
    hierarchical, multi-node task execution within Parsl. It starts (or
    connects to) an EnsembleLauncher orchestrator and submits tasks through
    a ``ClusterClient``.

    The executor supports two launch modes:

    1. **In-process mode** -- when ``client_only`` is ``False`` (the default),
       the orchestrator runs inside the current process.
    2. **Client-only mode** -- when ``client_only`` is ``True``, no
       orchestrator is started; only a ``ClusterClient`` is created that
       connects to an already-running orchestrator via its checkpoint
       directory.

    Parameters
    ----------
    cpus : list[int] or None, optional
        CPU core indices available for the orchestrator. Defaults to all
        cores reported by ``os.cpu_count()``.
    gpus : list[str | int] or None, optional
        GPU device identifiers available for the orchestrator. If ``None``
        (default), the comma-separated device list is read from the
        ``gpu_selector`` environment variable, and is empty when that
        variable is unset. Pass ``[]`` to force no GPUs.
    client_only : bool, optional
        If ``True``, skip starting an orchestrator and only create a
        ``ClusterClient`` that connects to an existing one. Default is
        ``False``.
    node_id : str, optional
        Scheduler node identifier the client connects to. ``"global"``
        (default) resolves to the global master node.
    child_executor_name : str, optional
        Name of the executor used for child processes in the orchestrator.
        Default is ``"async_mpi"``.
    task_executor_name : str or list[str], optional
        Name(s) of the executor used for task execution. Default is
        ``"async_processpool"``.
    comm_name : str, optional
        Communication backend name. Default is ``"async_zmq"``.
    nlevels : int, optional
        Number of hierarchical levels in the orchestrator tree. Default is
        ``0``.
    report_interval : float, optional
        Interval in seconds between resource usage reports. Default is
        ``10.0``.
    return_stdout : bool, optional
        If ``True``, capture and return task stdout. Default is ``False``.
    worker_logs : bool, optional
        Enable worker-level logging in the orchestrator. Default is
        ``False``.
    master_logs : bool, optional
        Enable master-level logging in the orchestrator. Default is
        ``False``.
    enable_workstealing : bool, optional
        Enable work-stealing across nodes. Default is ``False``.
    mpi_flavor : str or None, optional
        MPI implementation flavor (e.g., ``"openmpi"``, ``"mpich"``). If
        ``None`` (default), MPI configuration is omitted.
    gpu_selector : str, optional
        Environment variable used for GPU affinity masking. Default is
        ``"ZE_AFFINITY_MASK"``.
    overload_orchestrator_core : bool, optional
        Allow the orchestrator to share a CPU core with workers. Default is
        ``True``.
    checkpoint_dir : str or None, optional
        Directory for orchestrator checkpoint files. If ``None`` (default),
        a directory under the executor's ``run_dir`` is used.
    n_workers : int, optional
        Number of parallel send/recv pipelines in the ``ClusterClient``.
        Default is ``1``.
    checkpoint_timeout : float, optional
        Seconds to wait for checkpoint files to appear before raising an
        error. Default is ``300.0``.
    task_buffer_size : int, optional
        Flush the outgoing task buffer when it reaches this many tasks.
        Default is ``10000``.
    task_flush_interval : float, optional
        Seconds between periodic flushes of the task buffer. Default is
        ``0.5``.
    nodes : list[str] or None, optional
        Explicit list of node hostnames for the orchestrator to use. If
        ``None``, nodes are auto-detected.
    label : str, optional
        Label for this executor instance. Default is ``"EnsembleExecutor"``.
    children_scheduler_policy : str, optional
        Scheduling policy for distributing children across nodes. Default is
        ``"fixed_leafs_children_policy"``.
    leaf_nodes : int or None, optional
        Number of leaf nodes in the scheduler tree. Defaults to the number
        of detected nodes.
    nchildren : int or None, optional
        Number of children per level in the scheduler tree. Defaults to the
        number of detected nodes.
    """

    @typeguard.typechecked
    def __init__(
        self,
        cpus: list[int] | None = None,
        gpus: list[str | int] | None = None,
        client_only: bool = False,
        node_id: str = "global",
        child_executor_name: str = "async_mpi",
        task_executor_name: str | list[str] = "async_processpool",
        comm_name: str = "async_zmq",
        nlevels: int = 0,
        report_interval: float = 10.0,
        return_stdout: bool = False,
        worker_logs: bool = False,
        master_logs: bool = False,
        enable_workstealing: bool = False,
        mpi_flavor: str | None = None,
        gpu_selector: str = "ZE_AFFINITY_MASK",
        overload_orchestrator_core: bool = True,
        checkpoint_dir: str | None = None,
        n_workers: int = 1,
        checkpoint_timeout: float = 300.0,
        task_buffer_size: int = 10000,
        task_flush_interval: float = 0.5,
        nodes: list[str] | None = None,
        label: str = "EnsembleExecutor",
        children_scheduler_policy: str = "fixed_leafs_children_policy",
        leaf_nodes: int | None = None,
        nchildren: int | None = None,
    ):
        cpus = cpus or list(range(os.cpu_count() or 1))
        if gpus is None:
            gpus = [
                d.strip() for d in os.getenv(gpu_selector, "").split(",") if d.strip()
            ]
        if not _el_enabled:
            raise OptionalModuleMissing(
                ["ensemble_launcher"],
                "EnsembleExecutor requires the ensemble_launcher package",
            )

        super().__init__()
        self.label = label

        self._cpus = cpus
        self._gpus = gpus

        self._child_executor_name = child_executor_name
        self._task_executor_name = task_executor_name
        self._comm_name = comm_name
        self._nlevels = nlevels
        self._report_interval = report_interval
        self._return_stdout = return_stdout
        self._worker_logs = worker_logs
        self._master_logs = master_logs
        self._enable_workstealing = enable_workstealing
        self._mpi_flavor = mpi_flavor
        self._gpu_selector = gpu_selector
        self._overload_orchestrator_core = overload_orchestrator_core

        n_detected_nodes = len(get_nodes())
        self._leaf_nodes = leaf_nodes if leaf_nodes is not None else n_detected_nodes
        self._nchildren = nchildren if nchildren is not None else n_detected_nodes

        self._checkpoint_dir_arg = checkpoint_dir
        self._n_workers = n_workers
        self._checkpoint_timeout = checkpoint_timeout
        self._task_buffer_size = task_buffer_size
        self._task_flush_interval = task_flush_interval
        self._client_only = client_only
        self._node_id = node_id
        self._nodes = nodes

        self._el: EnsembleLauncher | None = None
        self._client: ClusterClient | None = None
        self._checkpoint_dir: str | None = None
        self._tasks: dict[str, Future] = {}

    def start(self) -> None:
        """Start the executor and launch the orchestrator.

        Resolves the checkpoint directory, then dispatches to one of two
        start paths depending on configuration:

        - In-process mode (``client_only=False``): starts the
          ``EnsembleLauncher`` in the current process.
        - Client-only mode (``client_only=True``): connects to an
          already-running orchestrator.
        """
        super().start()

        if self._checkpoint_dir_arg:
            self._checkpoint_dir = self._checkpoint_dir_arg
        else:
            self._checkpoint_dir = os.path.join(self.run_dir, self.label, "checkpoints")

        if not self._client_only:
            self._start_in_process()
        else:
            self._start_client()

    def _start_in_process(self) -> None:
        """Start the ``EnsembleLauncher`` in the current process.

        Builds ``SystemConfig`` and ``LauncherConfig`` from the executor's
        parameters, creates and starts an ``EnsembleLauncher`` instance,
        then starts a ``ClusterClient`` to submit tasks to it.

        Raises
        ------
        Exception
            Propagates any exception from ``EnsembleLauncher.start()`` or
            ``ClusterClient.start()``.
        """
        sys_config = SystemConfig(
            name="parsl-el",
            cpus=self._cpus,
            gpus=self._gpus,
            ncpus=len(self._cpus),
            ngpus=len(self._gpus),
        )

        launcher_kwargs: dict[str, Any] = {
            "child_executor_name": self._child_executor_name,
            "task_executor_name": self._task_executor_name,
            "comm_name": self._comm_name,
            "policy_config": PolicyConfig(
                nlevels=self._nlevels,
                nchildren=self._nchildren,
                leaf_nodes=self._leaf_nodes,
            ),
            "report_interval": self._report_interval,
            "return_stdout": self._return_stdout,
            "worker_logs": self._worker_logs,
            "master_logs": self._master_logs,
            "enable_workstealing": self._enable_workstealing,
            "gpu_selector": self._gpu_selector,
            "overload_orchestrator_core": self._overload_orchestrator_core,
            "cluster": True,
            "checkpoint_dir": self._checkpoint_dir,
            "log_dir": os.path.join(self.run_dir, self.label, "logs"),
        }
        if self._mpi_flavor is not None:
            launcher_kwargs["mpi_config"] = MPIConfig(flavor=self._mpi_flavor)

        launcher_config = LauncherConfig(**launcher_kwargs)

        self._el = EnsembleLauncher(
            ensemble_file={},
            system_config=sys_config,
            launcher_config=launcher_config,
            Nodes=self._nodes,
        )
        self._el.start()
        logger.info(
            "EnsembleLauncher started (checkpoint_dir=%s)", self._checkpoint_dir
        )

        self._start_client()

    def _start_client(self) -> None:
        """Create and start a ``ClusterClient`` synchronously.

        If the client fails to start and an ``EnsembleLauncher`` is running,
        the launcher is stopped before the exception is re-raised.

        Raises
        ------
        Exception
            Propagates any exception from ``ClusterClient.start()``. The
            ``EnsembleLauncher`` is stopped on failure to avoid orphaned
            processes.
        """
        try:
            self._client = ClusterClient(
                checkpoint_dir=self._checkpoint_dir,
                node_id=self._node_id,
                n_workers=self._n_workers,
                checkpoint_timeout=self._checkpoint_timeout,
                task_buffer_size=self._task_buffer_size,
                task_flush_interval=self._task_flush_interval,
            )
            self._client.start()
        except Exception:
            if self._el is not None:
                self._el.stop()
                self._el = None
            raise

        logger.info("ClusterClient started with %d pipeline(s)", self._n_workers)

    def submit(
        self,
        func: Callable,
        resource_specification: dict[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> Future:
        """Submit a task for execution on the orchestrator.

        Wraps ``func`` and its arguments into an ``ensemble_launcher.Task``,
        submits it through the ``ClusterClient``, and returns a
        ``Future`` that resolves when the task completes.

        Parameters
        ----------
        func : callable
            The callable to execute remotely.
        resource_specification : dict[str, Any]
            Resource requirements for the task. Supported keys are
            ``"ppn"`` (processes per node), ``"nnodes"`` (number of nodes),
            ``"ngpus_per_process"``, ``"cpu_affinity"``, ``"gpu_affinity"``,
            ``"env"`` (environment variables), and ``"run_dir"``.
        *args
            Positional arguments forwarded to ``func``.
        **kwargs
            Keyword arguments forwarded to ``func``.

        Returns
        -------
        Future
            A ``concurrent.futures.Future`` whose result is the return
            value of ``func(*args, **kwargs)``.

        Raises
        ------
        RuntimeError
            If the ``ClusterClient`` is not initialized -- either
            ``start()`` has not been called, or the executor has already
            been shut down.
        InvalidResourceSpecification
            If ``resource_specification`` contains an unrecognised key or
            a value of the wrong type.
        """
        if self._client is None:
            raise RuntimeError("ClusterClient is not initialized")

        self._validate_resource_spec(resource_specification)

        res = resource_specification or {}

        task_id = str(uuid.uuid4())
        task = Task(
            task_id=task_id,
            nnodes=res.get("nnodes", 1),
            ppn=res.get("ppn", 1),
            executable=func,
            ngpus_per_process=res.get("ngpus_per_process", 0),
            args=args,
            kwargs=kwargs,
            cpu_affinity=res.get("cpu_affinity", []),
            gpu_affinity=res.get("gpu_affinity", []),
            env=res.get("env", {}),
            run_dir=res.get("run_dir"),
        )

        fut = self._client.submit(task)
        fut.parsl_executor_task_id = task_id
        self._tasks[task_id] = fut
        fut.add_done_callback(lambda f: self._tasks.pop(task_id, None))
        return fut

    def shutdown(self) -> None:
        """Shut down the executor and release all resources.

        Tears down the ``ClusterClient``, stops the ``EnsembleLauncher``
        (if running in-process), and calls the parent ``shutdown``.
        Exceptions during teardown of individual components are logged
        but do not prevent the remaining cleanup from executing.
        """
        if self._client is not None:
            try:
                self._client.teardown()
            except Exception:
                logger.exception("Error during ClusterClient teardown")
            self._client = None

        if self._el is not None:
            try:
                self._el.stop()
            except Exception:
                logger.exception("Error during EnsembleLauncher stop")
            self._el = None

        super().shutdown()

    def monitor_resources(self) -> bool:
        """Indicate whether resource monitoring is supported.

        Returns
        -------
        bool
            Always ``False``; resource monitoring is handled internally
            by the ``EnsembleLauncher`` orchestrator.
        """
        return False

    def _validate_resource_spec(
        self, resource_specification: dict[str, Any] | None
    ) -> None:
        """Validate a task's resource specification.

        Checks that every key is recognised and that every value has a
        type ``ensemble_launcher.ensemble.Task`` will accept, so that a
        malformed specification is rejected at submit time rather than
        failing later inside the orchestrator.

        Parameters
        ----------
        resource_specification : dict[str, Any] or None
            The resource specification to validate. Recognised keys and
            their accepted types are defined in
            ``_RESOURCE_SPEC_VALIDATORS``. ``None`` and the empty dict
            are accepted and mean "use the defaults".

        Raises
        ------
        InvalidResourceSpecification
            If the specification contains an unrecognised key, or a
            recognised key whose value has the wrong type or is out of
            range.
        """
        if not resource_specification:
            return

        invalid_keys = set(resource_specification) - set(_RESOURCE_SPEC_VALIDATORS)
        if invalid_keys:
            message = (
                "EnsembleExecutor only accepts these resource specification "
                f"keys: {', '.join(sorted(_RESOURCE_SPEC_VALIDATORS))}"
            )
            logger.error(message)
            raise InvalidResourceSpecification(invalid_keys, message)

        bad_values = {}
        for key, value in resource_specification.items():
            expected = _RESOURCE_SPEC_VALIDATORS[key](value)
            if expected is not None:
                bad_values[key] = f"{key} must be {expected}, got {value!r}"

        if bad_values:
            message = "; ".join(bad_values[key] for key in sorted(bad_values))
            logger.error(message)
            raise InvalidResourceSpecification(set(bad_values), message)
