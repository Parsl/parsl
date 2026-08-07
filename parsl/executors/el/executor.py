import logging
import os
import threading
import uuid
from concurrent.futures import Future
from typing import Any, Callable

import typeguard

from parsl.errors import OptionalModuleMissing
from parsl.executors.status_handling import BlockProviderExecutor
from parsl.providers.base import ExecutionProvider

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

_VALID_RESOURCE_SPEC_KEYS: set[str] = {
    "ppn",
    "nnodes",
    "ngpus_per_process",
    "cpu_affinity",
    "gpu_affinity",
    "env",
    "run_dir",
}

_LAUNCH_SCRIPT_TEMPLATE = """\
import json
import sys

from ensemble_launcher import EnsembleLauncher
from ensemble_launcher.config import LauncherConfig, SystemConfig

with open(sys.argv[1]) as f:
    sys_config = SystemConfig.model_validate(json.load(f))
with open(sys.argv[2]) as f:
    launcher_config = LauncherConfig.model_validate(json.load(f))

nodes = sys.argv[3].split(",") if len(sys.argv) > 3 and sys.argv[3] else None

el = EnsembleLauncher(
    ensemble_file={{}},
    system_config=sys_config,
    launcher_config=launcher_config,
    Nodes=nodes,
)
el.run()
"""


class EnsembleExecutor(BlockProviderExecutor):
    """Executor that delegates task execution to an EnsembleLauncher cluster.

    EnsembleExecutor wraps the ``ensemble_launcher`` package to provide
    hierarchical, multi-node task execution within Parsl. It starts (or
    connects to) an EnsembleLauncher orchestrator and submits tasks through
    a ``ClusterClient``.

    The executor supports three launch modes:

    1. **Provider mode** -- when a ``provider`` is given, configuration files
       are written to disk and the orchestrator is launched on allocated nodes
       via the provider. The provider must use exactly one node per block and
       one block (``nodes_per_block=1``, ``init_blocks=1``, ``max_blocks=1``).
    2. **In-process mode** -- when no provider is given and ``client_only`` is
       ``False``, the orchestrator runs inside the current process.
    3. **Client-only mode** -- when ``client_only`` is ``True``, no
       orchestrator is started; only a ``ClusterClient`` is created that
       connects to an already-running orchestrator via its checkpoint
       directory.

    Parameters
    ----------
    cpus : list[int] or None, optional
        CPU core indices available for the orchestrator. Defaults to all
        cores reported by ``os.cpu_count()``.
    gpus : list[str | int] or None, optional
        GPU device identifiers available for the orchestrator. Defaults to
        an empty list.
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
        of detected nodes (in-process mode) or ``1`` (provider mode).
    nchildren : int or None, optional
        Number of children per level in the scheduler tree. Defaults to the
        number of detected nodes (in-process mode) or ``1`` (provider mode).
    provider : :class:`~parsl.providers.base.ExecutionProvider` or None, optional
        Execution provider for allocating compute resources. Must be
        configured with ``nodes_per_block=1``, ``init_blocks=1``, and
        ``max_blocks=1``. Default is ``None`` (in-process or client-only
        mode).
    block_error_handler : bool or callable, optional
        Handler for block errors. Default is ``True``.
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
        provider: ExecutionProvider | None = None,
        block_error_handler: bool | Callable = True,
    ):
        cpus = cpus or list(range(os.cpu_count()))
        gpus = gpus or []
        if not _el_enabled:
            raise OptionalModuleMissing(
                ["ensemble_launcher"],
                "EnsembleExecutor requires the ensemble_launcher package",
            )

        super().__init__(provider=provider, block_error_handler=block_error_handler)
        self.label = label

        if provider is not None:
            if provider.nodes_per_block != 1:
                raise ValueError(
                    f"EnsembleExecutor requires provider.nodes_per_block=1, "
                    f"got {provider.nodes_per_block}"
                )
            if provider.init_blocks != 1:
                raise ValueError(
                    f"EnsembleExecutor requires provider.init_blocks=1, "
                    f"got {provider.init_blocks}"
                )
            if provider.max_blocks != 1:
                raise ValueError(
                    f"EnsembleExecutor requires provider.max_blocks=1, "
                    f"got {provider.max_blocks}"
                )

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

        if provider is None:
            self._leaf_nodes = leaf_nodes if leaf_nodes is not None else len(get_nodes())
            self._nchildren = nchildren if nchildren is not None else len(get_nodes())
        else:
            self._leaf_nodes = leaf_nodes if leaf_nodes is not None else 1
            self._nchildren = nchildren if nchildren is not None else 1

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
        self._client_ready: threading.Event | None = None

    def start(self) -> None:
        """Start the executor and launch the orchestrator.

        Resolves the checkpoint directory, then dispatches to one of three
        start paths depending on configuration:

        - Provider mode (``provider`` is set): writes config files and
          launches via the provider.
        - In-process mode (no provider, ``client_only=False``): starts the
          ``EnsembleLauncher`` in the current process.
        - Client-only mode (``client_only=True``): connects to an
          already-running orchestrator.

        Raises
        ------
        OptionalModuleMissing
            If the ``ensemble_launcher`` package is not installed.
        """
        super().start()

        if self._checkpoint_dir_arg:
            self._checkpoint_dir = self._checkpoint_dir_arg
        else:
            self._checkpoint_dir = os.path.join(self.run_dir, self.label, "checkpoints")

        if self.provider is not None:
            self._start_via_provider()
        elif not self._client_only:
            self._start_in_process()
        else:
            self._start_client()

    def _start_via_provider(self) -> None:
        """Start the orchestrator through the execution provider.

        Writes system and launcher configuration files to disk, spawns a
        background thread that waits for the orchestrator to become ready
        and connects a ``ClusterClient``, then triggers provider-based
        block scaling.
        """
        self._setup_config_files()

        self._client_ready = threading.Event()
        client_thread = threading.Thread(
            target=self._connect_client, daemon=True, name="EL-Client-Connect"
        )
        client_thread.start()

        self.initialize_scaling()

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

    def _connect_client(self) -> None:
        """Connect a ``ClusterClient`` in a background thread.

        Intended to run in a daemon thread during provider-mode startup.
        Creates and starts a ``ClusterClient``, then signals
        ``_client_ready`` regardless of success or failure so that
        ``submit`` does not block indefinitely.
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
            logger.info("ClusterClient connected to orchestrator")
        except Exception:
            logger.exception("Failed to connect ClusterClient")
        finally:
            self._client_ready.set()

    def _setup_config_files(self) -> None:
        """Write orchestrator configuration files to disk.

        Creates three files under ``<run_dir>/<label>/el_configs/``:

        - ``system_config.json`` -- CPU/GPU hardware description.
        - ``launcher_config.json`` -- orchestrator behavioural settings.
        - ``_launch_el.py`` -- bootstrap script executed by the provider
          to start the ``EnsembleLauncher`` on the allocated node.

        The paths are stored in ``_system_config_path``,
        ``_launcher_config_path``, and ``_launch_script_path`` for use by
        ``_get_launch_command``.
        """
        config_dir = os.path.join(self.run_dir, self.label, "el_configs")
        os.makedirs(config_dir, exist_ok=True)

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

        self._system_config_path = os.path.join(config_dir, "system_config.json")
        with open(self._system_config_path, "w") as f:
            f.write(sys_config.model_dump_json(indent=2))

        self._launcher_config_path = os.path.join(config_dir, "launcher_config.json")
        with open(self._launcher_config_path, "w") as f:
            f.write(launcher_config.model_dump_json(indent=2))

        self._launch_script_path = os.path.join(config_dir, "_launch_el.py")
        with open(self._launch_script_path, "w") as f:
            f.write(_LAUNCH_SCRIPT_TEMPLATE)

    def initialize_scaling(self) -> None:
        """Initialize block scaling.

        This is a no-op for ``EnsembleExecutor`` because scaling is
        managed internally by the ``EnsembleLauncher`` orchestrator.
        """
        pass

    def _get_launch_command(self, block_id: str) -> str:
        """Build the shell command to launch the orchestrator on a block.

        Parameters
        ----------
        block_id : str
            Identifier of the provider block being launched.

        Returns
        -------
        str
            A ``python`` command that runs the bootstrap script with
            the system and launcher config paths as arguments, and
            optionally a comma-separated node list.
        """
        cmd = f"python {self._launch_script_path} {self._system_config_path} {self._launcher_config_path}"
        if self._nodes:
            cmd += f" {','.join(self._nodes)}"
        return cmd

    def outstanding(self) -> int:
        """Return the number of tasks that have not yet completed.

        Returns
        -------
        int
            Count of currently tracked (submitted but incomplete) tasks.
        """
        return len(self._tasks)

    @property
    def workers_per_node(self) -> int | float:
        """int or float: Number of workers per node.

        Always returns ``1`` because worker management is handled
        internally by the ``EnsembleLauncher`` orchestrator.
        """
        return 1

    @property
    def status_polling_interval(self) -> int:
        """int: Seconds between status polls.

        Returns the provider's polling interval if a provider is
        configured, otherwise ``0`` (no polling).
        """
        if self.provider is None:
            return 0
        return self.provider.status_polling_interval

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
            If the executor is in a bad state, the ``ClusterClient``
            failed to connect within ``checkpoint_timeout``, or the
            client is not initialized.
        """
        if self.bad_state_is_set:
            raise self.executor_exception

        if self._client_ready is not None and not self._client_ready.wait(
            timeout=self._checkpoint_timeout
        ):
            raise RuntimeError("ClusterClient failed to connect within timeout")

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
        (if running in-process), scales in any active provider blocks,
        and calls the parent ``shutdown``. Exceptions during teardown of
        individual components are logged but do not prevent the remaining
        cleanup from executing.
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

        if self.provider is not None:
            active_blocks = [
                block_id
                for block_id, status in self._status.items()
                if not status.terminal
            ]
            if active_blocks:
                self.scale_in(len(active_blocks))

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

        Parameters
        ----------
        resource_specification : dict[str, Any] or None
            The resource specification dictionary to validate. Valid
            keys are defined in ``_VALID_RESOURCE_SPEC_KEYS``.

        Note
        ----
        Currently a no-op. Subclasses may override to enforce
        constraints on allowed keys or value ranges.
        """
        pass
