"""A simplified interface for HTEx when running in MPI mode"""
from typing import Optional, Tuple, List, Union, Callable, Dict

import typeguard

from parsl.data_provider.staging import Staging
from parsl.executors.high_throughput.executor import HighThroughputExecutor, GENERAL_HTEX_PARAM_DOCS
from parsl.executors.status_handling import BlockProviderExecutor
from parsl.jobs.states import JobStatus
from parsl.providers import LocalProvider
from parsl.providers.base import ExecutionProvider


class MPIExecutor(HighThroughputExecutor):
    __doc__ = f"""A version of :class:`~parsl.HighThroughputExecutor` tuned for executing multi-node (e.g., MPI) tasks.

    The Provider _must_ use the :class:`~parsl.launchers.SimpleLauncher`,
    which places a single pool of workers on the first node of a block.
    Each worker can then make system calls which use an MPI launcher (e.g., ``mpirun``, ``srun``)
    to spawn multi-node tasks.

    Specify the maximum number of multi-node tasks to run at once using ``max_workers_per_block``.
    The maximum number should be smaller than the ``nodes_per_block`` in the Provider.

    Parameters
    ----------
    max_workers_per_block: int
        Maximum number of MPI applications to run at once per block

    {GENERAL_HTEX_PARAM_DOCS}
    """

    @typeguard.typechecked
    def __init__(self,
                 label: str = 'MPIExecutor',
                 provider: ExecutionProvider = LocalProvider(),
                 launch_cmd: Optional[str] = None,
                 address: Optional[str] = None,
                 worker_ports: Optional[Tuple[int, int]] = None,
                 worker_port_range: Optional[Tuple[int, int]] = (54000, 55000),
                 interchange_port_range: Optional[Tuple[int, int]] = (55000, 56000),
                 storage_access: Optional[List[Staging]] = None,
                 working_dir: Optional[str] = None,
                 worker_debug: bool = False,
                 max_workers_per_block: int = 1,
                 prefetch_capacity: int = 0,
                 heartbeat_threshold: int = 120,
                 heartbeat_period: int = 30,
                 drain_period: Optional[int] = None,
                 poll_period: int = 10,
                 address_probe_timeout: Optional[int] = None,
                 worker_logdir_root: Optional[str] = None,
                 mpi_launcher: str = "mpiexec",
                 block_error_handler: Union[bool, Callable[[BlockProviderExecutor, Dict[str, JobStatus]], None]] = True,
                 encrypted: bool = False):
        super().__init__(
            # Hard-coded settings
            cores_per_worker=1e-9,  # Ensures there will be at least an absurd number of workers
            enable_mpi_mode=True,
            max_workers_per_node=max_workers_per_block,

            # Everything else
            label=label,
            provider=provider,
            launch_cmd=launch_cmd,
            address=address,
            worker_ports=worker_ports,
            worker_port_range=worker_port_range,
            interchange_port_range=interchange_port_range,
            storage_access=storage_access,
            working_dir=working_dir,
            worker_debug=worker_debug,
            prefetch_capacity=prefetch_capacity,
            heartbeat_threshold=heartbeat_threshold,
            heartbeat_period=heartbeat_period,
            drain_period=drain_period,
            poll_period=poll_period,
            address_probe_timeout=address_probe_timeout,
            worker_logdir_root=worker_logdir_root,
            mpi_launcher=mpi_launcher,
            block_error_handler=block_error_handler,
            encrypted=encrypted
        )

        self.max_workers_per_block = max_workers_per_block
