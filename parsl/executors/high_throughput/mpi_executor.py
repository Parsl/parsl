"""A simplified interface for HTEx when running in MPI mode"""
from typing import Optional, Tuple, List, Union, Sequence, Callable, Dict
from inspect import signature
import re

import typeguard

from parsl.data_provider.staging import Staging
from parsl.executors.high_throughput.executor import HighThroughputExecutor
from parsl.executors.status_handling import BlockProviderExecutor
from parsl.jobs.states import JobStatus
from parsl.providers import LocalProvider
from parsl.providers.base import ExecutionProvider


class MPIExecutor(HighThroughputExecutor):
    """A version of :class:`~parsl.HighThroughputExecutor` tuned for executing MPI tasks

    Parameters
    ----------
    max_tasks: int
        Maximum number of MPI applications to run at once per block
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
                 max_tasks: int = 1,
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
            cores_per_worker=1e-9,  # Ensures there will always be enough workers
            enable_mpi_mode=True,
            max_workers_per_node=max_tasks,

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


# Update the docstring on import
def _update_from_htex_docstring():
    """Get the parameters from the HTEx docstring that are used in MPIEx"""

    # Get the portion of the HTEx docstring dealing with parameters (last, by convention)
    htex_docstring = HighThroughputExecutor.__doc__
    param_tag = 'Parameters\n    ----------\n'
    param_start = htex_docstring.index(param_tag)
    htex_docstring = htex_docstring[param_start + len(param_tag):]
    assert htex_docstring.startswith('\n')  # Using asserts to catch if the HTEx docstring changes

    # Gather the documentation by param type
    by_params = re.split(r"\n {4}(\w+)\s?:", htex_docstring)[1:]  # Follows
    htex_params = dict(zip(by_params[::2], by_params[1::2]))
    assert 'label' in htex_params

    # Remove those parameters not present in the MPIEx
    mpi_sig = signature(MPIExecutor.__init__)
    copied_params = dict((k.strip(), v) for k, v in htex_params.items() if k in mpi_sig.parameters)
    assert 'label' in copied_params
    assert 'mpi_mode' not in copied_params
    new_docstring = ""
    for name, param in copied_params.items():
        new_docstring = new_docstring + f"    {name} : {param}\n"
    MPIExecutor.__doc__ = f'{MPIExecutor.__doc__}\n{new_docstring.rstrip()}'


_update_from_htex_docstring()
