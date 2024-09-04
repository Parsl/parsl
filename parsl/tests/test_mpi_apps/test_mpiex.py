"""Tests for the wrapper class"""
from inspect import signature
from pathlib import Path

import pytest

from parsl import Config, HighThroughputExecutor
from parsl.executors.high_throughput.mpi_executor import MPIExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider

from .test_mpi_mode_enabled import get_env_vars

cwd = Path(__file__).parent.absolute()
pbs_nodefile = cwd.joinpath("mocks", "pbs_nodefile")


def local_config():
    return Config(
        executors=[
            MPIExecutor(
                max_workers_per_block=1,
                provider=LocalProvider(
                    worker_init=f"export PBS_NODEFILE={pbs_nodefile}",
                    launcher=SimpleLauncher()
                )
            )
        ]
    )


@pytest.mark.local
def test_docstring():
    """Ensure the old kwargs are copied over into the new class"""
    assert 'label' in MPIExecutor.__doc__
    assert 'max_workers_per_block' in MPIExecutor.__doc__
    assert 'available_accelerators' not in MPIExecutor.__doc__


@pytest.mark.local
def test_init():
    """Ensure all relevant kwargs are copied over from HTEx"""

    new_kwargs = {'max_workers_per_block', 'mpi_launcher'}
    excluded_kwargs = {'available_accelerators', 'cores_per_worker', 'max_workers_per_node',
                       'mem_per_worker', 'cpu_affinity', 'manager_selector'}

    # Get the kwargs from both HTEx and MPIEx
    htex_kwargs = set(signature(HighThroughputExecutor.__init__).parameters)
    mpix_kwargs = set(signature(MPIExecutor.__init__).parameters)

    assert mpix_kwargs.difference(htex_kwargs) == new_kwargs
    assert len(mpix_kwargs.intersection(excluded_kwargs)) == 0
    assert mpix_kwargs.union(excluded_kwargs).difference(new_kwargs) == htex_kwargs


@pytest.mark.local
def test_get_env():
    future = get_env_vars(parsl_resource_specification={
        "num_nodes": 3,
        "ranks_per_node": 5,
    })
    env_vars = future.result()
    assert env_vars['PARSL_NUM_RANKS'] == '15'
