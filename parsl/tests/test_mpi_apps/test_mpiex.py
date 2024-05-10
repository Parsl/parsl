"""Tests for the wrapper class"""
from inspect import signature

import pytest

from parsl import HighThroughputExecutor
from parsl.executors.high_throughput.mpi_executor import MPIExecutor


@pytest.mark.local
def test_docstring():
    """Ensure the old kwargs are copied over into the new class"""
    assert 'label' in MPIExecutor.__doc__
    assert 'max_workers_per_block' in MPIExecutor.__doc__
    assert 'available_accelerators' not in MPIExecutor.__doc__


@pytest.mark.local
def test_init():
    """Ensure all relevant kwargs are copied over from HTEx"""

    new_kwargs = {'max_workers_per_block'}
    excluded_kwargs = {'available_accelerators', 'enable_mpi_mode', 'cores_per_worker', 'max_workers_per_node',
                       'mem_per_worker', 'cpu_affinity', 'max_workers'}

    # Get the kwargs from both HTEx and MPIEx
    htex_kwargs = set(signature(HighThroughputExecutor.__init__).parameters)
    mpix_kwargs = set(signature(MPIExecutor.__init__).parameters)

    assert mpix_kwargs.difference(htex_kwargs) == new_kwargs
    assert len(mpix_kwargs.intersection(excluded_kwargs)) == 0
    assert mpix_kwargs.union(excluded_kwargs).difference(new_kwargs) == htex_kwargs
