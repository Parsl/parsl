import logging

import pytest

from parsl.executors.high_throughput.mpi_prefix_composer import (
    compose_all,
    compose_aprun_launch_cmd,
    compose_mpiexec_launch_cmd,
    compose_srun_launch_cmd,
)
from parsl.executors.high_throughput.mpi_resource_management import Scheduler

resource_spec = {"num_nodes": 2,
                 "num_ranks": 8,
                 "ranks_per_node": 4}


@pytest.mark.local
def test_srun_launch_cmd():
    prefix_name, composed_prefix = compose_srun_launch_cmd(
        resource_spec=resource_spec, node_hostnames=["node1", "node2"]
    )
    assert prefix_name == "PARSL_SRUN_PREFIX"
    logging.warning(composed_prefix)

    assert "None" not in composed_prefix


@pytest.mark.local
def test_aprun_launch_cmd():
    prefix_name, composed_prefix = compose_aprun_launch_cmd(
        resource_spec=resource_spec, node_hostnames=["node1", "node2"]
    )
    logging.warning(composed_prefix)
    assert prefix_name == "PARSL_APRUN_PREFIX"
    assert "None" not in composed_prefix


@pytest.mark.local
def test_mpiexec_launch_cmd():
    prefix_name, composed_prefix = compose_mpiexec_launch_cmd(
        resource_spec=resource_spec, node_hostnames=["node1", "node2"]
    )
    logging.warning(composed_prefix)
    assert prefix_name == "PARSL_MPIEXEC_PREFIX"
    assert "None" not in composed_prefix


@pytest.mark.local
def test_slurm_launch_cmd():
    table = compose_all(
        mpi_launcher="srun",
        resource_spec=resource_spec,
        node_hostnames=["NODE001", "NODE002"],
    )

    assert "PARSL_MPI_PREFIX" in table
    assert "PARSL_SRUN_PREFIX" in table


@pytest.mark.local
def test_default_launch_cmd():
    table = compose_all(
        mpi_launcher="srun",
        resource_spec=resource_spec,
        node_hostnames=["NODE001", "NODE002"],
    )

    assert "PARSL_MPI_PREFIX" in table
    assert "PARSL_SRUN_PREFIX" in table
    assert table["PARSL_MPI_PREFIX"] == table["PARSL_SRUN_PREFIX"]
