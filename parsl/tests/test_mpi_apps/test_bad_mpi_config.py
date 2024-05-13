import pytest

from parsl import Config
from parsl.executors import MPIExecutor
from parsl.launchers import AprunLauncher, SimpleLauncher, SrunLauncher
from parsl.providers import SlurmProvider


@pytest.mark.local
def test_bad_launcher():
    """TypeError if a launcher other than SimpleLauncher is supplied"""

    for launcher in [SrunLauncher(), AprunLauncher()]:
        with pytest.raises(TypeError):
            Config(executors=[
                MPIExecutor(
                    provider=SlurmProvider(launcher=launcher),
                )
            ])


@pytest.mark.local
def test_bad_mpi_launcher():
    """ValueError if an unsupported mpi_launcher is specified"""

    with pytest.raises(ValueError):
        Config(executors=[
            MPIExecutor(
                mpi_launcher="bad_launcher",
                provider=SlurmProvider(launcher=SimpleLauncher()),
            )
        ])


@pytest.mark.local
@pytest.mark.parametrize(
    "mpi_launcher",
    ["srun", "aprun", "mpiexec"]
)
def test_correct_launcher_with_mpi_mode(mpi_launcher: str):
    """Confirm that SimpleLauncher works with mpi_mode"""

    executor = MPIExecutor(
        mpi_launcher=mpi_launcher,
        provider=SlurmProvider(launcher=SimpleLauncher()),
    )

    assert isinstance(executor.provider.launcher, SimpleLauncher)
