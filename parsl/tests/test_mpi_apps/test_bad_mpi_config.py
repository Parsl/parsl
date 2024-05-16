import pytest

from parsl import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher, AprunLauncher, SimpleLauncher
from parsl.providers import SlurmProvider


@pytest.mark.local
def test_bad_launcher_with_mpi_mode():
    """AssertionError if a launcher other than SimpleLauncher is supplied"""

    for launcher in [SrunLauncher(), AprunLauncher()]:
        with pytest.raises(AssertionError):
            Config(executors=[
                HighThroughputExecutor(
                    enable_mpi_mode=True,
                    provider=SlurmProvider(launcher=launcher),
                )
            ])


@pytest.mark.local
def test_correct_launcher_with_mpi_mode():
    """Confirm that SimpleLauncher works with mpi_mode"""

    config = Config(executors=[
        HighThroughputExecutor(
            enable_mpi_mode=True,
            provider=SlurmProvider(launcher=SimpleLauncher()),
        )
    ])
    assert isinstance(config.executors[0].provider.launcher, SimpleLauncher)
