import pytest

import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider


class CancelRecordingProvider(LocalProvider):
    """A LocalProvider that never runs blocks, and records (and optionally
    fails) calls to cancel."""

    def __init__(self, *, cancel_raises: bool):
        super().__init__(init_blocks=0, min_blocks=0, max_blocks=0, launcher=SimpleLauncher())
        self.cancel_raises = cancel_raises
        self.cancel_calls = 0

    def cancel(self, job_ids):
        self.cancel_calls += 1
        if self.cancel_raises:
            raise RuntimeError("injected cancel failure")
        return [True for _ in job_ids]


@pytest.mark.local
def test_scale_in_failure_does_not_stop_cleanup(tmpd_cwd):
    """If scaling in one executor raises at DFK cleanup, the other executors
    are still scaled in, and cleanup itself completes without raising."""
    failing = CancelRecordingProvider(cancel_raises=True)
    working = CancelRecordingProvider(cancel_raises=False)
    config = Config(
        executors=[
            HighThroughputExecutor(label="htex_failing", provider=failing),
            HighThroughputExecutor(label="htex_working", provider=working),
        ],
        strategy='none',
        run_dir=str(tmpd_cwd),
    )

    with parsl.load(config):
        pass

    # htex scale_in calls provider.cancel at DFK shutdown even with no
    # blocks, so both paths were exercised.
    assert failing.cancel_calls >= 1
    assert working.cancel_calls >= 1
