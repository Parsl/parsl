import pytest

import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider


class HookRecordingProvider(LocalProvider):
    """A LocalProvider that never runs blocks, and records calls to cancel
    and to the shutdown hook."""

    def __init__(self, *, cancel_raises: bool, shutdown_raises: bool):
        super().__init__(init_blocks=0, min_blocks=0, max_blocks=0, launcher=SimpleLauncher())
        self.cancel_raises = cancel_raises
        self.shutdown_raises = shutdown_raises
        self.cancel_calls = 0
        self.shutdown_calls = 0

    def cancel(self, job_ids):
        self.cancel_calls += 1
        if self.cancel_raises:
            raise RuntimeError("injected cancel failure")
        return [True for _ in job_ids]

    def shutdown(self):
        self.shutdown_calls += 1
        if self.shutdown_raises:
            raise RuntimeError("injected provider shutdown failure")


@pytest.mark.local
def test_default_provider_shutdown_is_noop():
    assert LocalProvider().shutdown() is None


@pytest.mark.local
@pytest.mark.parametrize("cancel_raises", (False, True))
@pytest.mark.parametrize("shutdown_raises", (False, True))
def test_provider_shutdown_hook_called_once(tmpd_cwd, cancel_raises, shutdown_raises):
    """DFK cleanup should scale in the executor's blocks and then call the
    provider shutdown hook exactly once, even if cancelling blocks raised,
    and a failing hook must not make cleanup raise.
    """
    provider = HookRecordingProvider(cancel_raises=cancel_raises, shutdown_raises=shutdown_raises)
    htex = HighThroughputExecutor(label="htex_hook", provider=provider)
    config = Config(executors=[htex], strategy='none', run_dir=str(tmpd_cwd))

    with parsl.load(config):
        assert provider.shutdown_calls == 0

    # htex scale_in always calls provider.cancel at DFK shutdown, even with
    # no blocks, so this checks the failing-cancel path was exercised.
    assert provider.cancel_calls >= 1
    assert provider.shutdown_calls == 1
