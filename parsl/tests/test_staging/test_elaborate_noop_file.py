# this replaces the no-op file staging provider with a more
# complicated no-op provider that launches (empty) tasks
# for staging

# the elaborate no-op provider is based around the structure
# of the globus staging provider

import logging

import pytest

import parsl
from parsl import bash_app, python_app
from parsl.config import Config
from parsl.data_provider.files import File
from parsl.executors.threads import ThreadPoolExecutor
from parsl.tests.test_staging.staging_provider import NoOpError, NoOpTestingFileStaging

logger = logging.getLogger(__name__)


@bash_app
def touch(filename, outputs=()):
    return f"touch {filename}"


@python_app
def app_test_in(file):
    # does not need to do anything as this
    # is just for exercising staging
    pass


@pytest.fixture
def storage_access_parsl():
    def _setup_config(*args, **kwargs):
        tpe = ThreadPoolExecutor(
            label='local_threads',
            storage_access=[NoOpTestingFileStaging(*args, **kwargs)]
        )
        config = Config(executors=[tpe])
        parsl.load(config)

    yield _setup_config

    parsl.dfk().cleanup()


@pytest.mark.local
def test_regression_stage_out_does_not_stage_in(storage_access_parsl, tmpd_cwd):
    storage_access_parsl(allow_stage_in=False)

    # Test that the helper app runs with no staging
    touch(str(tmpd_cwd / "test.1"), outputs=[]).result()

    # Test with stage-out, checking that provider stage-in is never
    # invoked. If stage-in is invoked, then the NoOpTestingFileStaging
    # provider will raise an exception, which should propagate to
    # .result() here.
    fpath = tmpd_cwd / "test.2"
    touch(str(fpath), outputs=[File(fpath)]).result()

    # Test that stage-in exceptions propagate out to user code.
    with pytest.raises(NoOpError):
        touch("test.3", inputs=[File("test.3")]).result()


@pytest.mark.local
def test_regression_stage_in_does_not_stage_out(storage_access_parsl, tmpd_cwd):
    storage_access_parsl(allow_stage_out=False)

    fpath = tmpd_cwd / "test.4"
    fpath.write_text("test")

    # Test that stage in does not invoke stage out. If stage out is
    # attempted, then the NoOpTestingFileStaging provider will raise
    # an exception which should propagate here.
    app_test_in(File("test.4")).result()

    # Test that stage out exceptions propagate to user code.
    with pytest.raises(NoOpError):
        touch("test.5", outputs=[File("test.5")]).result()
