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
from parsl.tests.test_staging.staging_provider import NoOpTestingFileStaging

logger = logging.getLogger(__name__)


@bash_app
def touch(filename, inputs=[], outputs=[]):
    return "touch {}".format(filename)


@python_app
def test_in(file):
    # does not need to do anything as this
    # is just for exercising staging
    pass


@pytest.mark.local
def test_regression_stage_out_does_not_stage_in():
    no_stageout_config = Config(
        executors=[
            ThreadPoolExecutor(
                label='local_threads',
                storage_access=[NoOpTestingFileStaging(allow_stage_in=False)]
            )
        ]
    )

    parsl.load(no_stageout_config)

    # test with no staging
    touch("test.1", outputs=[File("test.1")]).result()

    # test with stage-out
    touch("test.2", outputs=[File("test.2")]).result()

    parsl.dfk().cleanup()
    parsl.clear()


@pytest.mark.local
def test_regression_stage_in_does_not_stage_out():
    no_stageout_config = Config(
        executors=[
            ThreadPoolExecutor(
                label='local_threads',
                storage_access=[NoOpTestingFileStaging(allow_stage_out=False)]
            )
        ]
    )

    parsl.load(no_stageout_config)

    # TODO create a file not using a task

    f = open("test.3", "a")
    f.write("test")
    f.close()

    test_in(File("test.3")).result()

    parsl.dfk().cleanup()
    parsl.clear()
