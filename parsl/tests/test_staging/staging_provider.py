# this replaces the no-op file staging provider with a more
# complicated no-op provider that launches (empty) tasks
# for staging, and asserts specific behaviours do/do not happen.

# the elaborate no-op provider is based around the structure
# of the globus staging provider

import logging

from parsl import python_app
from parsl.data_provider.staging import Staging
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class NoOpTestingFileStaging(Staging, RepresentationMixin):

    def __init__(self, allow_stage_in=True, allow_stage_out=True):
        """The NoOpTestingFileStaging provider can be configured to fail in
        specific ways to ensure that certain things are not happening.

        The provider will always claim that it can stage "file" URLs,
        but will raise an exception if staging is actually attempted
        if allow_stage[in/out] is set to False.
        """
        self.allow_stage_in = allow_stage_in
        self.allow_stage_out = allow_stage_out

    def can_stage_in(self, file):
        logger.info("NoOpTestingFileStaging checking file {} for stage-in".format(repr(file)))
        b = file.scheme == 'file'
        logger.info("b = {}".format(b))
        return b

    def can_stage_out(self, file):
        logger.info("NoOpTestingFileStaging checking file {} for stage-out".format(repr(file)))
        b = file.scheme == 'file'
        logger.info("b = {}".format(b))
        return b

    def stage_in(self, dm, executor, file, parent_fut):
        logger.info("Creating state-in app for file {}".format(file))
        if self.allow_stage_in:
            stage_in_app = make_stage_in_app(executor=executor, dfk=dm.dfk)
            app_fut = stage_in_app(outputs=[file], _parsl_staging_inhibit=True, parent_fut=parent_fut)
            return app_fut._outputs[0]
        else:
            raise NoOpError("NoOpTestingFileStaging provider configured to prohibit stage in")

    def stage_out(self, dm, executor, file, app_fu):
        logger.info("Creating state-out app for file {}".format(file))
        if self.allow_stage_out:
            stage_out_app = make_stage_out_app(executor=executor, dfk=dm.dfk)
            return stage_out_app(app_fu, inputs=[file], _parsl_staging_inhibit=True)
        else:
            raise NoOpError("NoOpTestingFileStaging provider configured to prohibit stage out")


def make_stage_out_app(executor, dfk):
    return python_app(executors=[executor], data_flow_kernel=dfk)(stage_out_noop)


def stage_out_noop(app_fu, inputs=[], _parsl_staging_inhibit=True):
    import time
    import logging
    logger = logging.getLogger(__name__)
    logger.info("stage_out_noop")
    time.sleep(1)
    return None


def make_stage_in_app(executor, dfk):
    return python_app(executors=[executor], data_flow_kernel=dfk)(stage_in_noop)


def stage_in_noop(parent_fut=None, outputs=[], _parsl_staging_inhibit=True):
    import time
    import logging
    logger = logging.getLogger(__name__)
    logger.info("stage_in_noop")
    time.sleep(1)
    return None


class NoOpError(RuntimeError):
    """This should behave the same as a RuntimeError, but is a subclass so
    that it can be checked for specifically in test cases."""
    pass
