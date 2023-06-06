import logging
from concurrent.futures import Future
from typing import Any, Callable, List, Optional, TYPE_CHECKING

from parsl.app.futures import DataFuture
from parsl.data_provider.files import File
from parsl.data_provider.file_noop import NoOpFileStaging
from parsl.data_provider.ftp import FTPSeparateTaskStaging
from parsl.data_provider.http import HTTPSeparateTaskStaging
from parsl.data_provider.staging import Staging

if TYPE_CHECKING:
    from parsl.dataflow.dflow import DataFlowKernel

logger = logging.getLogger(__name__)

# these will be shared between all executors that do not explicitly
# override, so should not contain executor-specific state
default_staging: List[Staging]
default_staging = [NoOpFileStaging(), FTPSeparateTaskStaging(), HTTPSeparateTaskStaging()]


class DataManager:
    """The DataManager is responsible for transferring input and output data.

    """

    def __init__(self, dfk: "DataFlowKernel") -> None:
        """Initialize the DataManager.

        Args:
           - dfk (DataFlowKernel): The DataFlowKernel that this DataManager is managing data for.

        """

        self.dfk = dfk

    def replace_task_stage_out(self, file: File, func: Callable, executor: str) -> Callable:
        """This will give staging providers the chance to wrap (or replace entirely!) the task function."""
        executor_obj = self.dfk.executors[executor]
        if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
            storage_access = executor_obj.storage_access  # type: List[Staging]
        else:
            storage_access = default_staging

        for provider in storage_access:
            logger.debug("stage_out checking Staging provider {}".format(provider))
            if provider.can_stage_out(file):
                newfunc = provider.replace_task_stage_out(self, executor, file, func)
                if newfunc:
                    return newfunc
                else:
                    return func

        logger.debug("reached end of staging provider list")
        # if we reach here, we haven't found a suitable staging mechanism
        raise ValueError("Executor {} cannot stage file {}".format(executor, repr(file)))

    def optionally_stage_in(self, input, func, executor):
        if isinstance(input, DataFuture):
            file = input.file_obj.cleancopy()
            # replace the input DataFuture with a new DataFuture which will complete at
            # the same time as the original one, but will contain the newly
            # copied file
            input = DataFuture(input, file, tid=input.tid)
        elif isinstance(input, File):
            file = input.cleancopy()
            input = file
        else:
            return (input, func)

        replacement_input = self.stage_in(file, input, executor)

        func = self.replace_task(file, func, executor)

        return (replacement_input, func)

    def replace_task(self, file: File, func: Callable, executor: str) -> Callable:
        """This will give staging providers the chance to wrap (or replace entirely!) the task function."""

        executor_obj = self.dfk.executors[executor]
        if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
            storage_access = executor_obj.storage_access
        else:
            storage_access = default_staging

        for provider in storage_access:
            logger.debug("stage_in checking Staging provider {}".format(provider))
            if provider.can_stage_in(file):
                newfunc = provider.replace_task(self, executor, file, func)
                if newfunc:
                    return newfunc
                else:
                    return func

        logger.debug("reached end of staging provider list")
        # if we reach here, we haven't found a suitable staging mechanism
        raise ValueError("Executor {} cannot stage file {}".format(executor, repr(file)))

    def stage_in(self, file: File, input: Any, executor: str) -> Any:
        """Transport the input from the input source to the executor, if it is file-like,
        returning a DataFuture that wraps the stage-in operation.

        If no staging in is required - because the ``file`` parameter is not file-like,
        then return that parameter unaltered.

        Args:
            - self
            - input (Any) : input to stage in. If this is a File or a
              DataFuture, stage in tasks will be launched with appropriate
              dependencies. Otherwise, no stage-in will be performed.
            - executor (str) : an executor the file is going to be staged in to.
        """

        if isinstance(input, DataFuture):
            parent_fut = input  # type: Optional[Future]
        elif isinstance(input, File):
            parent_fut = None
        else:
            raise ValueError("Internal consistency error - should have checked DataFuture/File earlier")

        executor_obj = self.dfk.executors[executor]
        if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
            storage_access = executor_obj.storage_access
        else:
            storage_access = default_staging

        for provider in storage_access:
            logger.debug("stage_in checking Staging provider {}".format(provider))
            if provider.can_stage_in(file):
                staging_fut = provider.stage_in(self, executor, file, parent_fut=parent_fut)
                if staging_fut:
                    return staging_fut
                else:
                    return input

        logger.debug("reached end of staging provider list")
        # if we reach here, we haven't found a suitable staging mechanism
        raise ValueError("Executor {} cannot stage file {}".format(executor, repr(file)))

    def stage_out(self, file: File, executor: str, app_fu: Future) -> Optional[Future]:
        """Transport the file from the local filesystem to the remote Globus endpoint.

        This function returns either a Future which should complete when the stageout
        is complete, or None, if no staging needs to be waited for.

        Args:
            - self
            - file (File) - file to stage out
            - executor (str) - Which executor the file is going to be staged out from.
            - app_fu (Future) - a future representing the main body of the task that should
                                complete before stageout begins.
        """
        executor_obj = self.dfk.executors[executor]
        if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
            storage_access = executor_obj.storage_access
        else:
            storage_access = default_staging

        for provider in storage_access:
            logger.debug("stage_out checking Staging provider {}".format(provider))
            if provider.can_stage_out(file):
                return provider.stage_out(self, executor, file, app_fu)

        logger.debug("reached end of staging provider list")
        # if we reach here, we haven't found a suitable staging mechanism
        raise ValueError("Executor {} cannot stage out file {}".format(executor, repr(file)))
