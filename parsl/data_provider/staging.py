from concurrent.futures import Future
from typing import Optional, Callable
from parsl.app.futures import DataFuture
from parsl.data_provider.files import File

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from parsl.data_provider.data_manager import DataManager


class Staging:
    """
    This class defines the interface for file staging providers.

    For each file to be staged in, the data manager will present the file
    to each configured Staging provider in turn: first, it will ask if the
    provider can stage this file by calling `can_stage_in`, and if so, it
    will call both `stage_in` and `replace_task` to give the provider the
    opportunity to perform staging.

    For each file to be staged out, the data manager will follow the same
    pattern using the corresponding stage out methods of this class.

    The default implementation of this class rejects all files, and
    performs no staging actions.

    To implement a concrete provider, one or both of the `can_stage_*`
    methods should be overridden to match the appropriate files, and then
    the corresponding `stage_*` and/or `replace_task*` methods should be
    implemented.
    """

    def can_stage_in(self, file: File) -> bool:
        """
        Given a File object, decide if this staging provider can
        stage the file. Usually this is be based on the URL
        scheme, but does not have to be. If this returns True,
        then other methods of this Staging object will be called
        to perform the staging.
        """
        return False

    def can_stage_out(self, file: File) -> bool:
        """
        Like can_stage_in, but for staging out.
        """
        return False

    def stage_in(self, dm: "DataManager", executor: str, file: File, parent_fut: Optional[Future]) -> Optional[DataFuture]:
        """
        This call gives the staging provider an opportunity to prepare for
        stage-in and to launch arbitrary tasks which must complete as part
        of stage-in.

        This call will be made with a fresh copy of the File that may be
        modified for the purposes of this particular staging operation,
        rather than the original application-provided File. This allows
        staging specific information (primarily localpath) to be set on the
        File without interfering with other stagings of the same File.

        The call can return a:
          - DataFuture: the corresponding task input parameter will be
            replaced by the DataFuture, and the main task will not
            run until that DataFuture is complete. The DataFuture result
            should be the file object as passed in.
          - None: the corresponding task input parameter will be replaced
            by a suitable automatically generated replacement that container
            the File fresh copy, or is the fresh copy.
        """
        return None

    def stage_out(self, dm: "DataManager", executor: str, file: File, app_fu: Future) -> Optional[Future]:
        """
        This call gives the staging provider an opportunity to prepare for
        stage-out and to launch arbitrary tasks which must complete as
        part of stage-out.

        Even though it should set up stageout, it will be invoked before
        the task executes. Any work which needs to happen after the main task
        should depend on app_fu.

        For a given file, either return a Future which completes when stageout
        is complete, or return None to indicate that no stageout action need
        be waited for. When that Future completes, parsl will mark the relevant
        output DataFuture complete.

        Note the asymmetry here between stage_in and stage_out: this can return
        any Future, while stage_in must return a DataFuture.
        """
        return None

    def replace_task(self, dm: "DataManager", executor: str, file: File, func: Callable) -> Optional[Callable]:
        """
        For a file to be staged in, optionally return a replacement app
        function, which usually should be the original app function wrapped
        in staging code.
        """
        return None

    def replace_task_stage_out(self, dm: "DataManager", executor: str, file: File, func: Callable) -> Optional[Callable]:
        """
        For a file to be staged out, optionally return a replacement app
        function, which usually should be the original app function wrapped
        in staging code.
        """
        return None
