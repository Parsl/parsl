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
        For a given file, either return a DataFuture to substitute
        for this file, or return None to perform no substitution
        """
        return None

    def stage_out(self, dm: "DataManager", executor: str, file: File, app_fu) -> Optional[DataFuture]:
        return None

    def replace_task(self, dm: "DataManager", executor: str, file: File, func: Callable) -> Optional[Callable]:
        """
        For a file to be staged in, either return a replacement app
        function, which usually should be the original app function wrapped
        in staging code.
        """
        return None

    def replace_task_stage_out(self, dm: "DataManager", executor: str, file: File, func: Callable) -> Optional[Callable]:
        return None
