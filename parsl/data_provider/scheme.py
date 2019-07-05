import typeguard
from typing import Optional

from parsl.utils import RepresentationMixin


class GlobusScheme(RepresentationMixin):
    """Specification for accessing data on a remote executor via Globus.

    Parameters
    ----------
    endpoint_uuid : str
        Universally unique identifier of the Globus endpoint at which the data can be accessed.
        This can be found in the `Manage Endpoints <https://www.globus.org/app/endpoints>`_ page.
    endpoint_path : str, optional
        FIXME
    local_path : str, optional
        FIXME
    """
    @typeguard.typechecked
    def __init__(self, endpoint_uuid: str, endpoint_path: Optional[str] = None, local_path: Optional[str] = None):
        self.endpoint_uuid = endpoint_uuid
        self.endpoint_path = endpoint_path
        self.local_path = local_path
