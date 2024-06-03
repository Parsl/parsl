from datetime import datetime
from typing import Any, List, Optional

from typing_extensions import TypedDict


class ManagerRecord(TypedDict, total=False):
    block_id: Optional[str]
    tasks: List[Any]
    worker_count: int
    max_capacity: int
    active: bool
    draining: bool
    hostname: str
    last_heartbeat: float
    idle_since: Optional[float]
    timestamp: datetime
    parsl_version: str
    python_version: str
