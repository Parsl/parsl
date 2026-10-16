from typing import Any, Dict, List, Optional

from typing_extensions import TypedDict


class ManagerRecord(TypedDict, total=False):
    block_id: str
    start_time: float
    tasks: List[Any]
    worker_count: int
    max_capacity: int
    active: bool
    draining: bool
    drain_time: float
    hostname: str
    last_heartbeat: float
    idle_since: Optional[float]
    parsl_version: str
    python_version: str
    packages: Dict[str, str]

    # Only used by monitoring
    uid: str
    cpu_count: Optional[int]
    total_memory: int
