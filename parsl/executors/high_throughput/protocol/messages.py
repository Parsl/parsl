from typing import Any, Dict, Generic, List, Optional, Sequence, TypeVar, Union

import msgspec
from typing_extensions import NotRequired, TypedDict, TypeForm

T = TypeVar("T")


class Channel(Generic[T]):
    def __init__(self, name: str, type: TypeForm[T]) -> None:
        self.name = name
        self.type = type

    def __repr__(self) -> str:
        return f"<Channel {self.name}>"


class Message(msgspec.Struct, tag_field="type"):
    def encode(self) -> bytes:
        from parsl.executors.high_throughput.protocol import encode
        return encode(self)


class TaskExecutor(TypedDict):
    f: str
    a: NotRequired[Sequence[Any]]
    k: NotRequired[Dict[str, Any]]


class ResourceSpec(TypedDict, total=False):
    priority: float
    num_nodes: int
    ranks_per_node: int
    num_ranks: int
    launcher_options: str
    MPI_NODELIST: str


class TaskContext(TypedDict, total=False):
    task_executor: TaskExecutor
    resource_spec: ResourceSpec


class Task(TypedDict):
    task_id: int
    context: TaskContext
    buffer: bytes


class Tasks(Message):
    tasks: List[Task]


class Result(Message):
    """Exactly one of ``result`` or ``exception`` should be set."""
    task_id: int
    result: Optional[bytes] = None
    exception: Optional[bytes] = None


class InterchangeFailure(Message):
    exception: bytes


class ConnectionProbe(Message):
    pass


class ConnectionProbeReply(Message):
    pass


class Registration(Message):
    parsl_v: str
    python_v: str
    packages: Dict[str, str]
    worker_count: int
    uid: str
    block_id: Optional[str]
    start_time: float
    prefetch_capacity: int
    max_capacity: int
    os: str
    hostname: str
    dir: str
    cpu_count: Optional[int]
    total_memory: int
    drain_time: float


class Heartbeat(Message):
    pass


class HeartbeatReply(Message):
    pass


class Drain(Message):
    pass


class Shutdown(Message):
    pass


class Monitoring(Message):
    payload: Any


INTERCHANGE_TO_EXECUTOR = Channel("interchange -> executor", Union[Result, InterchangeFailure])
MANAGER_TO_INTERCHANGE = Channel("manager -> interchange", Union[ConnectionProbe, Registration, Heartbeat, Result, Monitoring, Drain])
INTERCHANGE_TO_MANAGER = Channel("interchange -> manager", Union[ConnectionProbeReply, HeartbeatReply, Tasks, Shutdown])
