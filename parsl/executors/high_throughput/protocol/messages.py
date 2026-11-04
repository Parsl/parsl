from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import msgspec
from typing_extensions import (
    NotRequired,
    TypedDict,
    get_args,
    get_origin,
    get_original_bases,
)

T = TypeVar("T")


class Channel(Generic[T]):
    def __init__(self, type: Any, name: str) -> None:
        self.type = type
        self.name = name

    def __repr__(self) -> str:
        return f"<Channel {self.name}>"


class Message(msgspec.Struct):
    def to_wire(self) -> bytes:
        from parsl.executors.high_throughput.protocol import wire
        return wire.to_wire(self)


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


class Task(Message):
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


_R = TypeVar("_R", bound=Message)


class Command(Message, Generic[_R]):
    _reply_channel: ClassVar[Channel[Any]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for base in get_original_bases(cls):
            if get_origin(base) is Command:
                (reply_type,) = get_args(base)
                cls._reply_channel = Channel(reply_type, f"{cls.__name__} reply")
                return
        if not hasattr(cls, "_reply_channel"):
            raise TypeError(f"{cls.__name__} must subclass Command[<reply class>]")

    @property
    def reply_channel(self) -> Channel[_R]:
        return self._reply_channel


class WorkerBindsReply(Message):
    port: int


class WorkerBinds(Command[WorkerBindsReply]):
    pass


class ConnectedBlocksReply(Message):
    block_ids: List[Optional[str]]


class ConnectedBlocks(Command[ConnectedBlocksReply]):
    pass


class ManagerInfo(TypedDict):
    manager: str
    block_id: Optional[str]
    worker_count: int
    tasks: int
    idle_duration: float
    active: bool
    parsl_version: str
    python_version: str
    draining: bool


class ManagersReply(Message):
    managers: List[ManagerInfo]


class Managers(Command[ManagersReply]):
    pass


class ManagersPackagesReply(Message):
    packages: Dict[str, Dict[str, Optional[str]]]


class ManagersPackages(Command[ManagersPackagesReply]):
    pass


class HoldManagerReply(Message):
    pass


class HoldManager(Command[HoldManagerReply]):
    manager_id: str


class CommandError(Message):
    pass


class ConnectionProbe(Message):
    pass


class ConnectionProbeReply(Message):
    pass


class Registration(Message):
    parsl_v: str
    python_v: str
    packages: Dict[str, Optional[str]]
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
    drain_time: float = float("inf")


class Heartbeat(Message):
    pass


class HeartbeatReply(Message):
    pass


class Drain(Message):
    pass


class Shutdown(Message):
    pass


class Monitoring(Message):
    payload: bytes


EXECUTOR_TO_INTERCHANGE: Channel[Task] = Channel(Task, "executor -> interchange")
INTERCHANGE_TO_EXECUTOR: Channel[Result] = Channel(Result, "interchange -> executor")

AnyCommand = Union[WorkerBinds, ConnectedBlocks, Managers, ManagersPackages, HoldManager]
COMMANDS: Channel[AnyCommand] = Channel(AnyCommand, "commands")

ManagerToInterchange = Union[ConnectionProbe, Registration, Heartbeat, Result, Monitoring, Drain]
MANAGER_TO_INTERCHANGE: Channel[ManagerToInterchange] = Channel(ManagerToInterchange, "manager -> interchange")

InterchangeToManager = Union[ConnectionProbeReply, HeartbeatReply, Tasks, Shutdown]
INTERCHANGE_TO_MANAGER: Channel[InterchangeToManager] = Channel(InterchangeToManager, "interchange -> manager")

WorkerToManager = Union[Result, Monitoring]
WORKER_TO_MANAGER: Channel[WorkerToManager] = Channel(WorkerToManager, "worker -> manager")
