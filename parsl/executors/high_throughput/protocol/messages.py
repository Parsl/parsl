"""The messages of the HTEX protocol.

This module is the specification of every message exchanged between the
components of the :class:`~parsl.executors.high_throughput.executor.HighThroughputExecutor`:

- the **executor**, in the Parsl (or Globus Compute endpoint) process
- the **interchange**, a separate process started by the executor
- the **manager** (``process_worker_pool.py``), one per node, which may run in
  a *different* Python environment, with different versions of Python and
  Parsl, from the executor and interchange
- the **workers**, child processes of the manager

Every message is an instance of a :class:`~.schema.Message` subclass,
which names itself on the wire with its ``type`` tag. Build a message with
its dataclass constructor, encode it with
:meth:`~.schema.Message.to_wire`, and decode it with
:func:`~.protocol.from_wire`; :mod:`~.schema` also describes the wire format.

Channels
--------

``executor -> interchange`` (task channel)
    :class:`Task`: a task for a worker to run.

``interchange -> executor`` (result channel)
    :class:`Result`: a task outcome, forwarded verbatim from a worker, or generated
    by the interchange when a manager is lost. A result with ``task_id`` of
    ``-1`` and an ``exception`` means the interchange has failed fatally and
    the executor should fail all tasks.

``executor -> interchange`` (command channel; :data:`COMMANDS`)
    A :class:`Command` (:class:`WorkerBinds`, :class:`ConnectedBlocks`,
    :class:`Managers`, :class:`ManagersPackages`, :class:`HoldManager`),
    answered by its reply message.

``manager -> interchange`` (:data:`MANAGER_TO_INTERCHANGE`)
    :class:`ConnectionProbe`, :class:`Registration`, :class:`Heartbeat`,
    :class:`Result`, :class:`Monitoring`, :class:`Drain`. A manager first
    sends a connection probe, then its registration; the interchange ignores other messages from
    managers that have not registered.

``interchange -> manager`` (:data:`INTERCHANGE_TO_MANAGER`)
    :class:`ConnectionProbeReply`, :class:`HeartbeatReply`, :class:`Tasks`,
    :class:`Shutdown`.

``worker -> manager``
    :class:`Result` and :class:`Monitoring` messages, already encoded, which
    the manager forwards to the interchange without re-encoding. (Tasks
    travel from the manager to its workers over a local ``multiprocessing``
    queue, within one Python environment, so they are pickled rather than
    sent over the wire.)

Payloads
--------

Fields of type ``bytes`` are opaque to the protocol. In particular, the
task ``buffer`` and the ``result``/``exception`` of a result are serialized
by :mod:`parsl.serialize` (or by whatever the submitter of the task chose),
and the ``payload`` of a monitoring message is serialized by the monitoring
radio. The protocol does not interpret them.
"""
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

from typing_extensions import (
    NotRequired,
    TypedDict,
    get_args,
    get_origin,
    get_original_bases,
)

from parsl.executors.high_throughput.protocol.schema import Message


# Task contexts are plain dicts, rather than messages, because callers of
# HighThroughputExecutor.submit_payload build them. They hold no bytes, so
# they go over the wire as plain JSON, unchecked.
class TaskExecutor(TypedDict):
    """Run the task by importing ``f`` (a dotted import path) and calling
    ``f(buffer, *a, **k)``, instead of parsl's default ``execute_task``.
    ``a`` and ``k`` must be JSON values."""
    f: str
    a: NotRequired[Sequence[Any]]
    k: NotRequired[Dict[str, Any]]


class ResourceSpec(TypedDict, total=False):
    """A task's resource requests, validated by the executor."""
    priority: float  # read by the interchange; lower value = scheduled sooner
    # MPI mode: read by the manager and workers
    num_nodes: int
    ranks_per_node: int
    num_ranks: int
    launcher_options: str
    # set by the manager's MPI scheduler, for its workers; never sent
    MPI_NODELIST: str


class TaskContext(TypedDict, total=False):
    """Per-task information besides the function buffer."""
    task_executor: TaskExecutor
    resource_spec: ResourceSpec


class Task(Message):
    """executor -> interchange -> manager -> worker"""
    type = "task"
    task_id: int
    context: TaskContext
    buffer: bytes  # the serialized function and its arguments


class Tasks(Message):
    """interchange -> manager: a batch of tasks"""
    type = "tasks"
    tasks: List[Task]


class Result(Message):
    """worker -> manager -> interchange -> executor

    Exactly one of ``result`` or ``exception`` should be set.
    """
    type = "result"
    task_id: int
    result: Optional[bytes] = None  # serialized return value, on success
    exception: Optional[bytes] = None  # serialized exception, on failure


_R = TypeVar("_R", bound=Message)


class Command(Message, Generic[_R], abstract=True):
    """executor -> interchange: a request, answered by its reply message ``_R``

    Subclass as ``Command[<reply class>]``; the interchange answers every
    command, so the executor's REQ socket is never left waiting.
    """
    reply_type: ClassVar[Type[_R]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        for base in get_original_bases(cls):
            if (get_origin(base) or base) is Command:
                (cls.reply_type,) = get_args(base)
        super().__init_subclass__(**kwargs)


class WorkerBindsReply(Message):
    type = "worker_binds_reply"
    port: int  # the port managers connect to


class WorkerBinds(Command[WorkerBindsReply]):
    type = "worker_binds"


class ConnectedBlocksReply(Message):
    type = "connected_blocks_reply"
    block_ids: List[Optional[str]]  # in order of manager registration


class ConnectedBlocks(Command[ConnectedBlocksReply]):
    type = "connected_blocks"


class ManagerInfo(TypedDict):
    manager: str  # the manager ID
    block_id: Optional[str]
    worker_count: int
    tasks: int  # tasks sent to the manager, not yet finished
    idle_duration: float  # seconds
    active: bool  # False once held
    parsl_version: str
    python_version: str
    draining: bool


class ManagersReply(Message):
    type = "managers_reply"
    managers: List[ManagerInfo]


class Managers(Command[ManagersReply]):
    type = "managers"


class ManagersPackagesReply(Message):
    type = "managers_packages_reply"
    packages: Dict[str, Dict[str, Optional[str]]]  # manager ID -> distribution name -> version


class ManagersPackages(Command[ManagersPackagesReply]):
    type = "managers_packages"


class HoldManagerReply(Message):
    type = "hold_manager_reply"


class HoldManager(Command[HoldManagerReply]):
    """Stop sending tasks to a manager."""
    type = "hold_manager"
    manager_id: str


class ConnectionProbe(Message):
    type = "connection_probe"


class ConnectionProbeReply(Message):
    type = "connection_probe_reply"


class Registration(Message):
    type = "registration"
    parsl_v: str
    python_v: str  # e.g. "3.12.4"
    packages: Dict[str, Optional[str]]  # distribution name -> version
    worker_count: int
    uid: str  # the manager ID; matches the ZMQ socket identity
    block_id: Optional[str]
    start_time: float  # UNIX time at which the manager started
    prefetch_capacity: int
    max_capacity: int  # tasks the manager will accept at once
    os: str
    hostname: str
    dir: str
    cpu_count: Optional[int]
    total_memory: int  # bytes
    # UNIX time after which the interchange should drain this manager; may
    # be inf. None means "never".
    drain_time: Optional[float] = None


class Heartbeat(Message):
    type = "heartbeat"


class HeartbeatReply(Message):
    type = "heartbeat_reply"


class Drain(Message):
    """manager -> interchange: stop sending tasks to this manager"""
    type = "drain"


class Shutdown(Message):
    """interchange -> manager: the manager is drained and should exit"""
    type = "shutdown"


class Monitoring(Message):
    """worker -> manager -> interchange: a monitoring radio message"""
    type = "monitoring"
    payload: bytes


# Channels are unannotated tuples, so mypy sees each class; see protocol.from_wire

#: The commands the executor may send to the interchange.
COMMANDS = (WorkerBinds, ConnectedBlocks, Managers, ManagersPackages, HoldManager)
#: The messages a manager may send to the interchange.
MANAGER_TO_INTERCHANGE = (ConnectionProbe, Registration, Heartbeat, Result, Monitoring, Drain)
#: The messages the interchange may send to a manager.
INTERCHANGE_TO_MANAGER = (ConnectionProbeReply, HeartbeatReply, Tasks, Shutdown)
