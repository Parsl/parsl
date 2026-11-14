import pickle
from typing import Any, Dict, TypeVar, cast

import msgspec

from parsl.executors.high_throughput.protocol import messages as m
from parsl.executors.high_throughput.protocol.errors import ProtocolError

HEARTBEAT_CODE = (2 ** 32) - 1
SHUTDOWN_CODE = (2 ** 32) - 2

T = TypeVar("T")


def encode(message: m.Message) -> bytes:
    legacy_form: Any

    match message:
        case m.ConnectionProbeReply():
            return b""  # not pickled; was sent as this byte literal pre-protocol
        case m.Tasks(tasks=tasks):
            legacy_form = tasks
        case m.Result(task_id=task_id, result=result, exception=exception):
            d: Dict[str, Any] = {"type": "result", "task_id": task_id}
            if result is not None:
                d["result"] = result
            if exception is not None:
                d["exception"] = exception
            legacy_form = d
        case m.Registration():
            legacy_form = {
                "type": "registration",
                "parsl_v": message.parsl_v,
                "python_v": message.python_v,
                "packages": message.packages,
                "worker_count": message.worker_count,
                "uid": message.uid,
                "block_id": message.block_id,
                "start_time": message.start_time,
                "drain_time": message.drain_time,
                "prefetch_capacity": message.prefetch_capacity,
                "max_capacity": message.max_capacity,
                "os": message.os,
                "hostname": message.hostname,
                "dir": message.dir,
                "cpu_count": message.cpu_count,
                "total_memory": message.total_memory,
            }
        case m.InterchangeFailure(exception=exception):
            legacy_form = {"type": "result", "task_id": -1, "exception": exception}
        case m.ConnectionProbe():
            legacy_form = {"type": "connection_probe"}
        case m.Heartbeat():
            legacy_form = {"type": "heartbeat"}
        case m.Drain():
            legacy_form = {"type": "drain"}
        case m.Monitoring(payload=payload):
            legacy_form = {"type": "monitoring", "payload": payload}
        case m.HeartbeatReply():
            legacy_form = HEARTBEAT_CODE
        case m.Shutdown():
            legacy_form = SHUTDOWN_CODE
        case _:
            raise TypeError(f"{type(message).__name__} has no legacy protocol form")

    return pickle.dumps(legacy_form)


def decode(data: bytes, channel: m.Channel[T]) -> T:
    message: m.Message
    try:
        match channel:
            case m.INTERCHANGE_TO_EXECUTOR:
                message = _interchange_to_executor(data)
            case m.MANAGER_TO_INTERCHANGE:
                message = _manager_to_interchange(data)
            case m.INTERCHANGE_TO_MANAGER:
                message = _interchange_to_manager(data)
            case _:
                raise ProtocolError(f"Channel {channel.name!r} does not exist in legacy protocol")
    except ProtocolError:
        raise
    except Exception as e:
        raise ProtocolError(f"Malformed legacy message on {channel.name}: {e}") from e
    return cast(T, message)


def _interchange_to_executor(data: bytes) -> m.Message:
    match obj := pickle.loads(data):
        case {"type": "result", "task_id": -1, **rest}:
            return msgspec.convert(rest, m.InterchangeFailure)
        case {"type": "result", **rest}:
            return msgspec.convert(rest, m.Result)
        case _:
            raise ValueError(f"unexpected message {obj!r:.100}")


def _manager_to_interchange(data: bytes) -> m.Message:
    match obj := pickle.loads(data):
        case {"type": "connection_probe", **rest}:
            return msgspec.convert(rest, m.ConnectionProbe)
        case {"type": "registration", **rest}:
            return _registration(rest)
        case {"type": "heartbeat", **rest}:
            return msgspec.convert(rest, m.Heartbeat)
        case {"type": "result", **rest}:
            return msgspec.convert(rest, m.Result)
        case {"type": "monitoring", "payload": payload}:
            return m.Monitoring(payload=payload)
        case {"type": "drain", **rest}:
            return msgspec.convert(rest, m.Drain)
        case _:
            raise ValueError(f"unexpected message {obj!r:.100}")


def _interchange_to_manager(data: bytes) -> m.Message:
    if data == b"":
        return m.ConnectionProbeReply()
    match obj := pickle.loads(data):
        case int() if obj == HEARTBEAT_CODE:
            return m.HeartbeatReply()
        case int() if obj == SHUTDOWN_CODE:
            return m.Shutdown()
        case list():
            return m.Tasks(tasks=[_task(t) for t in obj])
        case _:
            raise ValueError(f"unexpected message {obj!r:.100}")


def _registration(fields: Dict[Any, Any]) -> m.Registration:
    defaults = {
        "packages": {},
        "worker_count": 1,
        "uid": "",
        "block_id": None,
        "start_time": 0.0,
        "prefetch_capacity": 0,
        "max_capacity": 1,
        "os": "",
        "hostname": "",
        "dir": "",
        "cpu_count": None,
        "total_memory": 0,
        "drain_time": float("inf"),
    }
    return msgspec.convert({**defaults, **fields}, m.Registration)


def _task(obj: object) -> m.Task:
    # special handling because Task and friends subclass TypedDict instead of Message
    if not isinstance(obj, dict):
        raise ValueError(f"expected a dict, got {type(obj).__name__}")
    task_id, context, buffer = obj["task_id"], obj["context"], obj["buffer"]
    if type(task_id) is not int:
        raise ValueError(f"expected an int task_id, got {type(task_id).__name__}")
    if not isinstance(context, dict):
        raise ValueError(f"expected a dict context, got {type(context).__name__}")
    if not isinstance(buffer, bytes):
        raise ValueError(f"expected a bytes buffer, got {type(buffer).__name__}")
    return m.Task(task_id=task_id, context=cast(m.TaskContext, context), buffer=buffer)
