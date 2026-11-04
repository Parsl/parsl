import pickle
from typing import Any, Callable, Dict, Mapping, Type, TypeVar, cast

import msgspec

from parsl.executors.high_throughput.protocol import messages as m
from parsl.executors.high_throughput.protocol.errors import ProtocolError

HEARTBEAT_CODE = (2 ** 32) - 1
SHUTDOWN_CODE = (2 ** 32) - 2

T = TypeVar("T")


def encode(message: m.Message) -> bytes:
    if isinstance(message, m.ConnectionProbeReply):
        return b""
    return pickle.dumps(_to_legacy(message))


def _task_dict(task: m.Task) -> Dict[str, Any]:
    return {"task_id": task.task_id, "context": task.context, "buffer": task.buffer}


def _to_legacy(message: m.Message) -> Any:
    match message:
        case m.Task():
            return _task_dict(message)
        case m.Tasks(tasks=tasks):
            return [_task_dict(t) for t in tasks]
        case m.Result(task_id=task_id, result=result, exception=exception):
            d: Dict[str, Any] = {"type": "result", "task_id": task_id}
            if result is not None:
                d["result"] = result
            if exception is not None:
                d["exception"] = exception
            return d
        case m.ConnectionProbe():
            return {"type": "connection_probe"}
        case m.Registration():
            return {
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
        case m.Heartbeat():
            return {"type": "heartbeat"}
        case m.Drain():
            return {"type": "drain"}
        case m.Monitoring(payload=payload):
            return {"type": "monitoring", "payload": pickle.loads(payload)}
        case m.HeartbeatReply():
            return HEARTBEAT_CODE
        case m.Shutdown():
            return SHUTDOWN_CODE
        case m.WorkerBinds():
            return "WORKER_BINDS"
        case m.ConnectedBlocks():
            return "CONNECTED_BLOCKS"
        case m.Managers():
            return "MANAGERS"
        case m.ManagersPackages():
            return "MANAGERS_PACKAGES"
        case m.HoldManager(manager_id=manager_id):
            return ["HOLD_MANAGER", manager_id]
        case m.WorkerBindsReply(port=port):
            return port
        case m.ConnectedBlocksReply(block_ids=block_ids):
            return block_ids
        case m.ManagersReply(managers=managers):
            return managers
        case m.ManagersPackagesReply(packages=packages):
            return packages
        case m.HoldManagerReply() | m.CommandError():
            return None
    raise TypeError(f"{type(message).__name__} has no protocol version 0 form")


def decode(data: bytes, channel: m.Channel[T]) -> T:
    message: m.Message
    try:
        match channel.type:
            case m.Task:
                message = _task(pickle.loads(data))
            case m.Result:
                message = _tagged(data, {"result": _convert(m.Result)})
            case m.AnyCommand:
                message = _command(data)
            case m.ManagerToInterchange:
                message = _tagged(data, {
                    "connection_probe": _convert(m.ConnectionProbe),
                    "registration": _registration,
                    "heartbeat": _convert(m.Heartbeat),
                    "result": _convert(m.Result),
                    "monitoring": _monitoring,
                    "drain": _convert(m.Drain),
                })
            case m.InterchangeToManager:
                message = _interchange_to_manager(data)
            case m.WorkerToManager:
                message = _tagged(data, {"result": _convert(m.Result), "monitoring": _monitoring})
            case m.WorkerBindsReply:
                message = _reply(data, m.WorkerBindsReply, "port")
            case m.ConnectedBlocksReply:
                message = _reply(data, m.ConnectedBlocksReply, "block_ids")
            case m.ManagersReply:
                message = _reply(data, m.ManagersReply, "managers")
            case m.ManagersPackagesReply:
                message = _reply(data, m.ManagersPackagesReply, "packages")
            case m.HoldManagerReply:
                message = _hold_manager_reply(data)
            case _:
                raise ProtocolError(f"Protocol version 0 has no channel for {channel.type!r}")
    except ProtocolError:
        raise
    except Exception as e:
        raise ProtocolError(f"Malformed version 0 message on {channel.name}: {e}") from e
    return cast(T, message)


def _dict(obj: object) -> Dict[str, Any]:
    if not isinstance(obj, dict):
        raise ValueError(f"expected a dict, got {type(obj).__name__}")
    return obj


def _task(obj: object) -> m.Task:
    d = _dict(obj)
    task_id, context, buffer = d["task_id"], d["context"], d["buffer"]
    if type(task_id) is not int:
        raise ValueError(f"expected an int task_id, got {type(task_id).__name__}")
    if not isinstance(context, dict):
        raise ValueError(f"expected a dict context, got {type(context).__name__}")
    if not isinstance(buffer, bytes):
        raise ValueError(f"expected a bytes buffer, got {type(buffer).__name__}")
    return m.Task(task_id=task_id, context=cast(m.TaskContext, context), buffer=buffer)


def _convert(cls: Type[m.Message]) -> Callable[[Dict[str, Any]], m.Message]:
    return lambda d: msgspec.convert(d, cls)


def _registration(d: Dict[str, Any]) -> m.Registration:
    packages = d.get("packages")
    if isinstance(packages, dict):
        named_packages = {name: v for name, v in packages.items() if isinstance(name, str)}
        d = {**d, "packages": named_packages}
    return msgspec.convert(d, m.Registration)


def _monitoring(d: Dict[str, Any]) -> m.Monitoring:
    return m.Monitoring(payload=pickle.dumps(d["payload"]))


def _tagged(data: bytes, table: Mapping[str, Callable[[Dict[str, Any]], m.Message]]) -> m.Message:
    d = _dict(pickle.loads(data))
    tag = d.get("type")
    if not isinstance(tag, str) or tag not in table:
        raise ValueError(f"unexpected message type {tag!r}")
    return table[tag](d)


def _interchange_to_manager(data: bytes) -> m.Message:
    if data == b"":
        return m.ConnectionProbeReply()
    obj = pickle.loads(data)
    if type(obj) is int and obj == HEARTBEAT_CODE:
        return m.HeartbeatReply()
    if type(obj) is int and obj == SHUTDOWN_CODE:
        return m.Shutdown()
    if isinstance(obj, list):
        return m.Tasks(tasks=[_task(t) for t in obj])
    raise ValueError(f"unexpected message {obj!r:.100}")


_COMMAND_NAMES: Dict[str, Type[m.Message]] = {
    "WORKER_BINDS": m.WorkerBinds,
    "CONNECTED_BLOCKS": m.ConnectedBlocks,
    "MANAGERS": m.Managers,
    "MANAGERS_PACKAGES": m.ManagersPackages,
}


def _command(data: bytes) -> m.Message:
    obj = pickle.loads(data)
    if isinstance(obj, str) and obj in _COMMAND_NAMES:
        return _COMMAND_NAMES[obj]()
    if isinstance(obj, list) and len(obj) == 2 and obj[0] == "HOLD_MANAGER":
        return msgspec.convert({"manager_id": obj[1]}, m.HoldManager)
    raise ValueError(f"unknown command {obj!r:.100}")


def _reply(data: bytes, cls: Type[m.Message], field: str) -> m.Message:
    return msgspec.convert({field: pickle.loads(data)}, cls)


def _hold_manager_reply(data: bytes) -> m.Message:
    if (obj := pickle.loads(data)) is not None:
        raise ValueError(f"expected None, got {obj!r:.100}")
    return m.HoldManagerReply()
