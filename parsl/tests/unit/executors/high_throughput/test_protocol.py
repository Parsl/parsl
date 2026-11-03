import json
import struct
from unittest import mock

import pytest

from parsl.executors.high_throughput import protocol
from parsl.executors.high_throughput.protocol import (
    ProtocolError,
    UnsupportedProtocolVersion,
    messages,
    schema,
)
from parsl.executors.high_throughput.protocol.schema import MAGIC


def _task(**kw) -> protocol.Task:
    return protocol.Task(**{"task_id": 1, "context": {}, "buffer": b"fn", **kw})


def _registration(**kw) -> protocol.Registration:
    return protocol.Registration(**{
        "parsl_v": "2026.10.27",
        "python_v": "3.12.4",
        "packages": {"parsl": "2026.10.27", "broken-dist": None},
        "worker_count": 4,
        "uid": "abc123",
        "block_id": "0",
        "start_time": 1234.5,
        "prefetch_capacity": 0,
        "max_capacity": 4,
        "os": "Linux",
        "hostname": "node01",
        "dir": "/home/user",
        "cpu_count": None,
        "total_memory": 1 << 30,
        **kw,
    })


@pytest.mark.local
@pytest.mark.parametrize("msg", (
    _task(),
    _task(context={
        "task_executor": {"f": "a.b.c", "a": ["x", 1, None], "k": {"k": [1.5, {"n": True}]}},
        "resource_spec": {"priority": 3, "num_nodes": 2, "launcher_options": "--exclusive"},
    }),
    protocol.Tasks(tasks=[_task(task_id=1, buffer=b"one"), _task(task_id=2, buffer=b"two")]),
    protocol.Tasks(tasks=[]),
    protocol.Result(task_id=7, result=b"\x00\x80 binary \xff"),
    protocol.Result(task_id=-1, exception=b""),
    protocol.WorkerBinds(),
    protocol.WorkerBindsReply(port=54321),
    protocol.ConnectedBlocks(),
    protocol.ConnectedBlocksReply(block_ids=["0", None]),
    protocol.Managers(),
    protocol.ManagersReply(managers=[]),
    protocol.ManagersReply(managers=[protocol.ManagerInfo(
        manager="abc", block_id=None, worker_count=4, tasks=2, idle_duration=0.5, active=True,
        parsl_version="2026.10.27", python_version="3.12.4", draining=False,
    )]),
    protocol.ManagersPackages(),
    protocol.ManagersPackagesReply(packages={"abc": {"parsl": "2026.10.27", "broken-dist": None}}),
    protocol.HoldManager(manager_id="abc123"),
    protocol.HoldManagerReply(),
    protocol.ConnectionProbe(),
    protocol.ConnectionProbeReply(),
    _registration(),
    _registration(drain_time=float("inf")),
    protocol.Heartbeat(),
    protocol.HeartbeatReply(),
    protocol.Drain(),
    protocol.Shutdown(),
    protocol.Monitoring(payload=b"radio bytes"),
))
def test_round_trip(msg):
    got = protocol.from_wire(msg.to_wire())
    assert type(got) is type(msg)
    assert got == msg


@pytest.mark.local
def test_tuples_decode_as_lists():
    msg = _task(context={"task_executor": {"f": "a.b", "a": (1, (2, 3))}})
    got = protocol.from_wire(msg.to_wire(), protocol.Task)
    assert got.context["task_executor"]["a"] == [1, [2, 3]]


@pytest.mark.local
def test_bytes_are_not_json_encoded():
    payload = b"\x00" * 1024
    data = _task(buffer=payload).to_wire()
    assert data.endswith(payload)
    assert len(data) < len(payload) + 200


@pytest.mark.local
def test_user_data_cannot_impersonate_buffers():
    msg = _task(context={"task_executor": {"f": "a.b", "a": [0, 1], "k": {"buffer": 0}}})
    assert protocol.from_wire(msg.to_wire()) == msg


@pytest.mark.local
def test_wire_prefix():
    data = protocol.Result(task_id=3, result=b"r").to_wire()
    magic, version, header_len = struct.unpack_from("!4sHI", data)
    assert magic == MAGIC
    assert version == schema.PROTOCOL_VERSION == schema.peek_version(data)
    header = json.loads(data[10:10 + header_len])
    assert header == {"m": {"type": "result", "task_id": 3, "result": 0, "exception": None}, "b": [1]}


@pytest.mark.local
def test_optional_fields_may_be_absent():
    data = _frame({"m": {"type": "result", "task_id": 3}, "b": []})
    assert protocol.from_wire(data) == protocol.Result(task_id=3)


@pytest.mark.local
def test_to_wire_rejects_non_json():
    msg = _task(context={"task_executor": {"f": "x", "a": [object()]}})
    with pytest.raises(ProtocolError, match="not JSON serializable"):
        msg.to_wire()


@pytest.mark.local
@pytest.mark.parametrize("buffer", ("not bytes", 5, None))
def test_to_wire_rejects_non_bytes_buffers(buffer):
    # mypy catches these where messages are built; at run time, they must
    # still fail rather than be sent (e.g. bytes(5) would be 5 zero bytes)
    with pytest.raises(TypeError):
        _task(buffer=buffer).to_wire()


def _frame(header: object, *buffers: bytes, version: int = 1, magic: bytes = MAGIC) -> bytes:
    h = json.dumps(header).encode()
    return struct.pack("!4sHI", magic, version, len(h)) + h + b"".join(buffers)


@pytest.mark.local
@pytest.mark.parametrize("data, err", (
    (b"", "too short"),
    (b"\x80\x04\x95\x00\x00\x00\x00\x00\x00\x00", "bad magic"),  # a pickle
    (_frame({}, magic=b"NOPE"), "bad magic"),
))
def test_from_wire_rejects_non_protocol_data(data, err):
    with pytest.raises(ProtocolError, match=err):
        protocol.from_wire(data)


@pytest.mark.local
@pytest.mark.parametrize("data", (
    _frame({"m": {"type": "heartbeat"}, "b": []})[:-1],  # truncated header
    _frame([]),
    _frame({"m": {}, "b": []}),  # no type tag
    _frame({"m": {"type": "heartbeat", "x": 1}, "b": []}),
    _frame({"m": {"type": "monitoring"}, "b": []}),
    _frame({"m": {"type": "monitoring", "payload": 1}, "b": [1]}, b"x"),
    _frame({"m": {"type": "monitoring", "payload": "x"}, "b": []}),
    _frame({"m": {"type": "tasks", "tasks": [[]]}, "b": []}),
))
def test_from_wire_malformed(data):
    with pytest.raises(ProtocolError, match="Malformed message") as pyt_e:
        protocol.from_wire(data)
    assert pyt_e.value.__cause__ is not None


@pytest.mark.local
def test_from_wire_expected_types():
    data = protocol.Heartbeat().to_wire()
    assert protocol.from_wire(data, protocol.MANAGER_TO_INTERCHANGE) == protocol.Heartbeat()
    assert protocol.from_wire(data, protocol.Heartbeat) == protocol.Heartbeat()
    with pytest.raises(ProtocolError, match="Unexpected message type"):
        protocol.from_wire(data, protocol.INTERCHANGE_TO_MANAGER)
    with pytest.raises(ProtocolError, match="Unexpected message type: 'heartbeat'"):
        protocol.from_wire(data, protocol.Result)


@pytest.mark.local
def test_from_wire_unknown_type():
    # an unknown tag is just another unexpected type
    data = _frame({"m": {"type": "nope"}, "b": []})
    with pytest.raises(ProtocolError, match="Unexpected message type: 'nope'"):
        protocol.from_wire(data)


@pytest.mark.local
@pytest.mark.parametrize("data", (
    _frame({"m": {}, "b": []}),  # no type tag
    _frame({"m": {"type": "heartbeat", "x": 1}, "b": []}),  # unknown field
    _frame({"m": {"type": "monitoring", "payload": 0}, "b": []}),  # missing buffer
))
def test_from_wire_other_lookup_errors_are_malformed(data):
    # only the type tag lookup reports an unexpected type
    with pytest.raises(ProtocolError, match="Malformed message"):
        protocol.from_wire(data, protocol.MANAGER_TO_INTERCHANGE)


@pytest.mark.local
def test_unsupported_version():
    old = schema.OLDEST_SUPPORTED_VERSION - 1
    data = _frame({"m": {"type": "heartbeat"}, "b": []}, version=old)
    assert schema.peek_version(data) == old
    with pytest.raises(UnsupportedProtocolVersion) as pyt_e:
        protocol.from_wire(data)
    assert pyt_e.value.version == old


@pytest.mark.local
def test_message_subclasses_need_unique_type_tags():
    with pytest.raises(TypeError, match="must set a `type` tag"):
        class Untagged(protocol.Message):
            pass

    with pytest.raises(TypeError, match="already used by Heartbeat"):
        class Duplicate(protocol.Message):
            type = "heartbeat"


@pytest.mark.local
def test_commands_know_their_replies():
    assert protocol.WorkerBinds.reply_type is protocol.WorkerBindsReply
    assert protocol.HoldManager.reply_type is protocol.HoldManagerReply
    assert protocol.Command not in schema.MESSAGES.values(), "abstract bases are not sent"

    with pytest.raises(ValueError, match="not enough values to unpack"):
        class Unpaired(protocol.Command):
            type = "unpaired"
    assert "unpaired" not in schema.MESSAGES, "a bad command must not be registered"


@pytest.mark.local
def test_command_subclasses_inherit_reply_type():
    # restore the registry afterwards, so the new tag doesn't leak into
    # other tests (e.g. test_channels_cover_all_messages)
    with mock.patch.dict(schema.MESSAGES):
        class HoldManagerSubclass(protocol.HoldManager):
            type = "hold_manager_subclass"

    assert HoldManagerSubclass.reply_type is protocol.HoldManagerReply


@pytest.mark.local
def test_channels_cover_all_messages():
    channel = {
        *{c.reply_type for c in messages.COMMANDS},
        *messages.MANAGER_TO_INTERCHANGE,
        *messages.INTERCHANGE_TO_MANAGER,
        *messages.COMMANDS,
    }
    non_channel = {messages.Task}
    exp = set(schema.MESSAGES.values()) - non_channel

    assert channel == exp
