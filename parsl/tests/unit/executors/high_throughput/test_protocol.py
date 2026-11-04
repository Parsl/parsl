import pickle
import struct
from typing import Any, Tuple

import msgspec
import pytest

from parsl.executors.high_throughput import protocol as proto
from parsl.executors.high_throughput.protocol import (
    ProtocolError,
    UnsupportedProtocolVersion,
    legacy,
    messages,
)


def _subclasses(cls):
    for sub in cls.__subclasses__():
        yield sub
        yield from _subclasses(sub)


MESSAGES = {
    c for c in _subclasses(proto.Message)
    if c.__module__ == messages.__name__
}
CHANNEL_MESSAGES = {
    proto.INTERCHANGE_TO_EXECUTOR: {proto.Result, proto.InterchangeFailure},
    proto.MANAGER_TO_INTERCHANGE: {proto.ConnectionProbe, proto.Registration, proto.Heartbeat, proto.Result, proto.Monitoring, proto.Drain},
    proto.INTERCHANGE_TO_MANAGER: {proto.ConnectionProbeReply, proto.HeartbeatReply, proto.Tasks, proto.Shutdown},
}

CTXT: Any = {
    "task_executor": {"f": "a.b.c", "a": ("x", 1, None), "k": {"k": [1.5, {"n": True}]}},
    "resource_spec": {"priority": 3, "num_nodes": 2, "launcher_options": "--exclusive"},
    "not_a_parsl_key": object.__name__,
}

REG_FIELDS: Any = {
    "parsl_v": "2026.10.26",
    "python_v": "3.12.4",
    "packages": {"parsl": "2026.10.26"},
    "worker_count": 4,
    "uid": "abc123",
    "block_id": "0",
    "start_time": 1234.5,
    "drain_time": float("inf"),
    "prefetch_capacity": 0,
    "max_capacity": 4,
    "os": "Linux",
    "hostname": "node01",
    "dir": "/home/user",
    "cpu_count": None,
    "total_memory": 1 << 30,
}

_monitoring = ("NODE_INFO", {"hostname": "node01", "run_id": "abc"})

LEGACY_EXAMPLES: Tuple[Tuple[proto.Message, proto.Channel[Any], bytes], ...] = (
    (proto.Tasks(tasks=[proto.Task(task_id=1, context={}, buffer=b"a"), proto.Task(task_id=2, context=CTXT, buffer=b"b")]),
     proto.INTERCHANGE_TO_MANAGER,
     pickle.dumps([{"task_id": 1, "context": {}, "buffer": b"a"}, {"task_id": 2, "context": CTXT, "buffer": b"b"}])),
    (proto.Result(task_id=7, result=b"\x00\x80 r"), proto.INTERCHANGE_TO_EXECUTOR,
     pickle.dumps({'type': 'result', 'task_id': 7, 'result': b"\x00\x80 r"})),
    (proto.InterchangeFailure(exception=b"e"), proto.INTERCHANGE_TO_EXECUTOR,
     pickle.dumps({'type': 'result', 'task_id': -1, 'exception': b"e"})),
    (proto.Result(task_id=7, exception=b"e"), proto.MANAGER_TO_INTERCHANGE,
     pickle.dumps({'type': 'result', 'task_id': 7, 'exception': b"e"})),
    (proto.ConnectionProbe(), proto.MANAGER_TO_INTERCHANGE, pickle.dumps({'type': 'connection_probe'})),
    (proto.Registration(**REG_FIELDS), proto.MANAGER_TO_INTERCHANGE,
     pickle.dumps({'type': 'registration', **REG_FIELDS})),
    (proto.Heartbeat(), proto.MANAGER_TO_INTERCHANGE, pickle.dumps({'type': 'heartbeat'})),
    (proto.Drain(), proto.MANAGER_TO_INTERCHANGE, pickle.dumps({'type': 'drain'})),
    (proto.Monitoring(payload=_monitoring), proto.MANAGER_TO_INTERCHANGE,
     pickle.dumps({'type': 'monitoring', 'payload': _monitoring})),
    (proto.ConnectionProbeReply(), proto.INTERCHANGE_TO_MANAGER, b''),
    (proto.HeartbeatReply(), proto.INTERCHANGE_TO_MANAGER, pickle.dumps((2 ** 32) - 1)),
    (proto.Shutdown(), proto.INTERCHANGE_TO_MANAGER, pickle.dumps((2 ** 32) - 2)),
)


@pytest.mark.local
@pytest.mark.parametrize("msg, channel, raw", LEGACY_EXAMPLES)
def test_legacy_round_trip(msg, channel, raw):
    assert legacy.encode(msg) == raw
    got = legacy.decode(raw, channel)
    assert type(got) is type(msg)
    assert got == msg


@pytest.mark.local
@pytest.mark.parametrize("msg, channel, _raw", LEGACY_EXAMPLES)
def test_wire_round_trip(msg, channel, _raw):
    got = proto.decode(msg.encode(), channel)
    assert type(got) is type(msg)
    assert got == msg


@pytest.mark.local
def test_examples_cover_all_messages_and_channels():
    assert {type(msg) for msg, _, _ in LEGACY_EXAMPLES} == MESSAGES
    for channel, expected in CHANNEL_MESSAGES.items():
        assert {type(msg) for msg, c, _ in LEGACY_EXAMPLES if c is channel} == expected


@pytest.mark.local
def test_legacy_task_context_passes_through_unchanged():
    tasks = legacy.decode(legacy.encode(proto.Tasks(tasks=[proto.Task(task_id=1, context=CTXT, buffer=b"")])), proto.INTERCHANGE_TO_MANAGER)
    assert isinstance(tasks, proto.Tasks)
    (got,) = tasks.tasks
    assert got["context"] == CTXT
    assert got["context"]["not_a_parsl_key"] == "object"
    assert type(got["context"]["resource_spec"]["priority"]) is int
    assert got["context"]["task_executor"]["a"] == ("x", 1, None)


@pytest.mark.local
def test_legacy_registration_without_drain_time():
    raw = {'type': 'registration', **REG_FIELDS}
    del raw['drain_time']
    got = legacy.decode(pickle.dumps(raw), proto.MANAGER_TO_INTERCHANGE)
    assert isinstance(got, proto.Registration)
    assert got.drain_time == float("inf")


@pytest.mark.local
def test_legacy_registration_needs_only_versions():
    raw = {'type': 'registration', 'parsl_v': "2016.01.01", 'python_v': "3.5.0"}
    got = legacy.decode(pickle.dumps(raw), proto.MANAGER_TO_INTERCHANGE)
    assert isinstance(got, proto.Registration)
    assert (got.parsl_v, got.python_v) == ("2016.01.01", "3.5.0")


@pytest.mark.local
def test_legacy_ignores_unknown_keys():
    raw = {'type': 'registration', **REG_FIELDS, 'from_the_future': 1}
    assert legacy.decode(pickle.dumps(raw), proto.MANAGER_TO_INTERCHANGE) == proto.Registration(**REG_FIELDS)
    raw = {'type': 'heartbeat', 'from_the_future': 1}
    assert legacy.decode(pickle.dumps(raw), proto.MANAGER_TO_INTERCHANGE) == proto.Heartbeat()


@pytest.mark.local
def test_legacy_checks_channel():
    data = legacy.encode(proto.Heartbeat())
    assert legacy.decode(data, proto.MANAGER_TO_INTERCHANGE) == proto.Heartbeat()
    for channel in (proto.INTERCHANGE_TO_MANAGER, proto.INTERCHANGE_TO_EXECUTOR):
        with pytest.raises(ProtocolError, match=f"on {channel.name}: unexpected message"):
            legacy.decode(data, channel)


@pytest.mark.local
@pytest.mark.parametrize("data, channel", (
    (b"", proto.MANAGER_TO_INTERCHANGE),
    (b"not a pickle", proto.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'heartbeat'})[:-1], proto.MANAGER_TO_INTERCHANGE),
    (pickle.dumps(["not", "a", "dict"]), proto.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({}), proto.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'nope'}), proto.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'monitoring'}), proto.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'result', 'task_id': True}), proto.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'registration'}), proto.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'registration', 'parsl_v': "2016.01.01"}), proto.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'registration', 'python_v': "3.5.0"}), proto.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'registration', **REG_FIELDS, 'packages': {None: "1.0"}}), proto.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'registration', **REG_FIELDS, 'packages': {"broken-dist": None}}), proto.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'result', 'task_id': -1}), proto.INTERCHANGE_TO_EXECUTOR),
    (pickle.dumps(12), proto.INTERCHANGE_TO_MANAGER),
    (pickle.dumps([{"task_id": "1", "context": {}, "buffer": b""}]), proto.INTERCHANGE_TO_MANAGER),
    (pickle.dumps([{"task_id": 1, "context": [], "buffer": b""}]), proto.INTERCHANGE_TO_MANAGER),
    (pickle.dumps([{"task_id": 1, "context": {}, "buffer": "fn"}]), proto.INTERCHANGE_TO_MANAGER),
    (pickle.dumps([{"task_id": 1, "context": {}}]), proto.INTERCHANGE_TO_MANAGER),
))
def test_legacy_malformed(data, channel):
    with pytest.raises(ProtocolError, match="Malformed legacy message") as pyt_e:
        legacy.decode(data, channel)
    assert pyt_e.value.__cause__ is not None


@pytest.mark.local
def test_legacy_rejects_unknown_channel():
    with pytest.raises(ProtocolError, match="does not exist in legacy protocol"):
        legacy.decode(pickle.dumps(1), proto.Channel[int]("ints"))


@pytest.mark.local
def test_peek_version():
    assert proto.peek_version(legacy.encode(proto.Heartbeat())) == "legacy"
    assert proto.peek_version(legacy.encode(proto.ConnectionProbeReply())) == "legacy"
    assert proto.peek_version(proto.MAGIC + struct.pack("!H", 7) + b"body") == 7
    with pytest.raises(ProtocolError, match="Truncated"):
        proto.peek_version(proto.MAGIC + b"\x00")


@pytest.mark.local
@pytest.mark.parametrize("version", (0, 1, 65535))
def test_decode_rejects_unsupported_versions(version):
    data = proto.MAGIC + struct.pack("!H", version) + msgspec.msgpack.encode({"type": "Heartbeat"})
    with pytest.raises(UnsupportedProtocolVersion, match="supports: legacy$") as pyt_e:
        proto.decode(data, proto.MANAGER_TO_INTERCHANGE)
    assert pyt_e.value.version == version


@pytest.mark.local
def test_decode_rejects_legacy_when_unsupported(monkeypatch):
    monkeypatch.setattr(proto, "WIRE_FORMATS", {2: legacy, 1: legacy})
    with pytest.raises(UnsupportedProtocolVersion, match="version legacy is not supported;.* supports: 1, 2$"):
        proto.decode(legacy.encode(proto.Heartbeat()), proto.MANAGER_TO_INTERCHANGE)


@pytest.mark.local
def test_legacy_is_never_prefixed():
    for pickle_protocol in range(2, pickle.HIGHEST_PROTOCOL + 1):
        assert pickle.dumps({'type': 'heartbeat'}, protocol=pickle_protocol)[:1] == b"\x80"
    for msg, _, _ in LEGACY_EXAMPLES:
        assert not legacy.encode(msg).startswith(proto.MAGIC)


@pytest.mark.local
def test_channels_cover_all_messages():
    assert set().union(*CHANNEL_MESSAGES.values()) == MESSAGES
