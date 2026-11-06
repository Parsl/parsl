import pickle
import struct
from typing import Any, Tuple, get_args

import msgspec
import pytest

from parsl.executors.high_throughput import protocol
from parsl.executors.high_throughput.protocol import (
    ProtocolError,
    UnsupportedProtocolVersion,
    messages,
    v0,
    wire,
)

P = protocol


def _subclasses(cls):
    for sub in cls.__subclasses__():
        yield sub
        yield from _subclasses(sub)


MESSAGES = {
    c for c in _subclasses(P.Message)
    if c.__module__ == messages.__name__ and c is not P.Command
}
CHANNELS = (
    P.EXECUTOR_TO_INTERCHANGE,
    P.INTERCHANGE_TO_EXECUTOR,
    P.COMMANDS,
    P.MANAGER_TO_INTERCHANGE,
    P.INTERCHANGE_TO_MANAGER,
    P.WORKER_TO_MANAGER,
    *(c._reply_channel for c in get_args(P.AnyCommand)),
)

CTXT: Any = {
    "task_executor": {"f": "a.b.c", "a": ("x", 1, None), "k": {"k": [1.5, {"n": True}]}},
    "resource_spec": {"priority": 3, "num_nodes": 2, "launcher_options": "--exclusive"},
    "not_a_parsl_key": object.__name__,
}

REG_FIELDS: Any = {
    "parsl_v": "2026.10.26",
    "python_v": "3.12.4",
    "packages": {"parsl": "2026.10.26", "broken-dist": None},
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

MANAGER_INFO: Any = {
    'manager': "abc123",
    'block_id': "0",
    'worker_count': 4,
    'tasks': 2,
    'idle_duration': 0.5,
    'active': True,
    'parsl_version': "2026.10.26",
    'python_version': "3.12.4",
    'draining': False,
}

_monitoring = ("NODE_INFO", {"hostname": "node01", "run_id": "abc"})

V0_EXAMPLES: Tuple[Tuple[P.Message, P.Channel[Any], bytes], ...] = (
    (P.Task(task_id=1, context=CTXT, buffer=b"fn"), P.EXECUTOR_TO_INTERCHANGE,
     pickle.dumps({"task_id": 1, "context": CTXT, "buffer": b"fn"})),
    (P.Tasks(tasks=[P.Task(task_id=1, context={}, buffer=b"a"), P.Task(task_id=2, context=CTXT, buffer=b"b")]),
     P.INTERCHANGE_TO_MANAGER,
     pickle.dumps([{"task_id": 1, "context": {}, "buffer": b"a"}, {"task_id": 2, "context": CTXT, "buffer": b"b"}])),
    (P.Result(task_id=7, result=b"\x00\x80 r"), P.INTERCHANGE_TO_EXECUTOR,
     pickle.dumps({'type': 'result', 'task_id': 7, 'result': b"\x00\x80 r"})),
    (P.Result(task_id=-1, exception=b"e"), P.INTERCHANGE_TO_EXECUTOR,
     pickle.dumps({'type': 'result', 'task_id': -1, 'exception': b"e"})),
    (P.Result(task_id=7, exception=b"e"), P.MANAGER_TO_INTERCHANGE,
     pickle.dumps({'type': 'result', 'task_id': 7, 'exception': b"e"})),
    (P.Result(task_id=7, result=b"r"), P.WORKER_TO_MANAGER,
     pickle.dumps({'type': 'result', 'task_id': 7, 'result': b"r"})),
    (P.ConnectionProbe(), P.MANAGER_TO_INTERCHANGE, pickle.dumps({'type': 'connection_probe'})),
    (P.Registration(**REG_FIELDS), P.MANAGER_TO_INTERCHANGE,
     pickle.dumps({'type': 'registration', **REG_FIELDS})),
    (P.Heartbeat(), P.MANAGER_TO_INTERCHANGE, pickle.dumps({'type': 'heartbeat'})),
    (P.Drain(), P.MANAGER_TO_INTERCHANGE, pickle.dumps({'type': 'drain'})),
    (P.Monitoring(payload=pickle.dumps(_monitoring)), P.MANAGER_TO_INTERCHANGE,
     pickle.dumps({'type': 'monitoring', 'payload': _monitoring})),
    (P.Monitoring(payload=pickle.dumps(_monitoring)), P.WORKER_TO_MANAGER,
     pickle.dumps({'type': 'monitoring', 'payload': _monitoring})),
    (P.ConnectionProbeReply(), P.INTERCHANGE_TO_MANAGER, b''),
    (P.HeartbeatReply(), P.INTERCHANGE_TO_MANAGER, pickle.dumps((2 ** 32) - 1)),
    (P.Shutdown(), P.INTERCHANGE_TO_MANAGER, pickle.dumps((2 ** 32) - 2)),
    (P.WorkerBinds(), P.COMMANDS, pickle.dumps("WORKER_BINDS")),
    (P.ConnectedBlocks(), P.COMMANDS, pickle.dumps("CONNECTED_BLOCKS")),
    (P.Managers(), P.COMMANDS, pickle.dumps("MANAGERS")),
    (P.ManagersPackages(), P.COMMANDS, pickle.dumps("MANAGERS_PACKAGES")),
    (P.HoldManager(manager_id="abc123"), P.COMMANDS, pickle.dumps(["HOLD_MANAGER", "abc123"])),
    (P.WorkerBindsReply(port=54321), P.WorkerBinds().reply_channel, pickle.dumps(54321)),
    (P.ConnectedBlocksReply(block_ids=["0", None]), P.ConnectedBlocks().reply_channel, pickle.dumps(["0", None])),
    (P.ManagersReply(managers=[MANAGER_INFO]), P.Managers().reply_channel, pickle.dumps([MANAGER_INFO])),
    (P.ManagersPackagesReply(packages={"abc123": {"parsl": "2026.10.26"}}), P.ManagersPackages().reply_channel,
     pickle.dumps({"abc123": {"parsl": "2026.10.26"}})),
    (P.HoldManagerReply(), P.HoldManager(manager_id="x").reply_channel, pickle.dumps(None)),
)


@pytest.mark.local
@pytest.mark.parametrize("msg, channel, legacy", V0_EXAMPLES)
def test_v0_round_trip(msg, channel, legacy):
    assert msg.to_wire() == legacy
    got = P.from_wire(legacy, channel)
    assert type(got) is type(msg)
    assert got == msg


@pytest.mark.local
def test_examples_cover_all_messages_and_channels():
    assert {type(msg) for msg, _, _ in V0_EXAMPLES} | {P.CommandError} == MESSAGES
    types = [c.type for c in CHANNELS]
    assert len(set(types)) == len(types)
    assert {channel.type for _, channel, _ in V0_EXAMPLES} == set(types)


@pytest.mark.local
def test_v0_command_error_is_none():
    assert P.CommandError().to_wire() == pickle.dumps(None)
    data = P.CommandError().to_wire()
    assert P.from_wire(data, P.HoldManager(manager_id="x").reply_channel) == P.HoldManagerReply()
    with pytest.raises(ProtocolError):
        P.from_wire(data, P.WorkerBinds().reply_channel)


@pytest.mark.local
def test_v0_task_context_passes_through_unchanged():
    got = P.from_wire(P.Task(task_id=1, context=CTXT, buffer=b"").to_wire(), P.EXECUTOR_TO_INTERCHANGE)
    assert got.context == CTXT
    assert got.context["not_a_parsl_key"] == "object"
    assert type(got.context["resource_spec"]["priority"]) is int
    assert got.context["task_executor"]["a"] == ("x", 1, None)


@pytest.mark.local
def test_v0_registration_without_drain_time():
    legacy = {'type': 'registration', **REG_FIELDS}
    del legacy['drain_time']
    got = P.from_wire(pickle.dumps(legacy), P.MANAGER_TO_INTERCHANGE)
    assert isinstance(got, P.Registration)
    assert got.drain_time == float("inf")


@pytest.mark.local
def test_v0_ignores_unknown_keys():
    legacy = {'type': 'registration', **REG_FIELDS, 'from_the_future': 1}
    assert P.from_wire(pickle.dumps(legacy), P.MANAGER_TO_INTERCHANGE) == P.Registration(**REG_FIELDS)
    legacy = {'type': 'heartbeat', 'from_the_future': 1}
    assert P.from_wire(pickle.dumps(legacy), P.MANAGER_TO_INTERCHANGE) == P.Heartbeat()


@pytest.mark.local
def test_v0_registration_drops_unnamed_packages():
    legacy = {'type': 'registration', **REG_FIELDS, 'packages': {None: "1.0", "parsl": "2026.10.26"}}
    got = P.from_wire(pickle.dumps(legacy), P.MANAGER_TO_INTERCHANGE)
    assert isinstance(got, P.Registration)
    assert got.packages == {"parsl": "2026.10.26"}


@pytest.mark.local
def test_from_wire_checks_channel():
    data = P.Heartbeat().to_wire()
    assert P.from_wire(data, P.MANAGER_TO_INTERCHANGE) == P.Heartbeat()
    for channel in (P.INTERCHANGE_TO_MANAGER, P.INTERCHANGE_TO_EXECUTOR, P.WORKER_TO_MANAGER, P.COMMANDS):
        with pytest.raises(ProtocolError, match=f"on {channel.name}: unexpected message|unknown command"):
            P.from_wire(data, channel)


@pytest.mark.local
@pytest.mark.parametrize("data, channel", (
    (b"", P.MANAGER_TO_INTERCHANGE),
    (b"not a pickle", P.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'heartbeat'})[:-1], P.MANAGER_TO_INTERCHANGE),
    (pickle.dumps(["not", "a", "dict"]), P.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({}), P.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'nope'}), P.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'monitoring'}), P.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'result', 'task_id': True}), P.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'result', 'task_id': 1, 'result': "not bytes"}), P.MANAGER_TO_INTERCHANGE),
    (pickle.dumps({'type': 'registration'}), P.MANAGER_TO_INTERCHANGE),
    (pickle.dumps(12), P.INTERCHANGE_TO_MANAGER),
    (pickle.dumps([{"task_id": "1", "context": {}, "buffer": b""}]), P.INTERCHANGE_TO_MANAGER),
    (pickle.dumps({"task_id": 1, "context": [], "buffer": b""}), P.EXECUTOR_TO_INTERCHANGE),
    (pickle.dumps({"task_id": 1, "context": {}, "buffer": "fn"}), P.EXECUTOR_TO_INTERCHANGE),
    (pickle.dumps({"task_id": 1, "context": {}}), P.EXECUTOR_TO_INTERCHANGE),
    (pickle.dumps("MANGERS_PACKAGES"), P.COMMANDS),
    (pickle.dumps(["HOLD_MANAGER"]), P.COMMANDS),
    (pickle.dumps(["HOLD_MANAGER", 5]), P.COMMANDS),
    (pickle.dumps(None), P.WorkerBinds().reply_channel),
    (pickle.dumps(1), P.HoldManager(manager_id="x").reply_channel),
))
def test_from_wire_malformed(data, channel):
    with pytest.raises(ProtocolError, match="Malformed version 0 message") as pyt_e:
        P.from_wire(data, channel)
    assert pyt_e.value.__cause__ is not None


@pytest.mark.local
def test_v0_rejects_unknown_channel():
    with pytest.raises(ProtocolError, match="no channel"):
        P.from_wire(pickle.dumps(1), P.Channel(int, "ints"))


@pytest.mark.local
def test_wire_version():
    assert wire.wire_version(P.Heartbeat().to_wire()) == 0
    assert wire.wire_version(P.ConnectionProbeReply().to_wire()) == 0
    assert wire.wire_version(wire.MAGIC + struct.pack("!H", 7) + b"body") == 7
    with pytest.raises(ProtocolError, match="Truncated"):
        wire.wire_version(wire.MAGIC + b"\x00")


@pytest.mark.local
@pytest.mark.parametrize("version", (0, 1, 65535))
def test_from_wire_rejects_unsupported_versions(version):
    data = wire.MAGIC + struct.pack("!H", version) + msgspec.msgpack.encode({"type": "Heartbeat"})
    with pytest.raises(UnsupportedProtocolVersion, match="supports versions \\[0\\]") as pyt_e:
        P.from_wire(data, P.MANAGER_TO_INTERCHANGE)
    assert pyt_e.value.version == version


@pytest.mark.local
def test_v0_is_never_prefixed():
    for proto in range(2, pickle.HIGHEST_PROTOCOL + 1):
        assert pickle.dumps({'type': 'heartbeat'}, protocol=proto)[:1] == b"\x80"
    for msg, _, _ in V0_EXAMPLES:
        assert not msg.to_wire().startswith(wire.MAGIC)


@pytest.mark.local
def test_commands_know_their_replies():
    assert P.WorkerBinds().reply_channel.type is P.WorkerBindsReply
    assert P.HoldManager(manager_id="x").reply_channel.type is P.HoldManagerReply

    with pytest.raises(TypeError, match="must subclass Command\\[<reply class>\\]"):
        class Unpaired(P.Command):
            pass


@pytest.mark.local
def test_command_subclasses_inherit_reply_channel():
    class HoldManagerSubclass(P.HoldManager):
        pass

    assert HoldManagerSubclass(manager_id="x").reply_channel is P.HoldManager(manager_id="x").reply_channel


@pytest.mark.local
def test_channels_cover_all_messages():
    on_channels = {t for c in CHANNELS for t in (get_args(c.type) or (c.type,))}
    assert P.Command not in on_channels
    assert on_channels | {P.CommandError} == MESSAGES


@pytest.mark.local
def test_channel_types_are_messages():
    for c in CHANNELS:
        assert all(issubclass(t, P.Message) for t in (get_args(c.type) or (c.type,)))
