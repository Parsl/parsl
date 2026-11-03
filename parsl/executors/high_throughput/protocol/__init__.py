"""The messaging protocol between the components of the HighThroughputExecutor.

The executor, interchange, and worker pools may run under different versions
of Python and Parsl, so messages are encoded in a versioned format that does
not depend on either: no pickle. The messages are dataclasses, specified in
:mod:`~.messages`; :mod:`~.schema` converts them to and from bytes::

    from parsl.executors.high_throughput import protocol

    data = protocol.Result(task_id=1, result=b"...").to_wire()
    match protocol.from_wire(data):
        case protocol.Result(task_id=tid):
            ...
"""
# Every public name in messages is exported, so a new message needs no line
# here. (No __all__: it would hide the star-imported names from mypy, and the
# `X as X` form re-exports the rest under mypy's no_implicit_reexport.)
from parsl.executors.high_throughput.protocol.errors import (
    ProtocolError as ProtocolError,
)
from parsl.executors.high_throughput.protocol.errors import (
    UnsupportedProtocolVersion as UnsupportedProtocolVersion,
)
from parsl.executors.high_throughput.protocol.messages import *
from parsl.executors.high_throughput.protocol.schema import Message as Message
from parsl.executors.high_throughput.protocol.schema import from_wire as from_wire
