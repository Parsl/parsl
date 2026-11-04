class ProtocolError(Exception):
    pass


class UnsupportedProtocolVersion(ProtocolError):
    def __init__(self, version: int) -> None:
        self.version = version

    def __str__(self) -> str:
        from parsl.executors.high_throughput.protocol.wire import (
            SUPPORTED_VERSIONS,
        )
        return (
            f"HTEX protocol version {self.version} is not supported;"
            f" this copy of Parsl supports versions {sorted(SUPPORTED_VERSIONS)}"
        )
