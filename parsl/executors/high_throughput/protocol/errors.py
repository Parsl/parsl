class ProtocolError(Exception):
    """A message could not be encoded or decoded, or does not match the protocol."""


class UnsupportedProtocolVersion(ProtocolError):
    """A message uses a protocol version this side does not understand."""

    def __init__(self, version: int, oldest_supported: int) -> None:
        self.version = version
        self.oldest_supported = oldest_supported

    def __str__(self) -> str:
        return (
            f"HTEX protocol version {self.version} is not supported;"
            f" the oldest supported version is {self.oldest_supported}"
        )
