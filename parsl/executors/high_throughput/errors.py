import time


class ManagerLost(Exception):
    """
    Task lost due to manager loss.  Manager is considered lost when multiple heartbeats
    have been missed.
    """
    def __init__(self, manager_id: bytes, hostname: str) -> None:
        self.manager_id = manager_id
        self.tstamp = time.time()
        self.hostname = hostname

    def __str__(self) -> str:
        return (
            f"Task failure due to loss of manager {self.manager_id.decode()} on"
            f" host {self.hostname}"
        )


class VersionMismatch(Exception):
    """Manager and Interchange versions do not match"""
    def __init__(self, interchange_version: str, manager_version: str):
        self.interchange_version = interchange_version
        self.manager_version = manager_version

    def __str__(self) -> str:
        return (
            f"Manager version info {self.manager_version} does not match interchange"
            f" version info {self.interchange_version}"
        )


class WorkerLost(Exception):
    """Exception raised when a worker is lost
    """
    def __init__(self, worker_id, hostname):
        self.worker_id = worker_id
        self.hostname = hostname

    def __str__(self):
        return "Task failure due to loss of worker {} on host {}".format(self.worker_id, self.hostname)


class CommandClientTimeoutError(Exception):
    """Raised when the command client times out waiting for a response.
    """


class CommandClientBadError(Exception):
    """Raised when the command client is bad from an earlier timeout.
    """
