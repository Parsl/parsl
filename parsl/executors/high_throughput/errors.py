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


class IncompatibleManager(Exception):
    """Manager cannot communicate with the Interchange"""
    def __init__(self, manager_id: bytes, reason: str):
        self.manager_id = manager_id
        self.reason = reason

    def __str__(self) -> str:
        return f"Manager {self.manager_id.decode()} is incompatible with the interchange: {self.reason}"


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
