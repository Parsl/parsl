class WorkerLost(Exception):
    """Exception raised when a worker is lost
    """
    def __init__(self, worker_id, hostname):
        self.worker_id = worker_id
        self.hostname = hostname

    def __repr__(self):
        return (f"Task failure due to loss of worker {self.worker_id} "
                f"on host {self.hostname}")

    def __str__(self):
        return self.__repr__()
