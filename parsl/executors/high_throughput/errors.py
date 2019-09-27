class WorkerLost(Exception):
    """Exception raised when a worker is lost
    """
    def __init__(self, worker_id, hostname):
        self.worker_id = worker_id
        self.hostname = hostname

    def __repr__(self):
        return "Task failure due to loss of worker {} on host {}".format(self.worker_id, self.hostname)

    def __str__(self):
        return self.__repr__()
