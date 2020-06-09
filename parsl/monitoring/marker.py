

class MonitorSubprocesses:
    """This is a marker class that executors should inherit from if they
    support process based resource monitoring.

    This will cause the parsl monitoring wrapper to launch a separate
    process to monitor resource usage of the task.

    This style of monitoring is not always appropriate. For example, the
    ThreadPoolExecutor uses one process for all of its tasks, so they
    cannot be distinguished by the monitor wrapper; and threads and
    process launching do not play well together and can result in
    runtime hangs.
    """
    pass
