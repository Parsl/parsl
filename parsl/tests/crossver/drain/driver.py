import pathlib
import sys
import time

from parsl.executors import HighThroughputExecutor

from .. import util

# long enough that the manager can only leave by draining, not by missing heartbeats
with util.load(pathlib.Path(sys.argv[1]), drain_period=10, heartbeat_threshold=3600) as dfk:
    htex = dfk.executors["HighThroughputExecutor"]
    assert isinstance(htex, HighThroughputExecutor)
    while not htex.connected_managers():
        time.sleep(0.1)
    while htex.connected_managers():
        time.sleep(0.1)
