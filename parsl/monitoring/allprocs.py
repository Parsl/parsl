import pickle
import psutil
import random
import signal
import time

from typing import Any, List

records: List
records = []


def summarize_and_exit(sig: Any, frame: Any) -> None:
    with open("./allprocs.pickle", "wb") as f:
        pickle.dump(records, f)

    raise KeyboardInterrupt


signal.signal(signal.SIGTERM, summarize_and_exit)
signal.signal(signal.SIGINT, summarize_and_exit)


while True:
    pass_start = time.time()

    for p in psutil.process_iter(attrs=['pid', 'name', 'ppid', 'create_time', 'cmdline', 'cpu_times', 'memory_info']):
        timestamp = time.time()
        records.append((timestamp, p.info))

    pass_end = time.time()

    print(pass_end - pass_start)

    time.sleep(3 + random.random() * 7)
