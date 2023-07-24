import logging
import pickle
# import statistics
import time

from typing import Any, List, Tuple

logger = logging.getLogger(__name__)

trace_by_logger = False
trace_by_dict = True

events: List[Tuple[float, str, str, Any]] = []
binds: List[Tuple[str, Any, str, Any]] = []


def event(name: str, spantype: str, spanid: Any):
    """Record an event.
    Using Any for spanid means anything that we can write out in format string
    most concretely a string or an int, but I'm not sure it should be
    concretely tied to only those two types.
    """
    t = time.time()

    if trace_by_logger:
        logger.info(f"EVENT {name} {spantype} {spanid}")

    if trace_by_dict:
        e = (t, name, spantype, spanid)
        events.append(e)


def span_bind_sub(super_spantype: str, super_spanid: Any, sub_spantype: str, sub_spanid: Any):
    if trace_by_logger:
        logger.info(f"BIND {super_spantype} {super_spanid} {sub_spantype} {sub_spanid}")
    if trace_by_dict:
        b = (super_spantype, super_spanid, sub_spantype, sub_spanid)
        binds.append(b)


def output_event_stats(directory="."):
    print("Event stats")
    print("===========")
    print(f"Count of events: {len(events)}")
    print(f"Count of binds: {len(binds)}")

    """
    flats = []
    all_tasks_t = 0
    for ((from_k, to_k), (total, count, raw)) in event_stats.items():
        mean = total / count
        dts = [t - last_t for (last_t, t) in raw]
        t_median = statistics.median(dts)
        t_max = max(dts)
        t_min = min(dts)

        flat = (mean, t_median, t_max, t_min, from_k, to_k, total, count)
        flats.append(flat)

        all_tasks_t += total

    flats.sort()

    for (t_mean, t_median, t_max, t_min, from_k, to_k, total, count) in flats:
        print(f"{from_k} -> {to_k} ({count} iters): min {t_min} / median {t_median} / mean {t_mean} / max {t_max}")

    print("===========")
    print(f"Total real time accounted for here: {all_tasks_t} sec")
    """
    summary = {"events": events, "binds": binds}
    with open(f"{directory}/parsl_tracing.pickle", "wb") as f:
        pickle.dump(summary, f)
