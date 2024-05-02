import logging
import pickle
import time

from typing import Any, List, Tuple

logger = logging.getLogger(__name__)

trace_by_logger = False
trace_by_dict = True

events: List[Tuple[float, str, str, Any]] = []
binds: List[Tuple[str, Any, str, Any]] = []


# the spantype/id will only have uniqueness in the context of an
# enclosing span - but processors of events won't necessarily be
# representing all that uniqueness: for example a log line might
# only talk about TASK 3 even though there can be many task 3s,
# one for each DFK in this process, or many BLOCK 0s, one for each
# scalable executor in each DFK in this process.

class Span:
    def __init__(self, spantype: str, spanid: Any):
        self.spantype = spantype
        self.spanid = spanid


def event(name: str, span: Span):
    """Record an event.
    Using Any for spanid means anything that we can write out in format string
    most concretely a string or an int, but I'm not sure it should be
    concretely tied to only those two types.
    """
    t = time.time()

    if trace_by_logger:
        # human readable
        logger.info(f"Event {name} on {span.spantype} {span.spanid}")

        # machine readable (ideally this format would be very unambiguous about span identities)
        logger.info(f"EVENT {name} {span.spantype} {span.spanid} {span}")

    if trace_by_dict:
        e = (t, name, span.spantype, span.spanid)
        events.append(e)


def span_bind_sub(super: Span, sub: Span):
    if trace_by_logger:
        logger.info(f"BIND {super.spantype} {super.spanid} {sub.spantype} {sub.spanid}")
    if trace_by_dict:
        b = (super.spantype, super.spanid, sub.spantype, sub.spanid)
        binds.append(b)


def output_event_stats(directory="."):
    # TODO: print PID here to help untangle what's happening across
    # forks: I can imagine that being complicated as a partially
    # completed trace buffer is inherited from a parent.
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
