import logging
import pickle
import statistics
import time

from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

# TODO: last_event should be a thread local
last_event = None

trace_by_logger = False
trace_by_dict = False

event_stats: Dict[Tuple[str, str],
                  Tuple[float, float, List[Tuple[float, float]]]
                 ]
event_stats = {}


def event(name: str):
    global last_event
    t = time.time()

    if last_event:
        (last_name, last_t) = last_event
        d_t = t - last_t
        if trace_by_logger:
            logger.info(f"{last_name} took {d_t} seconds; beginning new event {name}")
            # logger.info("%s took %s seconds; beginning new event %s", last_name, d_t, name)
        if trace_by_dict:
            k = (last_name, name)
            if k in event_stats:
                (total, count, raw) = event_stats[k]
                raw.append((last_t, t))
                event_stats[k] = (total + d_t, count + 1, raw)
            else:
                event_stats[k] = (d_t, 1, [(last_t, t)])

    last_event = (name, t)


def output_event_stats():
    print("Event stats")
    print("===========")
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
    with open("parslstats.pickle", "wb") as f:
        pickle.dump(event_stats, f)
