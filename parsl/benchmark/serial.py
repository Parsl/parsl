import parsl
import time

import parsl.trace as pt

# reuse config loading from parsl/tests/conftest.py
from parsl.tests.configs.htex_local_alternate import fresh_config


def event(descr):
    print(f"{time.time()} {descr}")


@parsl.python_app
def noop_app():
    pass


def run_tasks(n):
    for n in range(0, n_warmup):
        event("submit task " + str(n))
        f = noop_app()
        event("waiting task " + str(n))
        f.result()
        event("got_result task " + str(n))


if __name__ == "__main__":

    pt.trace_by_dict = True

    event("generate_config")

    config = fresh_config()

    event("load_parsl")

    parsl.load(config)

    event("begin_run")

    n_warmup = 1000
    run_tasks(n_warmup)
    event("end_run")

    pt.output_event_stats()
