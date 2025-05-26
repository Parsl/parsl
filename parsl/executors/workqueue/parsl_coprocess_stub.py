#! /usr/bin/env python3

import sys

from parsl.app.errors import RemoteExceptionWrapper


def name():
    return 'parsl_coprocess'


def run_parsl_task(a, b, c, d):
    import parsl.executors.workqueue.exec_parsl_function as epf
    (map_file, function_file, result_file, log_file) = (a, b, c, d)

    with open(log_file, "w") as logfile:
        try:

            (namespace, function_code, result_name) = epf.load_function(map_file, function_file, logfile)
            result = epf.execute_function(namespace, function_code, result_name)

        except Exception:
            result = RemoteExceptionWrapper(*sys.exc_info())

        epf.dump_result_to_file(result_file, result)

    return None
