#! /usr/bin/env python3

import sys

from parsl.app.errors import RemoteExceptionWrapper


def name():
    return 'parsl_coprocess'


def run_parsl_task(a, b, c):
    import parsl.executors.workqueue.exec_parsl_function as epf
    try:
        (map_file, function_file, result_file) = (a, b, c)

        try:
            (namespace, function_code, result_name) = epf.load_function(map_file, function_file)
        except Exception:
            raise

        try:
            result = epf.execute_function(namespace, function_code, result_name)
        except Exception:
            raise
    except Exception:
        result = RemoteExceptionWrapper(*sys.exc_info())

    epf.dump_result_to_file(result_file, result)

    return None
