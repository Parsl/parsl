#!/usr/bin/env python3

"""This script is for david adams to experiment with collecting
per-node data - ultimately to log to the parsl monitoring database,
but right now to look at which data fields to collect, and log them
to per-node CSV.

This expects to receive parameters that align with the parsl work queue worker
such as the block ID.
"""

import logging
import platform
import psutil
import subprocess
import sys
import time

from typing import Any
from parsl.log_utils import set_stream_logger

logger = logging.getLogger("parsl.monitoring.davidadams_reporter")


if __name__ == "__main__":

    set_stream_logger()
    logger.info(f"reporter starting, with args {sys.argv}")

    report_prefix = sys.argv[1]

    logger.info(f"will log to prefix {report_prefix}")

    args = sys.argv[2:]
    logger.info(f"reporter launching workers with args {args}")

    set_stream_logger()

    hostname = platform.node()
    csv_filename = report_prefix + "/" + hostname + "." + str(time.time()) + ".csv"

    worker_process = subprocess.Popen(args)

    ret: Any = None

    reading = 0
    with open(csv_filename, "w") as csv_file:
        while ret is None:
            ret = worker_process.poll()
            logger.info("sleeping in poll loop")
            print(f"{time.time()},{reading},{psutil.cpu_percent()}", file=csv_file, flush=True)

            reading += 1
            time.sleep(10)

    logger.info(f"subprocessed ended with return code {ret.returncode}")

    logger.info(f"node reporter ending, passing on return code {ret.returncode} from workers")
    sys.exit(ret.returncode)
