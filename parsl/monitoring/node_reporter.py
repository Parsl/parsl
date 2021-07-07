#!/usr/bin/env python3

"""This script is for sending a node report to monitoring at the start of a
batch job, immediately before launching a worker process.

This is not needed for the HighThroughputExecutor, which contains node
reporting code internally.

This expects to receive parameters that align with the parsl work queue worker
such as the block ID.
"""

import logging
import platform
import psutil
import subprocess
import sys
import uuid

from datetime import datetime
from parsl.log_utils import set_stream_logger
from parsl.monitoring.monitoring import FilesystemRadio

logger = logging.getLogger("parsl.monitoring.node_reporter")


def send_msg(*, active, uid, radio):

    now = datetime.today()

    msg = {
        'run_id': 'TODO',  # from commandline or environment?
        'hostname': platform.node(),
        'uid': uid,
        'block_id': 'TODO',  # maybe from environment?
        'cpu_count': psutil.cpu_count(logical=False),
        'total_memory': psutil.virtual_memory().total,
        'active': active,
        'worker_count': 1,  # from commandline
        'python_v': "{}.{}.{}".format(sys.version_info.major,
                                      sys.version_info.minor,
                                      sys.version_info.micro),

        # TODO: what's the difference between these two? timestamp is when the
        # message is sent, and last_heartbeat is when we last heard from the
        # worker? in the non-heartbeating case, that probably means set them both
        # to now.
        'timestamp': now,
        'last_heartbeat': now
    }

    logger.debug(f"created message {msg}")

    radio.send(msg)

    # TODO: send dict


if __name__ == "__main__":
    set_stream_logger()
    logger.info(f"node reporter starting, with args {sys.argv}")

    logger.info("initializing radio")

    # initial WQ-specific radio here - perhaps this should be parameterisable?
    # depending on if this will be wq specific and if wq transmission method
    # will be configurable.

    # monitoring_hub_url is unused - could perhaps become a "contact string"
    # that can specify many things? (and the url scheme could then specify
    # the radio protocol?)

    # radios now need to cope with source_id not being a task ID - None here,
    # but could be something else (eg the uid?) - what is it used for anyway?

    # and run_dir needs to be passed in somehow. wq workers probably don't get
    # this by default because they're expecting to live in a different
    # filesystem domain.

    # maybe i should hack this in really badly for now.

    run_dir = "/home/benc/parsl/src/parsl/runinfo/000/"   # TODO at least get the real version of this value, no matter how badly

    radio = FilesystemRadio(monitoring_hub_url="",   # TODO: monitoring_hub_url and source_id real values?
                            source_id=0, run_dir=run_dir)

    uid = str(uuid.uuid4())

    logger.info(f"node reporter uid = {uid}")

    send_msg(active=True, uid=uid, radio=radio)

    args = sys.argv[1:]
    logger.info(f"node reporter launching workqueue with args {args}")

    ret = subprocess.run(args)
    logger.info(f"subprocessed ended with return code {ret.returncode}")

    send_msg(active=False, uid=uid, radio=radio)

    logger.info(f"node reporter ending, passing on return code {ret.returncode}")
    sys.exit(ret.returncode)
