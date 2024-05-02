import os
from glob import glob
import logging
import random
import time

from parsl.dataflow.errors import RundirCreateError

logger = logging.getLogger(__name__)


def make_rundir(path: str, *, max_tries: int = 3) -> str:
    """When a path has not been specified, make the run directory.

    Creates a rundir with the following hierarchy:
        ./runinfo <- Home of all run directories
          |----000
          |----001 <- Directories for each run
          | ....
          |----NNN

    Kwargs:
        - path (str): String path to a specific run dir
    """
    backoff_time_s = random.random()

    os.makedirs(path, exist_ok=True)

    # try_count is 1-based for human readability
    try_count = 1
    while True:

        prev_rundirs = glob("[0-9]*[0-9]", root_dir=path)

        next = max([int(os.path.basename(x)) for x in prev_rundirs] + [-1]) + 1

        current_rundir = os.path.join(path, '{0:03}'.format(next))

        try:
            os.makedirs(current_rundir)
            logger.debug("rundir created: {}", current_rundir)
            return os.path.abspath(current_rundir)
        except FileExistsError:
            logger.warning(f"Could not create rundir {current_rundir} on try {try_count}")

            if try_count >= max_tries:
                raise
            else:
                logger.debug("Backing off {}s", backoff_time_s)
                time.sleep(backoff_time_s)
                backoff_time_s *= 2 + random.random()
                try_count += 1

    # this should never be reached - the above loop should have either returned
    # or raised an exception on the last try
    raise RundirCreateError()
