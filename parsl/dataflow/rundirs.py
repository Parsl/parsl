import os
from glob import glob
import logging

logger = logging.getLogger(__name__)


def make_rundir(path: str) -> str:
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
    try:
        if not os.path.exists(path):
            os.makedirs(path)

        prev_rundirs = glob(os.path.join(path, "[0-9]*[0-9]"))

        current_rundir = os.path.join(path, '000')

        if prev_rundirs:
            # Since we globbed on files named as 0-9
            x = sorted([int(os.path.basename(x)) for x in prev_rundirs])[-1]
            current_rundir = os.path.join(path, '{0:03}'.format(x + 1))

        os.makedirs(current_rundir)
        logger.debug("Parsl run initializing in rundir: {0}".format(current_rundir))
        return os.path.abspath(current_rundir)

    except Exception:
        logger.exception("Failed to create run directory")
        raise
