import logging
import os

from parsl.utils import RepresentationMixin
from parsl.data_provider.staging import Staging

logger = logging.getLogger(__name__)


class RSyncStaging(Staging, RepresentationMixin):
    """
    This staging provider will execute rsync on worker nodes
    to stage in files from a remote location.

    Worker nodes must be able to authenticate to the rsync server
    without interactive authentication - for example, worker
    initialization could include an appropriate SSH key configuration.

    The submit side will need to run an rsync-compatible server (for example,
    an ssh server with the rsync binary installed)

    """

    def __init__(self, hostname):
        self.hostname = hostname

    def can_stage_in(self, file):
        return file.scheme == "file"

    def can_stage_out(self, file):
        return file.scheme == "file"

    def stage_in(self, dm, executor, file, parent_fut):
        # we need to make path an absolute path, because
        # rsync remote name needs to include absolute path
        file.path = os.path.abspath(file.path)

        working_dir = dm.dfk.executors[executor].working_dir

        if working_dir:
            file.local_path = os.path.join(working_dir, file.filename)
        else:
            file.local_path = file.filename

        return None

    def stage_out(self, dm, executor, file, parent_fut):

        file.path = os.path.abspath(file.path)

        working_dir = dm.dfk.executors[executor].working_dir

        if working_dir:
            file.local_path = os.path.join(working_dir, file.filename)
        else:
            file.local_path = file.filename

        return None

    def replace_task(self, dm, executor, file, f):
        logger.debug("Replacing task for rsync stagein")
        working_dir = dm.dfk.executors[executor].working_dir
        return in_task_stage_in_wrapper(f, file, working_dir, self.hostname)

    def replace_task_stage_out(self, dm, executor, file, f):
        logger.debug("Replacing task for rsync stageout")
        working_dir = dm.dfk.executors[executor].working_dir
        return in_task_stage_out_wrapper(f, file, working_dir, self.hostname)


def in_task_stage_in_wrapper(func, file, working_dir, hostname):
    def wrapper(*args, **kwargs):
        import logging
        logger = logging.getLogger(__name__)
        logger.debug("rsync in_task_stage_in_wrapper start")
        if working_dir:
            os.makedirs(working_dir, exist_ok=True)

        logger.debug("rsync in_task_stage_in_wrapper calling rsync")
        r = os.system("rsync {hostname}:{permanent_filepath} {worker_filepath}".format(hostname=hostname,
                                                                                       permanent_filepath=file.path,
                                                                                       worker_filepath=file.local_path))
        if r != 0:
            raise RuntimeError("rsync returned {}, a {}".format(r, type(r)))
        logger.debug("rsync in_task_stage_in_wrapper calling wrapped function")
        result = func(*args, **kwargs)
        logger.debug("rsync in_task_stage_in_wrapper returned from wrapped function")
        return result
    return wrapper


def in_task_stage_out_wrapper(func, file, working_dir, hostname):
    def wrapper(*args, **kwargs):
        import logging
        logger = logging.getLogger(__name__)
        logger.debug("rsync in_task_stage_out_wrapper start")

        logger.debug("rsync in_task_stage_out_wrapper calling wrapped function")
        result = func(*args, **kwargs)
        logger.debug("rsync in_task_stage_out_wrapper returned from wrapped function, calling rsync")
        r = os.system("rsync {worker_filepath} {hostname}:{permanent_filepath}".format(hostname=hostname,
                                                                                       permanent_filepath=file.path,
                                                                                       worker_filepath=file.local_path))
        if r != 0:
            raise RuntimeError("rsync returned {}, a {}".format(r, type(r)))
        logger.debug("rsync in_task_stage_out_wrapper returned from rsync")
        return result
    return wrapper
