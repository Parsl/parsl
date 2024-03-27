import logging
import os
import parsl
import zipfile

from typing import Tuple

from parsl.data_provider.staging import Staging
from parsl.data_provider.files import File

from threading import Lock

# TODO: this is awkward: we don't want to write to the same zip
# file for writing multiple times at once, but there isn't any
# way in the task-based local thread _parsl_internal executor to
# ask for not blocking a worker vs a lock. So this lock will
# occupy workers each waiting for their own chance... instead of
# perhaps using those _parsl_internal workers for better things.
# There are a few different architectures I can imagine but without
# _parsl_internal resource allocation, they all end up blocking
# workers, I think.
global_zip_staging_lock = Lock()


logger = logging.getLogger(__name__)


class ZipFileStaging(Staging):
    """
    this is a stage-out only provider for zip files.
    that will do something involving staging output files into an
    archive (eg. a zip file) and removing them from the filesystem
    in a separate (_parsl_internal) task.

    need to design a URI scheme for this to represent "here's a zip
    file, and here's the name of a file within in"

    zip:path to zip:path to file inside zip, for example?
    URI scheme in general is very flexible...
    needs to fit into something the stdout/stderr autonaming can do
    (eg append autostuff onto the end of?)

    TODO: DANGER: staging multiple files at once will run multiple
    zip commands and that's probably not process safe... in the
    absence of a semaphore-resource aware _parsl_internal scheduler,
    can do an in-submit-process lock and accept that it'll wait
    a bunch
    """

    def can_stage_out(self, file: File) -> bool:
        """
        stage all file outputs into archive
        """
        logger.debug("archive provider checking File {}".format(repr(file)))
        return file.scheme == 'zip'

    def stage_out(self, dm, executor, file, parent_fut):
        assert file.scheme == 'zip'

        zip_path, inside_path = zip_path_split(file.path)

        working_dir = dm.dfk.executors[executor].working_dir

        # TODO: this probably should include the full relative path
        # inside the zip file: the primary use case for this involves
        # lots of files deliberately spread across directories, so
        # don't want staging to then put everything back into the
        # top level working_dir
        if working_dir:
            file.local_path = os.path.join(working_dir, inside_path)

            # TODO: I think its the right behaviour that a staging out provider should create the directory structure
            # for the file to be placed in?
            os.makedirs(os.path.dirname(file.local_path), exist_ok=True)
        else:
            # TODO: what are the situations where a working_dir is not specified,
            # but we still want to use staging? when the user hasn't set an
            # explicit working_dir? in which case we should use cwd?
            raise RuntimeError("working dir required - BENC")
            file.local_path = file.filename

        stage_out_app = _zip_stage_out_app(dm)
        app_fut = stage_out_app(zip_path, inside_path, working_dir, inputs=[file], _parsl_staging_inhibit=True, parent_fut=parent_fut)
        return app_fut


def _zip_stage_out(zip_file, inside_path, working_dir, parent_fut=None, inputs=[], _parsl_staging_inhibit=True):
    file = inputs[0]

    os.makedirs(os.path.dirname(zip_file), exist_ok=True)

    with global_zip_staging_lock:
        with zipfile.ZipFile(zip_file, mode='a', compression=zipfile.ZIP_DEFLATED) as z:
            z.write(file, arcname=inside_path)

    os.remove(file)


def _zip_stage_out_app(dm):
    return parsl.python_app(executors=['_parsl_internal'], data_flow_kernel=dm.dfk)(_zip_stage_out)


def zip_path_split(path: str) -> Tuple[str, str]:
    """Split zip: path into a zipfile name and a contained-file name.
    """
    index = path.find(".zip/")

    zip_path = path[:index + 4]
    inside_path = path[index + 5:]

    return (zip_path, inside_path)
