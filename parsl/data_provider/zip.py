import logging
import os
import zipfile
from typing import Tuple

import filelock

import parsl
from parsl.data_provider.files import File
from parsl.data_provider.staging import Staging
from parsl.errors import ParslError

logger = logging.getLogger(__name__)


class ZipAuthorityError(ParslError):
    def __init__(self, file):
        self.file = file

    def __str__(self):
        return f"ZipFileStaging cannot stage Files with an authority (netloc) section ({self.file.netloc}), for {self.file.url}"


class ZipFileStaging(Staging):
    """A stage-out provider for zip files.

    This provider will stage out files by writing them into the specified zip
    file.

    The filename of both the zip file and the file contained in that zip are
    specified using a zip: URL, like this:

    zip:/tmp/foo/this.zip/inside/here.txt

    This URL names a zip file ``/tmp/foo/this.zip`` containing a file
    ``inside/here.txt``.

    The provider will use the Python filelock package to lock the zip file so
    that it does not conflict with other instances of itself. This lock will
    not protect against other modifications to the zip file.
    """

    def can_stage_out(self, file: File) -> bool:
        return self.is_zip_url(file)

    def can_stage_in(self, file: File) -> bool:
        return self.is_zip_url(file)

    def is_zip_url(self, file: File) -> bool:
        logger.debug("archive provider checking File {}".format(repr(file)))

        # First check if this is the scheme we care about
        if file.scheme != "zip":
            return False

        # This is some basic validation to check that the user isn't specifying
        # an authority section and expecting it to mean something.
        if file.netloc != "":
            raise ZipAuthorityError(file)

        # If we got this far, we can stage this file
        return True

    def stage_out(self, dm, executor, file, parent_fut):
        assert file.scheme == 'zip'

        zip_path, inside_path = zip_path_split(file.path)

        working_dir = dm.dfk.executors[executor].working_dir

        if working_dir:
            file.local_path = os.path.join(working_dir, inside_path)

            # TODO: I think its the right behaviour that a staging out provider should create the directory structure
            # for the file to be placed in?
            os.makedirs(os.path.dirname(file.local_path), exist_ok=True)
        else:
            raise RuntimeError("zip file staging requires a working_dir to be specified")

        stage_out_app = _zip_stage_out_app(dm)
        app_fut = stage_out_app(zip_path, inside_path, working_dir, inputs=[file], _parsl_staging_inhibit=True, parent_fut=parent_fut)
        return app_fut

    def stage_in(self, dm, executor, file, parent_fut):
        assert file.scheme == 'zip'

        zip_path, inside_path = zip_path_split(file.path)

        working_dir = dm.dfk.executors[executor].working_dir

        if working_dir:
            file.local_path = os.path.join(working_dir, inside_path)

        stage_in_app = _zip_stage_in_app(dm)
        app_fut = stage_in_app(zip_path, inside_path, working_dir, outputs=[file], _parsl_staging_inhibit=True, parent_fut=parent_fut)
        return app_fut._outputs[0]


def _zip_stage_out(zip_file, inside_path, working_dir, parent_fut=None, inputs=[], _parsl_staging_inhibit=True):
    file = inputs[0]

    os.makedirs(os.path.dirname(zip_file), exist_ok=True)

    with filelock.FileLock(zip_file + ".lock"):
        with zipfile.ZipFile(zip_file, mode='a', compression=zipfile.ZIP_DEFLATED) as z:
            z.write(file, arcname=inside_path)

    os.remove(file)


def _zip_stage_out_app(dm):
    return parsl.python_app(executors=['_parsl_internal'], data_flow_kernel=dm.dfk)(_zip_stage_out)


def _zip_stage_in(zip_file, inside_path, working_dir, *, parent_fut, outputs, _parsl_staging_inhibit=True):
    with filelock.FileLock(zip_file + ".lock"):
        with zipfile.ZipFile(zip_file, mode='r') as z:
            content = z.read(inside_path)
        with open(outputs[0], "wb") as of:
            of.write(content)


def _zip_stage_in_app(dm):
    return parsl.python_app(executors=['_parsl_internal'], data_flow_kernel=dm.dfk)(_zip_stage_in)


def zip_path_split(path: str) -> Tuple[str, str]:
    """Split zip: path into a zipfile name and a contained-file name.
    """
    index = path.find(".zip/")

    zip_path = path[:index + 4]
    inside_path = path[index + 5:]

    return (zip_path, inside_path)
