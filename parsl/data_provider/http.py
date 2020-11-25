import logging
import os
import requests

from parsl import python_app

from parsl.utils import RepresentationMixin
from parsl.data_provider.staging import Staging

logger = logging.getLogger(__name__)


class HTTPSeparateTaskStaging(Staging, RepresentationMixin):
    """A staging provider that Performs HTTP and HTTPS staging
    as a separate parsl-level task. This requires a shared file
    system on the executor."""

    def can_stage_in(self, file):
        logger.debug("HTTPSeparateTaskStaging checking file {}".format(repr(file)))
        return file.scheme == 'http' or file.scheme == 'https'

    def stage_in(self, dm, executor, file, parent_fut):
        working_dir = dm.dfk.executors[executor].working_dir

        if working_dir:
            file.local_path = os.path.join(working_dir, file.filename)
        else:
            file.local_path = file.filename

        stage_in_app = _http_stage_in_app(dm, executor=executor)
        app_fut = stage_in_app(working_dir, outputs=[file], _parsl_staging_inhibit=True, parent_fut=parent_fut)
        return app_fut._outputs[0]


class HTTPInTaskStaging(Staging, RepresentationMixin):
    """A staging provider that performs HTTP and HTTPS staging
    as in a wrapper around each task. In contrast to
    HTTPSeparateTaskStaging, this provider does not require a
    shared file system."""

    def can_stage_in(self, file):
        logger.debug("HTTPInTaskStaging checking file {}".format(repr(file)))
        return file.scheme == 'http' or file.scheme == 'https'

    def stage_in(self, dm, executor, file, parent_fut):
        working_dir = dm.dfk.executors[executor].working_dir

        if working_dir:
            file.local_path = os.path.join(working_dir, file.filename)
        else:
            file.local_path = file.filename

        return None

    def replace_task(self, dm, executor, file, f):
        working_dir = dm.dfk.executors[executor].working_dir
        return in_task_transfer_wrapper(f, file, working_dir)


def in_task_transfer_wrapper(func, file, working_dir):
    def wrapper(*args, **kwargs):
        import requests
        if working_dir:
            os.makedirs(working_dir, exist_ok=True)
        resp = requests.get(file.url, stream=True)
        with open(file.local_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

        result = func(*args, **kwargs)
        return result
    return wrapper


def _http_stage_in(working_dir, parent_fut=None, outputs=[], _parsl_staging_inhibit=True):
    file = outputs[0]
    if working_dir:
        os.makedirs(working_dir, exist_ok=True)
    resp = requests.get(file.url, stream=True)
    with open(file.local_path, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)


def _http_stage_in_app(dm, executor):
    return python_app(executors=[executor], data_flow_kernel=dm.dfk)(_http_stage_in)
