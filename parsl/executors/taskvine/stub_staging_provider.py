import logging
from concurrent.futures import Future
from typing import Optional

from parsl.app.futures import DataFuture
from parsl.data_provider.files import File
from parsl.data_provider.staging import Staging
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)

known_url_schemes = ["http", "https", "taskvinetemp"]


class StubStaging(Staging, RepresentationMixin):

    def can_stage_in(self, file):
        logger.debug("Task vine staging provider checking passthrough for {}".format(repr(file)))
        return file.scheme in known_url_schemes

    def can_stage_out(self, file):
        logger.debug("Task vine staging provider checking passthrough for {}".format(repr(file)))
        return file.scheme in known_url_schemes

    def stage_in(self, dm, executor: str, file: File, parent_fut: Optional[Future]) -> Optional[DataFuture]:
        if file.netloc == '':
            file.netloc = file.filename
        if file.scheme in ["taskvinetemp", "https", "http"]:
            file.local_path = file.netloc
        return None

    def stage_out(self, dm, executor: str, file: File, app_fu: Future) -> Optional[Future]:
        if file.netloc == '':
            file.netloc = file.filename
        if file.scheme in ["taskvinetemp", "https", "http"]:
            file.local_path = file.netloc
        return None
