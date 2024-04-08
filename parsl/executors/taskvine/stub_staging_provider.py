import logging

from parsl.utils import RepresentationMixin
from parsl.data_provider.staging import Staging


logger = logging.getLogger(__name__)

known_url_schemes = ["file", "http", "https", "taskvinetemp"]

class StubStaging(Staging, RepresentationMixin):

    def can_stage_in(self, file):
        logger.debug("Task vine staging provider checking passthrough for {}".format(repr(file)))
        return file.scheme in known_url_schemes

    def can_stage_out(self, file):
        logger.debug("Task vine staging provider checking passthrough for {}".format(repr(file)))
        return file.scheme in known_url_schemes
