import logging

from parsl.utils import RepresentationMixin
from parsl.data_provider.staging import Staging


logger = logging.getLogger(__name__)


class NoOpFileStaging(Staging, RepresentationMixin):

    def can_stage_in(self, file):
        logger.debug("NoOpFileStaging checking file {}".format(file.__repr__()))
        logger.debug("file has scheme {}".format(file.scheme))
        return file.scheme == 'file'

    def can_stage_out(self, file):
        logger.debug("NoOpFileStaging checking file {}".format(file.__repr__()))
        logger.debug("file has scheme {}".format(file.scheme))
        return file.scheme == 'file'
