import logging

from parsl.channels.base import Channel
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class LocalChannel(Channel, RepresentationMixin):
    pass
