import logging
import os

from parsl.channels.base import Channel
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class LocalChannel(Channel, RepresentationMixin):
    ''' This is not even really a channel, since opening a local shell is not heavy
    and done so infrequently that they do not need a persistent channel
    '''

    def __init__(self):
        ''' Initialize the local channel. script_dir is required by set to a default.

        KwArgs:
            - script_dir (string): Directory to place scripts
        '''
        self.script_dir = None

    @property
    def script_dir(self):
        return self._script_dir

    @script_dir.setter
    def script_dir(self, value):
        if value is not None:
            value = os.path.abspath(value)
        self._script_dir = value
