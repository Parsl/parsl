import logging
from abc import ABCMeta, abstractmethod

logger = logging.getLogger(__name__)


class MonitoringRadioSender(metaclass=ABCMeta):
    @abstractmethod
    def send(self, message: object) -> None:
        pass
