import logging
from abc import ABCMeta, abstractmethod
from typing import Optional

_db_manager_excepts: Optional[Exception]

logger = logging.getLogger(__name__)


class MonitoringRadioSender(metaclass=ABCMeta):
    @abstractmethod
    def send(self, message: object) -> None:
        pass
