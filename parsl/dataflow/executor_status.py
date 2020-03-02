import parsl

from abc import ABCMeta, abstractmethod
from typing import Dict


class ExecutorStatus(metaclass=ABCMeta):
    @property
    @abstractmethod
    def executor(self) -> "parsl.executors.base.ParslExecutor":
        pass

    @property
    @abstractmethod
    def status(self) -> Dict[object, "parsl.providers.provider_base.JobStatus"]:
        pass
