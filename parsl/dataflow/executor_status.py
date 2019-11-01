from abc import ABCMeta, abstractmethod
from typing import Dict, Any

import parsl  # noqa F401


class ExecutorStatus(metaclass=ABCMeta):
    @property
    @abstractmethod
    def executor(self) -> "parsl.executors.base.ParslExecutor":
        pass

    @property
    @abstractmethod
    def status(self) -> Dict[Any, "parsl.providers.provider_base.JobStatus"]:
        pass
