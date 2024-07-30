from abc import ABCMeta
from dataclasses import dataclass


class RetryBehaviour(metaclass=ABCMeta):
    pass


@dataclass
class CompleteWithAlternateValue(RetryBehaviour):
    value: object
