from __future__ import annotations

import json
import typing as t
import uuid
from typing import Protocol


class Message(Protocol):
    """ The Message Protocol

    """

    def __getstate__(self) -> t.Dict:
        pass

    def __setstate__(self, state: t.Dict) -> None:
        pass


class TaskMessage(Message):
    """ Task Message"""

    def __init__(self, task_uuid: uuid.UUID):
        self.task_uuid = task_uuid

