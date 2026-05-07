from contextvars import ContextVar
from typing import Optional

forced_executor: ContextVar[Optional[str]] = ContextVar(
    "parsl_forced_executor",
    default=None
)
