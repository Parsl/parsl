import os
from enum import IntEnum
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class JobState(IntEnum):
    """Defines a set of states that a job can be in"""
    UNKNOWN = 0
    PENDING = 1
    RUNNING = 2
    CANCELLED = 3
    COMPLETED = 4
    FAILED = 5
    TIMEOUT = 6
    HELD = 7

    def __str__(self) -> str:
        return self.__class__.__name__ + "." + self.name


TERMINAL_STATES = [JobState.CANCELLED, JobState.COMPLETED, JobState.FAILED,
                   JobState.TIMEOUT]


class JobStatus:
    """Encapsulates a job state together with other details:

    Args:
        state: The machine-readable state of the job this status refers to
        message: Optional human readable message
        exit_code: Optional exit code
        stdout_path: Optional path to a file containing the job's stdout
        stderr_path: Optional path to a file containing the job's stderr
    """
    SUMMARY_TRUNCATION_THRESHOLD = 2048

    def __init__(self, state: JobState, message: Optional[str] = None, exit_code: Optional[int] = None,
                 stdout_path: Optional[str] = None, stderr_path: Optional[str] = None):
        self.state = state
        self.message = message
        self.exit_code = exit_code
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def status_name(self) -> str:
        return self.state.name

    def __repr__(self) -> str:
        if self.message is not None:
            extra = f"state={self.state} message={self.message}".format(self.state, self.message)
        else:
            extra = f"state={self.state}".format(self.state)
        return f"<{type(self).__module__}.{type(self).__qualname__} object at {hex(id(self))}, {extra}>"

    def __str__(self) -> str:
        if self.message is not None:
            return "{} ({})".format(self.state, self.message)
        else:
            return "{}".format(self.state)

    @property
    def stdout(self) -> Optional[str]:
        return self._read_file(self.stdout_path)

    @property
    def stderr(self) -> Optional[str]:
        return self._read_file(self.stderr_path)

    def _read_file(self, path: Optional[str]) -> Optional[str]:
        if path is None:
            return None
        try:
            with open(path, 'r') as f:
                return f.read()
        except Exception:
            logger.exception("Converting exception to None")
            return None

    @property
    def stdout_summary(self) -> Optional[str]:
        return self._read_summary(self.stdout_path)

    @property
    def stderr_summary(self) -> Optional[str]:
        return self._read_summary(self.stderr_path)

    def _read_summary(self, path: Optional[str]) -> Optional[str]:
        if not path:
            # can happen for synthetic job failures
            return None
        try:
            with open(path, 'r') as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                f.seek(0, os.SEEK_SET)
                if size > JobStatus.SUMMARY_TRUNCATION_THRESHOLD:
                    half_threshold = int(JobStatus.SUMMARY_TRUNCATION_THRESHOLD / 2)
                    head = f.read(half_threshold)
                    f.seek(size - half_threshold, os.SEEK_SET)
                    tail = f.read(half_threshold)
                    return head + '\n...\n' + tail
                else:
                    f.seek(0, os.SEEK_SET)
                    return f.read()
        except FileNotFoundError:
            # When output is redirected to a file, but the process does not produce any output
            # bytes, no file is actually created. This handles that case.
            return None
