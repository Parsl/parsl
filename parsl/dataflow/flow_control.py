import logging

from typing import Sequence

from parsl.executors.base import ParslExecutor
from parsl.dataflow.job_status_poller import JobStatusPoller
from parsl.utils import Timer

logger = logging.getLogger(__name__)


class FlowControl(Timer):
    """This class periodically makes a callback to the JobStatusPoller
    to give the block scaling strategy a chance to execute.
    """

    def __init__(self, dfk):
        """Initialize the flowcontrol object.

        We start the timer thread here

        Args:
             - dfk (DataFlowKernel) : DFK object to track parsl progress

        """
        self.job_status_poller = JobStatusPoller(dfk)
        callback = self.job_status_poller.poll
        super().__init__(callback, interval=5, name="FlowControl")

    def add_executors(self, executors: Sequence[ParslExecutor]) -> None:
        self.job_status_poller.add_executors(executors)
