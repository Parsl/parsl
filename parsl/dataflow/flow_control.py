import logging
import threading
import time

from typing import Sequence

from parsl.executors.base import ParslExecutor
from parsl.dataflow.job_status_poller import JobStatusPoller

logger = logging.getLogger(__name__)


class FlowControl:
    """This class periodically makes a callback to the JobStatusPoller
    to give the block scaling strategy a chance to execute.
    """

    def __init__(self, dfk, *args, interval=5):
        """Initialize the flowcontrol object.

        We start the timer thread here

        Args:
             - dfk (DataFlowKernel) : DFK object to track parsl progress

        KWargs:
             - interval (int) : seconds after which timer expires
        """
        self.interval = interval
        self.cb_args = args
        self.job_status_poller = JobStatusPoller(dfk)
        self.callback = self.job_status_poller.poll
        self._handle = None
        self._event_count = 0
        self._wake_up_time = time.time() + 1
        self._kill_event = threading.Event()
        self._thread = threading.Thread(target=self._wake_up_timer, args=(self._kill_event,), name="FlowControl-Thread")
        self._thread.daemon = True
        self._thread.start()

    def _wake_up_timer(self, kill_event: threading.Event) -> None:
        """Internal. This is the function that the thread will execute.
        waits on an event so that the thread can make a quick exit when close() is called

        Args:
            - kill_event (threading.Event) : Event to wait on
        """

        while True:
            prev = self._wake_up_time

            # Waiting for the event returns True only when the event
            # is set, usually by the parent thread
            time_to_die = kill_event.wait(float(max(prev - time.time(), 0)))

            if time_to_die:
                return

            self.make_callback()

    def make_callback(self) -> None:
        """Makes the callback and resets the timer.
        """
        self._wake_up_time = time.time() + self.interval
        try:
            self.callback()
        except Exception:
            logger.error("Flow control callback threw an exception - logging and proceeding anyway", exc_info=True)

    def add_executors(self, executors: Sequence[ParslExecutor]) -> None:
        self.job_status_poller.add_executors(executors)

    def close(self) -> None:
        """Merge the threads and terminate."""
        self._kill_event.set()
        self._thread.join()
