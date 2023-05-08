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

    def __init__(self, dfk):
        """Initialize the flowcontrol object.

        We start the timer thread here

        Args:
             - dfk (DataFlowKernel) : DFK object to track parsl progress

        """
        self.interval = 5
        self.cb_args = ()
        self.job_status_poller = JobStatusPoller(dfk)
        self.callback = self.job_status_poller.poll
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


class Timer:
    """This timer is a simplified version of the FlowControl timer.

    This is based on the following logic :

    .. code-block:: none


        BEGIN (INTERVAL, THRESHOLD, callback) :
            start = current_time()

            while (current_time()-start < INTERVAL) :
                 wait()
                 break

            callback()

    """

    def __init__(self, callback, *args, interval=5, name=None):
        """Initialize the Timer object
        We start the timer thread here

        KWargs:
             - interval (int) : number of seconds between callback events
             - name (str) : a base name to use when naming the started thread
        """

        self.interval = interval
        self.cb_args = args
        self.callback = callback
        self._wake_up_time = time.time() + 1

        self._kill_event = threading.Event()
        if name is None:
            name = "Timer-Thread-{}".format(id(self))
        else:
            name = "{}-Timer-Thread-{}".format(name, id(self))
        self._thread = threading.Thread(target=self._wake_up_timer, args=(self._kill_event,), name=name)
        self._thread.daemon = True
        self._thread.start()

    def _wake_up_timer(self, kill_event: threading.Event) -> None:
        """Internal. This is the function that the thread will execute.
        waits on an event so that the thread can make a quick exit when close() is called

        Args:
            - kill_event (threading.Event) : Event to wait on
        """

        # Sleep till time to wake up
        while True:
            prev = self._wake_up_time

            # Waiting for the event returns True only when the event
            # is set, usually by the parent thread
            time_to_die = kill_event.wait(float(max(prev - time.time(), 0)))

            if time_to_die:
                return

            if prev == self._wake_up_time:
                self.make_callback()
            else:
                print("Sleeping a bit more")

    def make_callback(self) -> None:
        """Makes the callback and resets the timer.
        """
        self._wake_up_time = time.time() + self.interval

        try:
            self.callback(*self.cb_args)
        except Exception:
            logger.error("Callback threw an exception - logging and proceeding anyway", exc_info=True)

    def close(self) -> None:
        """Merge the threads and terminate.
        """
        self._kill_event.set()
        self._thread.join()
