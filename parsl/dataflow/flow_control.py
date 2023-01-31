import logging
import threading
import time

from typing import Sequence

from parsl.executors.base import ParslExecutor
from parsl.dataflow.job_status_poller import JobStatusPoller

logger = logging.getLogger(__name__)


class FlowControl(object):
    """Implements threshold-interval based flow control.

    The overall goal is to trap the flow of apps from the
    workflow, measure it and redirect it the appropriate executors for
    processing.

    This is based on the following logic:

    .. code-block:: none

        BEGIN (INTERVAL, THRESHOLD, callback) :
            start = current_time()

            while (current_time()-start < INTERVAL) :
                 count = get_events_since(start)
                 if count >= THRESHOLD :
                     break

            callback()

    This logic ensures that the callbacks are activated with a maximum delay
    of ``interval`` for systems with infrequent events as well as systems which would
    generate large bursts of events.

    Once a callback is triggered, the callback generally runs a strategy
    method on the sites available as well asqeuque

    TODO: When the debug logs are enabled this module emits duplicate messages.
    This issue needs more debugging. What I've learnt so far is that the duplicate
    messages are present only when the timer thread is started, so this could be
    from a duplicate logger being added by the thread.
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
        self._event_buffer = []
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
            self.callback(tasks=self._event_buffer)
        except Exception:
            logger.error("Flow control callback threw an exception - logging and proceeding anyway", exc_info=True)
        self._event_buffer = []

    def add_executors(self, executors: Sequence[ParslExecutor]) -> None:
        self.job_status_poller.add_executors(executors)

    def close(self) -> None:
        """Merge the threads and terminate."""
        self._kill_event.set()
        self._thread.join()


class Timer(object):
    """This timer is a simplified version of the FlowControl timer.
    This timer does not employ notify events.

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
        """Initialize the flowcontrol object
        We start the timer thread here

        Args:
             - dfk (DataFlowKernel) : DFK object to track parsl progress

        KWargs:
             - interval (int) : seconds after which timer expires
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
        self.callback(*self.cb_args)

    def close(self) -> None:
        """Merge the threads and terminate.
        """
        self._kill_event.set()
        self._thread.join()
