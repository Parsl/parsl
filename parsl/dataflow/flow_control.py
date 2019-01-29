import sys
import threading
import logging
import time

from parsl.dataflow.strategy import Strategy

logger = logging.getLogger(__name__)


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

    def __init__(self, callback, *args, interval=5):
        """Initialize the flowcontrol object
        We start the timer thread here

        Args:
             - dfk (DataFlowKernel) : DFK object to track parsl progress

        KWargs:
             - threshold (int) : Tasks after which the callback is triggered
             - interval (int) : seconds after which timer expires
        """

        self.interval = interval
        self.cb_args = args
        self.callback = callback
        self._wake_up_time = time.time() + 1

        self._kill_event = threading.Event()
        self._thread = threading.Thread(target=self._wake_up_timer, args=(self._kill_event,))
        self._thread.daemon = True
        self._thread.start()

    def _wake_up_timer(self, kill_event):
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
                self.make_callback(kind='timer')
            else:
                print("Sleeping a bit more")

    def make_callback(self, kind=None):
        """Makes the callback and resets the timer.
        """
        self._wake_up_time = time.time() + self.interval
        self.callback(*self.cb_args)

    def close(self):
        """Merge the threads and terminate.
        """
        self._kill_event.set()
        self._thread.join()


if __name__ == "__main__":

    def foo():
        print("Callback made at :", time.time())

    timer = Timer(foo)

    time.sleep(60)
    timer.close()
    exit(0)

