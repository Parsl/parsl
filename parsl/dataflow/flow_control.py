import sys
import threading
import logging
import time

from parsl.dataflow.strategy import Strategy

logger = logging.getLogger(__name__)


class FlowNoControl(object):
    ''' FlowNoControl implements similar interfaces as FlowControl but
    with null handlers so as to mimic the FlowControl class.

    '''

    def __init__(self, dfk, config, *args, threshold=2, interval=2):
        ''' Initialize the flowcontrol object. This does nothing.

        Args:
             - dfk (DFK object) : DFK object to track parsl progress
             - config (Dict) : Config dict structure

        KWargs:
             - threshold (int) : Tasks after which the callback is triggered
             - interval (int) : seconds after which timer expires
        '''
        pass

    def notify(self, event_id):
        ''' This notifiy fn does nothing
        '''
        pass

    def close(self):
        ''' This close fn does nothing
        '''
        pass


class FlowControl(object):
    '''FlowControl timer is designed to implement threshold-interval based
    flow control. The overall goal is to trap the flow of apps from the
    workflow, measure it and redirect it the appropriate executors for
    processing.

    This is based on the following logic :

    >>> BEGIN (INTERVAL, THRESHOLD, callback) :
    >>>     start = current_time()
    >>>
    >>>     while (current_time()-start < INTERVAL) :
    >>>          count = get_events_since(start)
    >>>          if count >= THRESHOLD :
    >>>              break
    >>>
    >>>     callback()

    This logic ensures that the callbacks are activated with a maximum delay
    of INTERVAL for systems with infrequent events as well as systems which would
    generate large bursts of events.

    Once a callback is triggered, the callback generally runs a strategy
    method on the sites available as well asqeuque


    TODO : When the debug logs are enabled this module emits duplicate messages.
    This issue needs more debugging. What I've learnt so far is that the duplicate
    messages are present only when the timer thread is started, so this could be
    from a duplicate logger being added by the thread.

    '''

    def __init__(self, dfk, config, *args, threshold=20, interval=5):
        ''' Initialize the flowcontrol object
        We start the timer thread here

        Args:
             - dfk (DFK object) : DFK object to track parsl progress
             - config (Dict) : Config dict structure

        KWargs:
             - threshold (int) : Tasks after which the callback is triggered
             - interval (int) : seconds after which timer expires
        '''

        self.dfk = dfk
        self.threshold = threshold
        self.interval = interval
        self.cb_args = args
        self.strategy = Strategy(dfk)
        self.callback = self.strategy.strategize
        self._handle = None
        self._event_count = 0
        self._event_buffer = []
        self._wake_up_time = time.time() + 1
        self._thread = threading.Thread(target=self._wake_up_timer)
        self._thread.daemon = True
        self._thread.start()

    def _wake_up_timer(self):

        # Sleep till time to wake up
        while True:
            prev = self._wake_up_time
            time.sleep(max(prev - time.time(), 0))
            if prev == self._wake_up_time:
                self.make_callback(kind='timer')
            else:
                print("Sleeping a bit more")

    def notify(self, event_id):
        ''' Let the FlowControl system know that there's an event
        '''

        self._event_buffer.extend([event_id])
        self._event_count += 1
        if self._event_count >= self.threshold:
            logger.debug("Eventcount >= threshold")
            self.make_callback(kind="event")

    def make_callback(self, kind=None):
        ''' Makes the callback and resets the timer.
        '''
        self._wake_up_time = time.time() + self.interval
        self.callback(tasks=self._event_buffer, kind=kind)
        self._event_buffer = []

    def close(self):
        ''' Merge the threads and terminate.
        '''
        self._thread.join()


if __name__ == "__main__":

    print("This is broken")

    def cback(*args):
        print("*" * 40)
        print("Callback at {0} with args : {1}".format(time.time(), args))
        print("*" * 40)

    fc = FlowControl(cback)

    print("Testing")
    print("Press E(Enter) to create and event, X(Enter) to exit")
    while True:
        x = sys.stdin.read(1)
        if x.lower() == 'e':
            print("Event")
            fc.notify()
        elif x.lower() == 'x':
            print("Exiting ...")
            break
        else:
            print("Continuing.. got[%s]", x)
