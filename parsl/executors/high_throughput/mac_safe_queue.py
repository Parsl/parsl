import multiprocessing
import multiprocessing.queues
import logging

logger = logging.getLogger(__name__)


class MacSafeQueue(multiprocessing.queues.Queue):
    """ Multiprocessing queues do not have qsize attributes on MacOS.
    This is slower but more portable version of the multiprocessing Queue
    that adds a explicit counter

    Reference : https://github.com/keras-team/autokeras/commit/4ddd568b06b4045ace777bc0fb7bc18573b85a75
    """

    def __init__(self, *args, **kwargs):
        if 'ctx' not in kwargs:
            kwargs['ctx'] = multiprocessing.get_context()
        super().__init__(*args, **kwargs)
        self._counter = multiprocessing.Value('i', 0)

    def put(self, *args, **kwargs):
        # logger.critical("Putting item {}".format(args))
        x = super().put(*args, **kwargs)
        with self._counter.get_lock():
            self._counter.value += 1
        return x

    def get(self, *args, **kwargs):
        x = super().get(*args, **kwargs)
        with self._counter.get_lock():
            self._counter.value -= 1
        # logger.critical("Getting item {}".format(x))
        return x

    def qsize(self):
        return self._counter.value

    def empty(self):
        return not self._counter.value


def consumer(in_q, out_q, delay=0):
    import time
    while True:
        print("In loop")
        x = in_q.get()
        time.sleep(delay)
        out_q.put(x)
        if x == 'STOP':
            break


if __name__ == "__main__":

    task_q = MacSafeQueue()
    result_q = MacSafeQueue()

    p = multiprocessing.Process(target=consumer, args=(task_q, result_q,))
    p.start()
    for i in range(10):
        print(f"Putting {i}")
        task_q.put(i)
        x = result_q.get()
        print(f"Got {x}")
    task_q.put('STOP')
    p.terminate()
