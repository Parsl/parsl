import logging
import multiprocessing
import threading

logger = logging.getLogger(__name__)


def wrap_with_logs(fn):
    def wrapped(*args, **kwargs):
        print("process logger start")
        logger.info("process logger start")

        process = multiprocessing.current_process()
        thread = threading.current_thread()

        name = "thread {} in process {}".format(thread.name, process.name)

        try:
            print("process logger started for {}".format(name))
            logger.info("process logger started for {}".format(name))
            r = fn(*args, **kwargs)
            print("process logger normal ending for {}".format(name))
            logger.info("process logger normal ending for {}".format(name))
            return r
        except:
            print("process logger exceptional ending for {}".format(name))
            logger.error("process logger exceptional ending for {}".format(name))
            logger.exception("process logger caught an exception for {}".format(name))
            raise
    return wrapped
