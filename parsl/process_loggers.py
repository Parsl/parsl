import logging
import multiprocessing
import threading

logger = logging.getLogger(__name__)


def wrap_with_logs(fn):
    def wrapped(*args, **kwargs):
        print("exception wrapper: start")
        logger.info("exception wrapper: start")
        logger.debug("exception wrapper: about to get current process")

        process = multiprocessing.current_process()
        logger.debug("got current process, about to get thread")
        thread = threading.current_thread()
        logger.debug("got thread, about to form name")

        name = "thread {} in process {}".format(thread.name, process.name)

        try:
            print("exception wrapper: started for {}".format(name))
            logger.info("exception wrapper: started for {}".format(name))
            r = fn(*args, **kwargs)
            print("exception wrapper: normal ending for {}".format(name))
            logger.info("exception wrapper: normal ending for {}".format(name))
            return r
        except:
            print("exception wrapper: exceptional ending for {}".format(name))
            logger.error("exception wrapper: exceptional ending for {}".format(name))
            logger.exception("exception wrapper: caught an exception for {}".format(name))
            raise
    return wrapped
