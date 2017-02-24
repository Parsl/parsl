import logging
import subprocess

logger = logging.getLogger(__name__)

from concurrent.futures import Future
from threading import Event

class AppFuture(Future):
    ''' AppFuture extends the Future class to contain
    a list of output futures that are accessible at the return.
    '''
    def __init__ (self, uid):
        super (AppFuture, self).__init__()
        self.uid = uid

    def wait(self, timeout=None):
        '''
        if future has completed : Return the result
        else:
            if wait has exceeded timeout: raise TimeoutError
            else: 
                return Result.
        '''
        return self.result(timeout)


class DataFuture():
    ''' DataFuture contains/points at the AppFuture that generates the outputs for the future behavior.
    The data items     
    '''
    def __init__ (self, app_future, url):
        #super (AppFuture, self).__init__()
        self.app_future = app_future
        self.url = url

    def wait(self, timeout=None):
        '''
        if future has completed : Return the result
        else:
            if wait has exceeded timeout: raise TimeoutError
            else: 
                return Result.
        '''
        return self.app_future.result(timeout)

    def cancel(self):
        return self.app_future.cancel()

    def cancelled(self):
        return self.app_future.cancelled()

    def running(self):
        return self.app_future.running()

    def done(self):
        return self.app_future.done()

    def result(self, timeout=None):
        try:
            r = self.app_future.results(timeout=timeout)
        except concurrent.futures.TimeoutError :
            logger.error("Timeout expired in waiting for {0}".format(self.url))
            raise concurrent.futures.TimeoutError
        return 

    def add_done_callback(self, fn):
        return self.app_future.add_done_callback(fn)



    
