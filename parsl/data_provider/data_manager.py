import logging
import concurrent.futures as cf
from parsl.executors.base import ParslExecutor

logger = logging.getLogger(__name__)


class DataManager(ParslExecutor):
    """ The DataManager uses the familiar Executor interface, where staging tasks are submitted
    to it, and DataFutures are returned.
    """

    def __init__(self, max_workers=10, config=None):
        ''' Initialize the DataManager

        Kwargs:
           - max_workers (int) : Number of threads (Default=10)
           - config (dict): The config dict object for the site:

        '''

        self._scaling_enabled = False
        self.executor = cf.ProcessPoolExecutor(max_workers=max_workers)

        if not config:
            logger.error("No config provided to DataManager")
            raise Exception(
                "DataManager must be initialized with a valid config. No config provided.")

        self.config = config

    def submit(self, *args, **kwargs):
        ''' Submit a staging request.
        '''
        return self.executor.submit(*args, **kwargs)

    def scale_in(self, blocks, *args, **kwargs):
        pass

    def scale_out(self, *args, **kwargs):
        pass

    def shutdown(self, block=False):
        return self.executor.shutdown(wait=block)

    def scaling_enabled(self):
        return self._scaling_enabled


if __name__ == "__main__":

    from parsl.data_provider.files import File
    from parsl.app.futures import DataFuture
    dm = DataManager(config={'a': 1})

    f = File("/tmp/a.txt")

    print(type(f), f)
    fut = dm.submit(f.stage_in, "foo")
    df = DataFuture(fut, f, parent=None, tid=None)

    print(df)
