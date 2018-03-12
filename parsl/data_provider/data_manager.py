import os
import logging
import concurrent.futures as cf
from parsl.executors.base import ParslExecutor
from parsl.data_provider.globus import getGlobus

logger = logging.getLogger(__name__)


class DataManager(ParslExecutor):
    """ The DataManager uses the familiar Executor interface, where staging tasks are submitted
    to it, and DataFutures are returned.
    """

    """
    In general a site where remote file is going to be stage in is unknown until
    Executer submits an app that depends on the file. However, in most practical
    cases, a site where an app ir executed and a file needs to be staged in is
    known. Such cases should be detected by DataManager to optimize file
    transfers. Possible cases are:
    1. Config defines one site only.
    2. Config defines several sites but all of the sites share the filesystem /
       use the same Globus endpoint.
    3. Config defines several sites with different Globus endpoints but a user
       specified explicitely that apps must be executed on a particular site.
    """

    default_data_manager = None

    @classmethod
    def get_data_manager(cls):
        return cls.default_data_manager

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
        self.files = []
        self.globus = None

        if not DataManager.get_data_manager():
            DataManager.default_data_manager = self

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

    def add_file(self, file):
        if file.scheme == 'globus':
            if not self.globus:
                self.globus = getGlobus()
            # keep a list of all remote files for optimization purposes (TODO)
            self.files.append(file)
            self._set_local_path(file)

    """
    def stage_in(file):
        pass

    def stage_out(file):
        pass
    """

    def stage_in(self, file, site=None):
        ''' The stage_in call transports the file from the site of origin
        to the site. The function returns DataFuture.

        Args:
            - self
            - file (File) - file to stage in
        '''

        if file.scheme == 'globus':
            globus_ep = self._get_globus_endpoint(site)

            file.local_path = os.path.join(
                    globus_ep['local_directory'], file.filename)
            dst_path = os.path.join(
                    globus_ep['endpoint_path'], file.filename)
            self.globus.transfer_file(
                    file.netloc, globus_ep['endpoint_name'],
                    file.path, dst_path)
        file.staged = True
        file.local_path = os.path.join(globus_ep['local_directory'], file.filename)

    def stage_out(self, file, site=None):
        ''' The stage_out call transports the file from local filesystem
        to the remote Globus endpoint. The function return DataFuture.

        Args:
            - self
            - file (File) - file to stage out
        '''
        if file.scheme == 'globus':
            globus_ep = self._get_globus_endpoint(site)
            src_path = os.path.join(
                    globus_ep['endpoint_path'], file.filename)
            self.globus.transfer_file(
                    globus_ep['endpoint_name'], file.netloc,
                    src_path, file.path)
        file.staged = True

    def _set_local_path(self,  file):
        globus_ep = self._get_globus_endpoint()
        file.local_path = os.path.join(
                globus_ep['local_directory'],
                file.filename)

    def _get_globus_endpoint(self, site=None):
        for s in self.config['sites']:
            if site is None or s['site'] == site:
                if 'data' in s:
                    if 'globus' in s['data']:
                        return s['data']['globus']
        raise Exception('No site with a Globus endpoint defined')


if __name__ == "__main__":

    from parsl.data_provider.files import File
    from parsl.app.futures import DataFuture
    dm = DataManager(config={'a': 1})

    f = File("/tmp/a.txt")

    print(type(f), f)
    fut = dm.submit(f.stage_in, "foo")
    df = DataFuture(fut, f, parent=None, tid=None)

    print(df)
