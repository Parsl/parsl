import os
import logging
import concurrent.futures as cf
from parsl.executors.base import ParslExecutor
from parsl.data_provider.globus import get_globus

logger = logging.getLogger(__name__)


class DataManager(ParslExecutor):
    """DataManager uses the familiar Executor interface, where staging tasks are submitted
    to it, and DataFutures are returned.
    """

    """
    In general a site where remote file is going to be stage in is unknown until
    Executer submits an app that depends on the file. However, in most practical
    cases, a site where an app is executed and a file needs to be staged in is
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
        """Initialize the DataManager.

        Kwargs:
           - max_workers (int) : Number of threads (Default=10)
           - config (dict): The config dict object for the site:

        """
        self._scaling_enabled = False
        self.executor = cf.ThreadPoolExecutor(max_workers=max_workers)

        self.config = config
        if not self.config:
            self.config = {"sites": []}
        self.files = []
        self.globus = None

        if not DataManager.get_data_manager():
            DataManager.default_data_manager = self

    def submit(self, *args, **kwargs):
        """Submit a staging request."""
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
                self.globus = get_globus()
            # keep a list of all remote files for optimization purposes (TODO)
            self.files.append(file)
            self._set_local_path(file)

    def _set_local_path(self, file):
        globus_ep = self._get_globus_site()
        file.local_path = os.path.join(
                globus_ep['working_dir'],
                file.filename)

    def _get_globus_site(self, site_name=None):
        for s in self.config['sites']:
            if site_name is None or s['site'] == site_name:
                if 'data' not in s:
                    continue
                data = s['data']
                if 'globus' not in s['data'] or 'working_dir' not in s['data']:
                    continue
                globus_ep = data['globus']
                if 'endpoint_name' not in globus_ep:
                    continue
                endpoint_name = data['globus']['endpoint_name']
                working_dir = os.path.normpath(data['working_dir'])
                if 'endpoint_path' in globus_ep and 'local_path' in globus_ep:
                    endpoint_path = os.path.normpath(globus_ep['endpoint_path'])
                    local_path = os.path.normpath(globus_ep['local_path'])
                    common_path = os.path.commonpath((local_path, working_dir))
                    if local_path != common_path:
                        raise Exception('"local_path" must be equal or an absolute subpath of "working_dir"')
                    relative_path = os.path.relpath(working_dir, common_path)
                    endpoint_path = os.path.join(endpoint_path, relative_path)
                else:
                    endpoint_path = working_dir
                return {'site_name': s['site'],
                        'endpoint_name': endpoint_name,
                        'endpoint_path': endpoint_path,
                        'working_dir': working_dir}
        raise Exception('No site with a Globus endpoint and working_dir defined')

    def stage_in(self, file, site_name=None):
        """Transport the file from the site of origin to the site.

        This function returns a DataFuture.

        Args:
            - self
            - file (File) - file to stage in
            - site_name (str) - a name of a site the file is going to be staged in to.
                                If the site argument is not specified for a file
                                with 'globus' scheme, the file will be staged in to
                                the first site with the "globus" key in a config.
        """

        if file.scheme == 'file':
            site_name = None
        elif file.scheme == 'globus':
            globus_ep = self._get_globus_site(site_name)

        df = file.get_data_future(globus_ep['site_name'])
        if df:
            return df

        if file.scheme == 'file':
            f = self.submit(self._file_transfer_in, file)
        elif file.scheme == 'globus':
            f = self.submit(self._globus_transfer_in, file, globus_ep)

        from parsl.app.futures import DataFuture

        df = DataFuture(f, file)
        file.set_data_future(df, globus_ep['site_name'])
        return df

    def stage_out(self, file, site_name=None):
        """Transport the file from the local filesystem to the remote Globus endpoint.

        This function returns a DataFuture.

        Args:
            - self
            - file (File) - file to stage out
            - site_name (str) - a name of a site the file is going to be staged out from.
                                If the site argument is not specified for a file
                                with 'globus' scheme, the file will be staged in to
                                the first site with the "globus" key in a config.
        """

        if file.scheme == 'file':
            site_name = None
            f = self.submit(self._file_transfer_out)
            return f
        if file.scheme == 'globus':
            globus_ep = self._get_globus_site(site_name)
            f = self.submit(self._globus_transfer_out, file, globus_ep)
            return f

    def _file_transfer_in(self, file):
        pass

    def _globus_transfer_in(self, file, globus_ep):
        file.local_path = os.path.join(
                globus_ep['working_dir'], file.filename)
        dst_path = os.path.join(
                globus_ep['endpoint_path'], file.filename)
        self.globus.transfer_file(
                file.netloc, globus_ep['endpoint_name'],
                file.path, dst_path)

    def _file_transfer_out(self, file):
        pass

    def _globus_transfer_out(self, file, globus_ep):
        src_path = os.path.join(
                globus_ep['endpoint_path'], file.filename)
        self.globus.transfer_file(
                globus_ep['endpoint_name'], file.netloc,
                src_path, file.path)
