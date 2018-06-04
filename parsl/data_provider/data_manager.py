import os
import logging
import requests
import ftplib
import concurrent.futures as cf
from parsl.executors.base import ParslExecutor
from parsl.data_provider.globus import get_globus
from parsl.app.app import App

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
    def get_data_manager(cls, dfk=None, config=None):
        if not cls.default_data_manager or dfk:
            cls.default_data_manager = DataManager(dfk, config=config)
        return cls.default_data_manager

    def __init__(self, dfk, max_workers=10, config=None):
        """Initialize the DataManager.

        Kwargs:
           - max_workers (int) : Number of threads (Default=10)
           - config (dict): The config dict object for the site:

        """
        self._scaling_enabled = False
        self.executor = cf.ThreadPoolExecutor(max_workers=max_workers)

        self.dfk = dfk
        self.config = config
        if not self.config:
            self.config = {"sites": []}
        self.files = []
        self.globus = None

    def submit(self, *args, **kwargs):
        """Submit a staging app. All optimization should be here."""
        return self.executor.submit(*args, **kwargs)

    def scale_in(self, blocks, *args, **kwargs):
        pass

    def scale_out(self, *args, **kwargs):
        pass

    def shutdown(self, block=False):
        """Shutdown the ThreadPool.

        Kwargs:
            - block (Bool): To block for confirmations or not

        """
        x = self.executor.shutdown(wait=block)
        logger.debug("Done with executor shutdown")
        return x

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

    def _get_globus_site(self, site_name='all'):
        if isinstance(site_name, list):
            site_name = site_name[0]
        for s in self.config['sites']:
            if site_name is 'all' or s['site'] == site_name:
                if 'data' not in s:
                    continue
                data = s['data']
                if 'globus' not in data or 'working_dir' not in data:
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

    def _get_working_dir(self, site_name):
        for s in self.config['sites']:
            if s['site'] == site_name:
                if 'data' not in s:
                    break
                data = s['data']
                if 'working_dir' not in data:
                    break
                return os.path.normpath(data['working_dir'])
        return None

    def stage_in(self, file, site_name):
        """Transport the file from the site of origin to the site.

        This function returns a DataFuture.

        Args:
            - self
            - file (File) : file to stage in
            - site_name (str) : a site the file is going to be staged in to.
                                If the site argument is not specified for a file
                                with 'globus' scheme, the file will be staged in to
                                the first site with the "globus" key in a config.
        """

        if file.scheme == 'file':
            stage_in_app = self._file_stage_in_app()
            app_fut = stage_in_app(outputs=[file])
            return app_fut._outputs[0]
        elif file.scheme == 'ftp':
            working_dir = self._get_working_dir(site_name)
            stage_in_app = self._ftp_stage_in_app(site_name=site_name)
            app_fut = stage_in_app(working_dir, outputs=[file])
            return app_fut._outputs[0]
        elif file.scheme == 'http' or file.scheme == 'https':
            working_dir = self._get_working_dir(site_name)
            stage_in_app = self._http_stage_in_app(site_name=site_name)
            app_fut = stage_in_app(working_dir, outputs=[file])
            return app_fut._outputs[0]
        elif file.scheme == 'globus':
            globus_ep = self._get_globus_site(site_name)
            stage_in_app = self._globus_stage_in_app()
            app_fut = stage_in_app(globus_ep, outputs=[file])
            return app_fut._outputs[0]

    def _file_stage_in_app(self):
        return App("python", self.dfk, sites=['data_manager'])(self._file_stage_in)

    def _file_stage_in(self, outputs=[]):
        pass

    def _ftp_stage_in_app(self, site_name):
        return App("python", self.dfk, sites=site_name)(self._ftp_stage_in)

    def _ftp_stage_in(self, working_dir, outputs=[]):
        file = outputs[0]
        if working_dir:
            os.makedirs(working_dir, exist_ok=True)
            file.local_path = os.path.join(working_dir, file.filename)
        else:
            file.local_path = file.filename
        with open(file.local_path, 'wb') as f:
            ftp = ftplib.FTP(file.netloc)
            ftp.login()
            ftp.cwd(os.path.dirname(file.path))
            ftp.retrbinary('RETR {}'.format(file.filename), f.write)
            ftp.quit()

    def _http_stage_in_app(self, site_name):
        return App("python", self.dfk, sites=site_name)(self._http_stage_in)

    def _http_stage_in(self, working_dir, outputs=[]):
        file = outputs[0]
        if working_dir:
            os.makedirs(working_dir, exist_ok=True)
            file.local_path = os.path.join(working_dir, file.filename)
        else:
            file.local_path = file.filename
        resp = requests.get(file.url, stream=True)
        with open(file.local_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

    def _globus_stage_in_app(self):
        return App("python", self.dfk, sites=['data_manager'])(self._globus_stage_in)

    def _globus_stage_in(self, globus_ep, outputs=[]):
        file = outputs[0]
        file.local_path = os.path.join(
                globus_ep['working_dir'], file.filename)
        dst_path = os.path.join(
                globus_ep['endpoint_path'], file.filename)
        self.globus.transfer_file(
                file.netloc, globus_ep['endpoint_name'],
                file.path, dst_path)

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
            stage_out_app = self._file_stage_out_app()
            return stage_out_app()
        elif file.scheme == 'http' or file.scheme == 'https':
            raise Exception('FTP file staging out is not supported')
        elif file.scheme == 'ftp':
            raise Exception('HTTP/HTTPS file staging out is not supported')
        elif file.scheme == 'globus':
            globus_ep = self._get_globus_site(site_name)
            stage_out_app = self._globus_stage_out_app()
            return stage_out_app(globus_ep, inputs=[file])

    def _file_stage_out_app(self):
        return App("python", self.dfk, sites=['data_manager'])(self._file_stage_out)

    def _file_stage_out(self):
        pass

    def _globus_stage_out_app(self):
        return App("python", self.dfk, sites=['data_manager'])(self._globus_stage_out)

    def _globus_stage_out(self, globus_ep, inputs=[]):
        file = inputs[0]
        src_path = os.path.join(
                globus_ep['endpoint_path'], file.filename)
        self.globus.transfer_file(
                globus_ep['endpoint_name'], file.netloc,
                src_path, file.path)
