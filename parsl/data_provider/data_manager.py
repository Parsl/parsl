import os
import logging
import requests
import ftplib
import concurrent.futures as cf
from parsl.data_provider.scheme import GlobusScheme
from parsl.executors.base import ParslExecutor
from parsl.data_provider.globus import get_globus
from parsl.app.app import python_app

logger = logging.getLogger(__name__)


def _http_stage_in(working_dir, outputs=[]):
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


def _ftp_stage_in(working_dir, outputs=[]):
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


class DataManager(ParslExecutor):
    """The DataManager is responsible for transferring input and output data.

    It uses the Executor interface, where staging tasks are submitted
    to it, and DataFutures are returned.
    """

    @classmethod
    def get_data_manager(cls):
        """Return the DataManager of the currently loaded DataFlowKernel.
        """
        from parsl.dataflow.dflow import DataFlowKernelLoader
        dfk = DataFlowKernelLoader.dfk()

        return dfk.executors['data_manager']

    def __init__(self, dfk, max_threads=10):
        """Initialize the DataManager.

        Args:
           - dfk (DataFlowKernel): The DataFlowKernel that this DataManager is managing data for.

        Kwargs:
           - max_threads (int): Number of threads. Default is 10.
           - executors (list of Executors): Executors for which data transfer will be managed.
        """
        self._scaling_enabled = False

        self.label = 'data_manager'
        self.dfk = dfk
        self.max_threads = max_threads
        self.globus = None
        self.managed = True

    def start(self):
        self.executor = cf.ThreadPoolExecutor(max_workers=self.max_threads)

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
            - block (bool): To block for confirmations or not

        """
        x = self.executor.shutdown(wait=block)
        logger.debug("Done with executor shutdown")
        return x

    @property
    def scaling_enabled(self):
        return self._scaling_enabled

    def initialize_globus(self):
        if self.globus is None:
            self.globus = get_globus()

    def _get_globus_endpoint(self, executor_label):
        if executor_label is None:
            raise ValueError("executor_label is mandatory")
        executor = self.dfk.executors[executor_label]
        if not hasattr(executor, "storage_access"):
            raise ValueError("specified executor does not have storage_access attribute")
        for scheme in executor.storage_access:
            if isinstance(scheme, GlobusScheme):
                if executor.working_dir:
                    working_dir = os.path.normpath(executor.working_dir)
                else:
                    raise ValueError("executor working_dir must be specified for GlobusScheme")
                if scheme.endpoint_path and scheme.local_path:
                    endpoint_path = os.path.normpath(scheme.endpoint_path)
                    local_path = os.path.normpath(scheme.local_path)
                    common_path = os.path.commonpath((local_path, working_dir))
                    if local_path != common_path:
                        raise Exception('"local_path" must be equal or an absolute subpath of "working_dir"')
                    relative_path = os.path.relpath(working_dir, common_path)
                    endpoint_path = os.path.join(endpoint_path, relative_path)
                else:
                    endpoint_path = working_dir
                return {'endpoint_uuid': scheme.endpoint_uuid,
                        'endpoint_path': endpoint_path,
                        'working_dir': working_dir}
        raise Exception('No suitable Globus endpoint defined for executor {}'.format(executor_label))

    def stage_in(self, file, executor):
        """Transport the file from the input source to the executor.

        This function returns a DataFuture.

        Args:
            - self
            - file (File) : file to stage in
            - executor (str) : an executor the file is going to be staged in to.
                                If the executor argument is not specified for a file
                                with 'globus' scheme, the file will be staged in to
                                the first executor with the "globus" key in a config.
        """

        if file.scheme == 'ftp':
            working_dir = self.dfk.executors[executor].working_dir
            stage_in_app = self._ftp_stage_in_app(executor=executor)
            app_fut = stage_in_app(working_dir, outputs=[file])
            return app_fut._outputs[0]
        elif file.scheme == 'http' or file.scheme == 'https':
            working_dir = self.dfk.executors[executor].working_dir
            stage_in_app = self._http_stage_in_app(executor=executor)
            app_fut = stage_in_app(working_dir, outputs=[file])
            return app_fut._outputs[0]
        elif file.scheme == 'globus':
            globus_ep = self._get_globus_endpoint(executor)
            stage_in_app = self._globus_stage_in_app()
            app_fut = stage_in_app(globus_ep, outputs=[file])
            return app_fut._outputs[0]
        else:
            raise Exception('Staging in with unknown file scheme {} is not supported'.format(file.scheme))

    def _ftp_stage_in_app(self, executor):
        return python_app(executors=[executor])(_ftp_stage_in)

    def _http_stage_in_app(self, executor):
        return python_app(executors=[executor])(_http_stage_in)

    def _globus_stage_in_app(self):
        return python_app(executors=['data_manager'])(self._globus_stage_in)

    def _globus_stage_in(self, globus_ep, outputs=[]):
        file = outputs[0]
        file.local_path = os.path.join(
                globus_ep['working_dir'], file.filename)
        dst_path = os.path.join(
                globus_ep['endpoint_path'], file.filename)

        self.initialize_globus()

        self.globus.transfer_file(
                file.netloc, globus_ep['endpoint_uuid'],
                file.path, dst_path)

    def stage_out(self, file, executor):
        """Transport the file from the local filesystem to the remote Globus endpoint.

        This function returns a DataFuture.

        Args:
            - self
            - file (File) - file to stage out
            - executor (str) - Which executor the file is going to be staged out from.
                                If the executor argument is not specified for a file
                                with the 'globus' scheme, the file will be staged in to
                                the first executor with the "globus" key in a config.
        """

        if file.scheme == 'http' or file.scheme == 'https':
            raise Exception('HTTP/HTTPS file staging out is not supported')
        elif file.scheme == 'ftp':
            raise Exception('FTP file staging out is not supported')
        elif file.scheme == 'globus':
            globus_ep = self._get_globus_endpoint(executor)
            stage_out_app = self._globus_stage_out_app()
            return stage_out_app(globus_ep, inputs=[file])
        else:
            raise Exception('Staging out with unknown file scheme {} is not supported'.format(file.scheme))

    def _globus_stage_out_app(self):
        return python_app(executors=['data_manager'])(self._globus_stage_out)

    def _globus_stage_out(self, globus_ep, inputs=[]):
        file = inputs[0]
        src_path = os.path.join(globus_ep['endpoint_path'], file.filename)

        self.initialize_globus()

        self.globus.transfer_file(
            globus_ep['endpoint_uuid'], file.netloc,
            src_path, file.path
        )
