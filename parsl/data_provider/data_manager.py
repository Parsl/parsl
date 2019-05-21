import logging
import concurrent.futures as cf
from parsl.executors.base import ParslExecutor
from parsl.data_provider.ftp import _ftp_stage_in_app
from parsl.data_provider.globus import _get_globus_scheme
from parsl.data_provider.http import _http_stage_in_app

logger = logging.getLogger(__name__)


class DataManager(ParslExecutor):
    """The DataManager is responsible for transferring input and output data.

    It uses the Executor interface, where staging tasks are submitted
    to it, and DataFutures are returned.
    """

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

    def stage_in(self, file, executor):
        """Transport the file from the input source to the executor.

        This function returns a DataFuture.

        Args:
            - self
            - file (File) : file to stage in
            - executor (str) : an executor the file is going to be staged in to.
        """

        if file.scheme == 'ftp':
            working_dir = self.dfk.executors[executor].working_dir
            stage_in_app = _ftp_stage_in_app(self, executor=executor)
            app_fut = stage_in_app(working_dir, outputs=[file])
            return app_fut._outputs[0]
        elif file.scheme == 'http' or file.scheme == 'https':
            working_dir = self.dfk.executors[executor].working_dir
            stage_in_app = _http_stage_in_app(self, executor=executor)
            app_fut = stage_in_app(working_dir, outputs=[file])
            return app_fut._outputs[0]
        elif file.scheme == 'globus':
            # what should happen here is...
            # we should acquire the GlobusScheme object that goes with
            # this executor (rather than _get_globus_endpoint)
            # and then ask the scheme to provide the stage_in_app without
            # invocations to that app needing any further globus specific
            # parameters.
            # The longer term path is then that http/ftp also become a
            # Scheme, and we allow each one a chance to inspect the
            # file to see if it can handle it (rather than the data manager
            # having to know about staging)
            # At that point, we wouldn't be looking up the GlobusScheme
            # object explicitly - instead we'd be in possession of it just
            # like the other schemes... and each scheme would know all the
            # parameterisation it needs to.
            # so the three real cases of this if/elif block should
            # look pretty much identical
            globus_scheme = _get_globus_scheme(self.dfk, executor)
            stage_in_app = globus_scheme._globus_stage_in_app(executor=executor, dfk=self.dfk)
            app_fut = stage_in_app(outputs=[file])
            return app_fut._outputs[0]
        else:
            raise Exception('Staging in with unknown file scheme {} is not supported'.format(file.scheme))

    def stage_out(self, file, executor):
        """Transport the file from the local filesystem to the remote Globus endpoint.

        This function returns a DataFuture.

        Args:
            - self
            - file (File) - file to stage out
            - executor (str) - Which executor the file is going to be staged out from.
        """

        if file.scheme == 'http' or file.scheme == 'https':
            raise Exception('HTTP/HTTPS file staging out is not supported')
        elif file.scheme == 'ftp':
            raise Exception('FTP file staging out is not supported')
        elif file.scheme == 'globus':
            globus_scheme = _get_globus_scheme(self.dfk, executor)
            stage_out_app = globus_scheme._globus_stage_out_app(executor=executor, dfk=self.dfk)
            return stage_out_app(inputs=[file])
        else:
            raise Exception('Staging out with unknown file scheme {} is not supported'.format(file.scheme))
