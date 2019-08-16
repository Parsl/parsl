import logging

from parsl.app.futures import DataFuture
from parsl.data_provider.files import File
from parsl.data_provider.ftp import _ftp_stage_in_app
from parsl.data_provider.globus import _get_globus_scheme
from parsl.data_provider.http import _http_stage_in_app

logger = logging.getLogger(__name__)


class DataManager(object):
    """The DataManager is responsible for transferring input and output data.

    """

    def __init__(self, dfk):
        """Initialize the DataManager.

        Args:
           - dfk (DataFlowKernel): The DataFlowKernel that this DataManager is managing data for.

        Kwargs:
           - executors (list of Executors): Executors for which data transfer will be managed.
        """

        self.dfk = dfk
        self.globus = None

    def stage_in(self, input, executor):
        """Transport the input from the input source to the executor, if it is file-like,
        returning a DataFuture that wraps the stage-in operation.

        If no staging in is required - because the `file` parameter is not file-like,
        then return that parameter unaltered.

        Args:
            - self
            - input (Any) : input to stage in. If this is a File or a
              DataFuture, stage in tasks will be launched with appropriate
              dependencies. Otherwise, no stage-in will be performed.
            - executor (str) : an executor the file is going to be staged in to.
        """

        if isinstance(input, DataFuture) and input.file_obj.is_remote():
            file = input.file_obj
            parent_fut = input
        elif isinstance(input, File) and input.is_remote():
            file = input
            parent_fut = None
        else:
            return input

        if file.scheme == 'ftp':
            working_dir = self.dfk.executors[executor].working_dir
            stage_in_app = _ftp_stage_in_app(self, executor=executor)
            app_fut = stage_in_app(working_dir, parent_fut=parent_fut, outputs=[file], staging_inhibit_output=True)
            return app_fut._outputs[0]
        elif file.scheme == 'http' or file.scheme == 'https':
            working_dir = self.dfk.executors[executor].working_dir
            stage_in_app = _http_stage_in_app(self, executor=executor)
            app_fut = stage_in_app(working_dir, parent_fut=parent_fut, outputs=[file], staging_inhibit_output=True)
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
            app_fut = stage_in_app(parent_fut=parent_fut, outputs=[file], staging_inhibit_output=True)
            return app_fut._outputs[0]
        else:
            raise Exception('Staging in with unknown file scheme {} is not supported'.format(file.scheme))

    def stage_out(self, file, executor, app_fu):
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
            globus_scheme._update_stage_out_local_path(file, executor, self.dfk)
            stage_out_app = globus_scheme._globus_stage_out_app(executor=executor, dfk=self.dfk)
            return stage_out_app(app_fu, inputs=[file])
        else:
            raise Exception('Staging out with unknown file scheme {} is not supported'.format(file.scheme))
