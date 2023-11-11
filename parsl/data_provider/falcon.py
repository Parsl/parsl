import logging
from functools import partial
import subprocess
import zmq
import json

from parsl.app.app import python_app
from parsl.data_provider.staging import Staging
from parsl.utils import RepresentationMixin

# Initialize the logger
logger = logging.getLogger(__name__)


class FalconStaging(Staging, RepresentationMixin):

    def can_stage_in(self, directory):
        """
        Returns True if the input directory can be staged in, False otherwise.
        """
        logger.debug("Falcon checking directory {}".format(repr(directory)))
        return directory.scheme == 'falcon'

    def stage_in(self, dm, executor, directory, parent_fut):
        """
        Stages in a directory using Falcon.

        Parameters:
        - dm: DataMover instance
        - executor: the executor to be used
        - directory: the directory to be staged in
        - parent_fut: the future representing the parent task

        Returns:
        - the future representing the staged in directory
        """
        stage_in_app = self._falcon_stage_in_app(executor=executor, dfk=dm.dfk)
        app_fut = stage_in_app(outputs=[directory], _parsl_staging_inhibit=True, parent_fut=parent_fut)
        return app_fut._outputs[0]

    def __init__(self, host_ip):
        """
        Initializes a FalconStaging instance.
        """
        self.host_ip = host_ip

    def _falcon_stage_in_app(self, executor, dfk):
        """
        Returns a Parsl app that stages in a directory using Falcon.

        Parameters:
        - executor: the executor to be used
        - dfk: the data flow kernel

        Returns:
        - a Parsl app that stages in a directory using Falcon
        """
        executor_obj = dfk.executors[executor]
        f = partial(_falcon_stage_in, self, executor_obj)
        return python_app(executors=['_parsl_internal'], data_flow_kernel=dfk)(f)

    def initialize_transfer(self, working_dir, directory):
        zmq_context = zmq.Context()

        # Initialize a REQ socket and connect to the specified netloc
        zmq_socket = zmq_context.socket(zmq.REQ)
        zmq_socket.connect("tcp://" + directory.netloc + ":5555")

        # Define the data to send
        data = {
            "host": self.host_ip,
            "port": directory.query,
            "directory_path": directory.path
        }

        # Convert the data to a JSON string
        json_data = json.dumps(data)

        # Send the JSON data
        zmq_socket.send_string(json_data)
        zmq_socket.close()
        zmq_context.term()

        sender_command = ["falcon", "receiver", "--host", self.host_ip, "--port", directory.query, "--data_dir",
                          working_dir]

        try:
            subprocess.run(sender_command, check=True)
        except subprocess.CalledProcessError as e:
            logger.debug(f"Command failed with exit code {e.returncode}: {e.stderr}")
        except FileNotFoundError:
            logger.debug("The 'falcon' command was not found. Make sure it's installed and in your system's PATH.")


def _falcon_stage_in(provider, executor, parent_fut=None, outputs=[], _parsl_staging_inhibit=True):
    # Initialize the transfer
    directory = outputs[0]
    provider.initialize_transfer(executor.working_dir ,directory)
