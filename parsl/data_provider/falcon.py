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

"""
To run Falcon data_provider
- Setup your virtual environments on both source and destination server
  and install required python packages.
- On the destination server, you can stage data files using FalconStaging.
  The following example shows creation of a Flacon-accessible file
  e.g. File('falcon://192.168.10.11/parsl/data/file.txt/?8080')
- On the source server(s), you need to run a sender which:
        - accept a JSON object from a zmq on port 5555, this JSON object will include:
            - host: host IP
            - port: port number for the falcon transfer
            - directory_path: Path to the director for the transfer
        - run the command "falcon sender --host 'host' --port 'port' --data_dir
          'directory_path' --method probe"
        - sleep for 0.1 seconds (in order for the receiver instance to run before sender instance)
        e.g.
            zmq_context = zmq.Context()
            zmq_socket = zmq_context.socket(zmq.REP)
            zmq_socket.bind("tcp://*:5555")

            # Wait for the next request from the client
            json_data = zmq_socket.recv_string()
            print("Received JSON data:", json_data)

            # Parse the JSON data
            data = json.loads(json_data)

            # Extract the message and host from the data
            host = data.get("host", "No host provided")
            port = data.get("port", "No port provided")
            file_path = data.get("file_path", "No file path provided")

            # Start the sender
            sender_command = ["falcon", "sender", "--host", host, "--port", port, "--data_dir",
                              file_path, "--method", "probe"]
            print(sender_command)
            time.sleep(0.1)
            try:
                subprocess.run(sender_command, check=True)
            except subprocess.CalledProcessError as e:
                print(f"Command failed with exit code {e.returncode}: {e.stderr}")
            except FileNotFoundError:
                print("The 'falcon' command was not found. Make sure it's installed and in your system's PATH.")

            # Send the response back to the client
            zmq_socket.send_string("Received")

            zmq_socket.close()
            zmq_context.term()

You can learn about Falcon and how it runs here: https://pypi.org/project/falcon-datamover/
"""

class FalconStaging(Staging, RepresentationMixin):
    """
    Specification for accessing data on a remote executor via Falcon.
    In this function the File object is used to represent the directory being transferred.
    FalconStaging requires the declaration of working_dir

    Parameters
    ----------
    host_ip : str
        The IP address for the host running this instance (destination host IP address)

    URL
    ----------
    A falcon url should look something like:
    "falcon:{source_host_IP}{path_to_directory_wanted_to_be_transferred}?{port_for_falcon_transfer}"
    """
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
        """
        All communication through Falcon requires a sender instance to be running
        on the sender host, the sender receives the required argument for falcon
        sender which are populated in the JSON variable data, the sender instance
        needs to:
        - accept a JSON object from a zmq on port 5555, this JSON object will include:
            - host: host IP
            - port: port number for the falcon transfer
            - directory_path: Path to the director for the transfer
        - run the command "falcon sender --host 'host' --port 'port' --data_dir
        'directory_path' --method probe"
        - sleep for 0.1 seconds (in order for the receiver instance to run before sender instance)
        """
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
            raise Exception(f"Command failed with exit code {e.returncode}: {e.stderr}")
        except FileNotFoundError:
            raise Exception("The 'falcon' command was not found. Make sure it's installed and in your system's PATH.")


def _falcon_stage_in(provider, executor, parent_fut=None, outputs=[], _parsl_staging_inhibit=True):
    # Initialize the transfer
    directory = outputs[0]
    provider.initialize_transfer(executor.working_dir, directory)
