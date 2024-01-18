import concurrent.futures
import logging
from functools import partial
import subprocess
import zmq
import json
import os

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
  The following example shows creation of a Falcon-accessible file
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
            import zmq
            import subprocess
            import json
            import time

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
            directory_path = data.get("directory_path", "No file path provided")

            # Start the sender
            sender_command = ["falcon", "sender", "--host", host, "--port", port, "--data_dir",
                              directory_path, "--method", "probe"]
            try:
                subprocess.run(sender_command, check=True)
            except subprocess.CalledProcessError as e:
                raise Exception(f"Command failed with exit code {e.returncode}: {e.stderr}")
            except FileNotFoundError:
                raise Exception("The 'falcon' command was not found. Make sure it's installed and in your system's PATH.")

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
        receiver_command = ["falcon", "receiver", "--host", self.host_ip, "--port", directory.query, "--data_dir",
                            working_dir]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit the run_sender_command function with the sender_command as an argument
            future = executor.submit(run_receiver_command, receiver_command)

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

            try:
                # Wait for the command to finish
                future.result()
            except Exception as e:
                # Raise error encountered during the execution of the command
                logger.exception(f"Error during Falcon transfer initialization: {e}")
                raise

def run_receiver_command(receiver_command):
    """
    Run a command using subprocess.run and return the result.

    Parameters:
    - receiver_command (list): The command to be run as a list of strings.

    Returns:
    - CompletedProcess: An object representing the result of the command execution.

    Raises:
    - subprocess.CalledProcessError: If the command returns a non-zero exit code and check=True.
    - Exception: If any other exception occurs during the command execution.
    """
    try:
        result = subprocess.run(receiver_command, check=True)
        return result
    except subprocess.CalledProcessError as e:
        raise Exception(f"Command failed with exit code {e.returncode}: {e.stderr}")
    except FileNotFoundError:
        raise Exception("The 'falcon' command was not found. Make sure it's installed and in your system's PATH.")
    except Exception as e:
        # Handle other exceptions if needed
        raise


def _falcon_stage_in(provider, executor, parent_fut=None, outputs=[], _parsl_staging_inhibit=True):
    # Initialize the transfer
    if executor.working_dir:
        working_dir = os.path.normpath(executor.working_dir)
    else:
        raise ValueError("executor working_dir must be specified for FalconStaging")
    directory = outputs[0]
    provider.initialize_transfer(working_dir, directory)
