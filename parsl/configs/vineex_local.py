from parsl.config import Config
from parsl.executors import VineExecutor

import uuid

config = Config(
    executors=[
        VineExecutor(
            label="parsl-vine-example",

            # If a project_name is given, then TaskVine will periodically
            # report its status and performance back to the global TaskVine catalog,
            # which can be viewed here:  http://ccl.cse.nd.edu/software/taskvine/status

            # To disable status reporting, comment out the project_name.
            project_name="parsl-vine-" + str(uuid.uuid4()),

            # The port number that TaskVine will listen on for connecting workers.
            port=9123,

            # A shared filesystem is not needed when using TaskVine.
            shared_fs=False
        )
    ]
)
