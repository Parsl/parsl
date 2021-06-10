from parsl.config import Config
from parsl.executors import WorkQueueExecutor

import uuid

config = Config(
    executors=[
        WorkQueueExecutor(
            label="parsl-wq-example",

            # If a project_name is given, then Work Queue will periodically
            # report its status and performance back to the global WQ catalog,
            # which can be viewed here:  http://ccl.cse.nd.edu/software/workqueue/status

            # To disable status reporting, comment out the project_name.
            project_name="parsl-wq-" + str(uuid.uuid4()),

            # The port number that Work Queue will listen on for connecting workers.
            port=9123,

            # A shared filesystem is not needed when using Work Queue.
            shared_fs=False
        )
    ]
)
