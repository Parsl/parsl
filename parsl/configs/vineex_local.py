import uuid

from parsl.config import Config
from parsl.executors.taskvine import TaskVineExecutor, TaskVineManagerConfig
from parsl.usage_tracking.levels import LEVEL_1

config = Config(
    executors=[
        TaskVineExecutor(
            label="parsl-vine-example",

            # If a project_name is given, then TaskVine will periodically
            # report its status and performance back to the global TaskVine catalog,
            # which can be viewed here:  http://ccl.cse.nd.edu/software/taskvine/status

            # To disable status reporting, comment out the project_name.
            manager_config=TaskVineManagerConfig(project_name="parsl-vine-" + str(uuid.uuid4())),
        )
    ],
    usage_tracking=LEVEL_1,
)
