from enum import Enum


class MessageType(Enum):

    # Reports any task related info such as launch, completion etc.
    TASK_INFO = 0

    # Top level workflow information
    WORKFLOW_INFO = 2

    # Reports of the resource capacity for each node
    NODE_INFO = 3
