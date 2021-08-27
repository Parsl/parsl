"""Package defining FluxExecutor."""

import collections

TaskResult = collections.namedtuple("TaskResult", ("returnval", "exception"))
