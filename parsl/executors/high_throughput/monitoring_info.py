# this is a global that will be worker-specific
# and can be set to the result queue which can
# then be acquired by any other code running in
# a worker context - specifically the monitoring
# wrapper code.
from queue import Queue
from typing import Optional

result_queue: Optional[Queue] = None
