# this is a global that will be worker-specific
# and can be set to the result queue which can
# then be acquired by any other code running in
# a worker context - specifically the monitoring
# wrapper code.
from typing import Optional
from queue import Queue
result_queue: Optional[Queue] = None
