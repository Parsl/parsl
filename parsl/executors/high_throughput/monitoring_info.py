# this is a global that will be worker-specific
# and can be set to the result queue which can
# then be acquired by any other code running in
# a worker context - specifically the monitoring
# wrapper code.
import multiprocessing
from typing import Optional, Union
from queue import Queue
result_queue: Optional[Union[Queue, multiprocessing.Queue]] = None
