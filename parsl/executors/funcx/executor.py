import threading
import time
from parsl.utils import RepresentationMixin
from parsl.executors.status_handling import NoStatusHandlingExecutor
from concurrent.futures import Future
import os
import uuid

from parsl.serialize import pack_apply_message, deserialize


class FuncXExecutor(NoStatusHandlingExecutor, RepresentationMixin):

    def __init__(self,
                 label="FuncXExecutor",
                 workdir='FXEX'):
        self.label = label   
        self.workdir = os.path.abspath(workdir)
        self.task_count = 0
        self._kill_event = threading.Event()

    def start(self):
        """ Called when DFK starts the executor when the config is loaded
        1. Make workdir        
        2. Start task_status_poller
        3. Create a funcx SDK client
        """
        os.mak
        pass

    def submit(self):
        pass

    def task_status_poller(self):
        "Task status poller thread that keeps polling the status of existing tasks"
        while not kill_event.is_set():
            to_remove = []
            if 
        pass

    def shutdown(self):
        pass

    def scale_in(self):
        pass
    
    def scale_out(self):
        pass
    
    def scaling_enabled(self):
        return False
