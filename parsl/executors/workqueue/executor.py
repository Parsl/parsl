import threading
from collections import deque
from concurrent.futures import Future

import os
import pickle

from ipyparallel.serialize import pack_apply_message

from parsl.executors.errors import *
from parsl.executors.base import ParslExecutor

from work_queue import *


def WorkQueueThread(tasks={},
                    task_queue=deque,
                    queue_lock = threading.Lock(),
                    tasks_lock = threading.Lock(), 
                    port = 50000,
                    launch_cmd = None,
                    data_dir="."):
    
    print("spin up the thread")

    wq_to_parsl = {}

    q = WorkQueue(port)
    
    continue_running = True
    while(continue_running):
        # Monitor the Task Queue

        place_holder_queue = []

        queue_lock.acquire()
        queue_len = len(task_queue) 
        while queue_len > 0:
            place_holder_queue.append(task_queue.pop())
            queue_len -= 1
        queue_lock.release()
            
        # Submit Tasks
        for item in place_holder_queue:
            parsl_id = item["task_id"]
            if parsl_id < 0:
                continue_running = False
                break
            function_data_loc = item["data_loc"]
            function_result_loc = item["result_loc"]
            function_result_loc_remote = function_result_loc.split("/")[-1]
            function_data_loc_remote = function_data_loc.split("/")[-1]
           
            # TODO Make this general 
            full_script_name = "/afs/crc.nd.edu/user/a/alitteke/parsl/parsl//executors/workqueue/workqueue_worker.py"
            script_name = full_script_name.split("/")[-1]
            command_str = launch_cmd.format(input_file = function_data_loc,
                                            output_file = function_result_loc)
            print(command_str)
            t = Task(command_str)
            t.specify_file(full_script_name, script_name, WORK_QUEUE_INPUT, cache = True)
            t.specify_file(function_result_loc, function_result_loc_remote, WORK_QUEUE_OUTPUT, cache = False)
            t.specify_file(function_data_loc, function_data_loc_remote, WORK_QUEUE_INPUT, cache = False)
            wq_id = q.submit(t)
            wq_to_parsl[wq_id] = parsl_id
            print(str(wq_id)+" submitted")

        if continue_running == False:
           print("exiting while loop")
           break

        # Wait for Tasks
        task_found = True
        while task_found:
            t = q.wait(5)
            if t == None:
                task_found = False
            else:
                print(t.id)
                wq_tid = t.id
                status = t.return_status
                if status != 0:
                    # Should probably do something smarter for this later
                    del wq_to_parsl[wq_tid]
                    continue
                parsl_tid = wq_to_parsl[wq_tid]
                result_loc = os.path.join(data_dir, "task_"+str(parsl_tid)+"_function_result")
                f = open(result_loc, "rb")
                result = pickle.load(f)
                f.close()

                tasks_lock.acquire()
                future = tasks[parsl_tid]
                tasks_lock.release()
               
                future.set_result(result) 
                del wq_to_parsl[wq_tid]

    for wq_task in wq_to_parsl:
        q.cancel_by_taskid(wq_task)

    print("returning")
    return

class WorkQueueExecutor(ParslExecutor):
    """Define the strict interface for all Executor classes.

    This is a metaclass that only enforces concrete implementations of
    functionality by the child classes.

    In addition to the listed methods, a ParslExecutor instance must always
    have a member field:

       label: str - a human readable label for the executor, unique
              with respect to other executors.

    """

    def __init__(self,
              label="wq",
              working_dir=None,
              managed = True,
              port=WORK_QUEUE_DEFAULT_PORT):
        self.label = label
        self.managed = managed
        self.task_queue = deque()
        self.tasks = {}
        self.port = port
        self.task_counter = 0
        self.scaling_enabled = False
        print("init for wq Executed")

        self.launch_cmd = ("python3 workqueue_worker.py {input_file} {output_file}")

    def start(self):
        self.queue_lock = threading.Lock()
        self.tasks_lock = threading.Lock()

        self.function_data_dir = os.path.join(self.run_dir, "function_data")

        os.mkdir(self.function_data_dir) 

        thread_kwargs = {"port": self.port,
                         "tasks": self.tasks,
                         "task_queue": self.task_queue,
                         "queue_lock": self.queue_lock,
                         "tasks_lock": self.tasks_lock,
                         "launch_cmd": self.launch_cmd,
                         "data_dir": self.function_data_dir} 
        self.master_thread = threading.Thread(target = WorkQueueThread,
                                    name = "master_thread", 
                                    kwargs = thread_kwargs)
        self.master_thread.start()
 
    def submit(self, func, *args, **kwargs):
        """Submit.

        We haven't yet decided on what the args to this can be,
        whether it should just be func, args, kwargs or be the partially evaluated
        fn
        """
        self.task_counter += 1
        task_id = self.task_counter

        fu = Future()
        self.tasks_lock.acquire()
        self.tasks[task_id] = fu
        self.tasks_lock.release()

        # Pickle the result into object to pass into message buffer
        # TODO Try/Except Block
        function_data_file = os.path.join(self.function_data_dir, "task_"+str(task_id) + "_function_data")
        function_result_file = os.path.join(self.function_data_dir, "task_"+str(task_id) + "_function_result")
        f = open(function_data_file, "wb")
        fn_buf = pack_apply_message(func, args, kwargs,
                           buffer_threshold=1024 * 1024,
                           item_threshold=1024)
        pickle.dump(fn_buf, f)
        f.close

        msg = {"task_id": task_id,
               "data_loc": function_data_file,
               "result_loc": function_result_file}

        self.queue_lock.acquire()
        self.task_queue.appendleft(msg)
        self.queue_lock.release()

        return fu

    def scale_out(self, *args, **kwargs):
        """Scale out method.

        We should have the scale out method simply take resource object
        which will have the scaling methods, scale_out itself should be a coroutine, since
        scaling tasks can be slow.
        """
        pass

    def scale_in(self, count):
        """Scale in method.

        Cause the executor to reduce the number of blocks by count.

        We should have the scale in method simply take resource object
        which will have the scaling methods, scale_in itself should be a coroutine, since
        scaling tasks can be slow.
        """
        pass

    def shutdown(self, *args, **kwargs):
        """Shutdown the executor.

        This includes all attached resources such as workers and controllers.
        """
        print("shutdown")
        msg = {"task_id": -1,
               "data_loc": None,
               "result_loc": None}

        self.queue_lock.acquire()
        self.task_queue.append(msg)
        self.queue_lock.release()

        self.master_thread.join()

        return True

    def scaling_enabled(self):
        """Specify if scaling is enabled.

        The callers of ParslExecutors need to differentiate between Executors
        and Executors wrapped in a resource provider
        """
        return False

    def run_dir(self):
        """Path to the run directory.
        """
        return self._run_dir

    def run_dir(self, value):
        self._run_dir = value
