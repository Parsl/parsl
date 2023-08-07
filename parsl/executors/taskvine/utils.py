from typing import Optional


class ParslTaskToVine:
    """ Helper class to transform a Parsl function into a TaskVine task."""

    def __init__(self,
                 executor_id: int,                 # executor id of Parsl function
                 exec_mode: str,                   # execution mode of function, out of {regular, python, serverless}
                 category: str,                    # category of Parsl function
                 input_files: list,                # list of input files to this function
                 output_files: list,               # list of output files to this function
                 map_file: Optional[str],          # pickled file containing mapping of local to remote names of files
                 function_file: Optional[str],     # pickled file containing the function information
                 argument_file: Optional[str],     # pickled file containing the arguments to the function call
                 result_file: Optional[str],       # path to the pickled result object of the function execution
                 cores: Optional[float],           # number of cores to allocate
                 memory: Optional[int],            # amount of memory in MBs to allocate
                 disk: Optional[int],              # amount of disk in MBs to allocate
                 gpus: Optional[float],            # number of gpus to allocate
                 priority: Optional[float],        # priority of function, the higher the more priority
                 running_time_min: Optional[int],  # minimum amount of time for the function to run on a worker
                 env_pkg: Optional[str],           # path to a poncho environment tarball
                 ):
        self.executor_id = executor_id
        self.exec_mode = exec_mode
        self.category = category
        self.map_file = map_file
        self.function_file = function_file
        self.argument_file = argument_file
        self.result_file = result_file
        self.input_files = input_files
        self.output_files = output_files
        self.cores = cores
        self.memory = memory
        self.disk = disk
        self.gpus = gpus
        self.priority = priority
        self.running_time_min = running_time_min
        self.env_pkg = env_pkg


class VineTaskToParsl:
    """
    Support structure to communicate final status of TaskVine tasks to Parsl
    result is only valid if result_received is True
    reason and status are only valid if result_received is False
    """
    def __init__(self,
                 executor_id: int,          # executor id of task
                 result_received: bool,     # whether result is received or not
                 result,                    # result object if available
                 reason: Optional[str],     # string describing why execution fails
                 status: Optional[int]      # exit code of execution of task
                 ):
        self.executor_id = executor_id
        self.result_received = result_received
        self.result = result
        self.reason = reason
        self.status = status


class ParslFileToVine:
    """
    Support structure to report Parsl filenames to TaskVine.
    parsl_name is the local_name or filepath attribute of a Parsl file object.
    stage tells whether the file should be copied by TaskVine to the workers.
    cache tells whether the file should be cached at workers. Only valid if stage == True
    """
    def __init__(self,
                 parsl_name: str,   # name of file
                 stage: bool,       # whether TaskVine should know about this file
                 cache: bool        # whether TaskVine should cache this file
                 ):
        self.parsl_name = parsl_name
        self.stage = stage
        self.cache = cache


def run_parsl_function(map_file, function_file, argument_file, result_file):
    """
    Wrapper function to deploy with FunctionCall as serverless tasks.
    """
    from parsl.executors.taskvine.exec_parsl_function import run
    run(map_file, function_file, argument_file, result_file)
