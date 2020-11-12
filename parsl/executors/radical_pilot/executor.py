""" WorkQueueExecutor utilizes the Work Queue distributed framework developed by the
Cooperative Computing Lab (CCL) at Notre Dame to provide a fault-tolerant,
high-throughput system for delegating Parsl tasks to thousands of remote machines
"""
import logging
import threading
from concurrent.futures import Future

import pickle
import inspect
import shutil
from pathlib import Path

from radical.pilot import UnitManager, ComputeUnitDescription, states, TRANSFER, CREATE_PARENTS

from parsl.app.errors import RemoteExceptionWrapper
from parsl.data_provider.files import File
from parsl.serialize import pack_apply_message
from parsl.executors.status_handling import NoStatusHandlingExecutor

import typeguard
from typing import Dict, List, Optional


logger = logging.getLogger(__name__)


class RadicalPilotExecutor(NoStatusHandlingExecutor):
    """Executor to use Radical Pilot


        Parameters
        ----------

        label: str
            A human readable label for the executor, unique
            with respect to other Work Queue master programs.
            Default is "WorkQueueExecutor".

        unit_manager: UnitManager
            A pre-configured RP Unit Manager to use for submitting RP Units

        working_dir: str
            Location for Parsl to perform app delegation to the Work
            Queue system. Defaults to current directory.


        source: bool
            Choose whether to transfer parsl app information as
            source code. (Note: this increases throughput for
            @python_apps, but the implementation does not include
            functionality for @bash_apps, and thus source=False
            must be used for programs utilizing @bash_apps.)
            Default is False. Set to True if pack is True

        keep_task_dir: bool
            If True, keep the function directory where the function
            and results are serialized

        keep_failed_task_dir: bool
            If True and the invocation fails, keep the function directory.
            This is automatically True if keep_task_dir is True.

        wrapper_path: str
            Path of the Parsl function execution wrapper (exec_parsl_function.py).
            By default this is left at 'exec_parsl_function.py'.

    """

    @typeguard.typechecked
    def __init__(self,
                 label: str = "RadicalPilotExecutor",
                 unit_manager: Optional[UnitManager] = None,
                 working_dir: Optional[str] = ".",
                 source: Optional[bool] = False,
                 keep_task_dir: Optional[bool] = False,
                 keep_failed_task_dir: Optional[bool] = True,
                 managed: Optional[bool] = True,
                 wrapper_path: Optional[str] = 'exec_parsl_function.py'):
        NoStatusHandlingExecutor.__init__(self)

        self.unit_manager = unit_manager
        self.label = label
        self.working_dir = working_dir
        self.source = source
        self.keep_task_dir = keep_task_dir
        self.keep_failed_task_dir = keep_failed_task_dir
        self.managed = managed
        self.wrapper_path = wrapper_path

        self.task_counter = -1
        self.tasks_lock = threading.Lock()

    def start(self):
        """
        """
        self.data_dir = Path(self.run_dir) / 'data_dir'
        self.unit_manager.register_callback(self.cb)

    def _create_task_dir(self, task_id: str) -> Path:
        """
        """
        task_dir = '{:04d}'.format(task_id)
        path = self.data_dir / task_dir
        path.mkdir(parents=True)
        return path

    def _maybe_remove_task_dir(self, task_dir: Path, exception: bool = False):
        if exception:
            if not self.keep_task_dir and not self.keep_failed_task_dir:
                shutil.rmtree(task_dir)
        else:
            if not self.keep_task_dir:
                shutil.rmtree(task_dir)

    def _add_output(self, cud: ComputeUnitDescription, f: object) -> None:
        if not isinstance(f, File):
            return
        # This is not a good choice for a production system since it does not prevent
        # collisions. A better solution would be a hash of the canonical path.
        cud.output_staging.append({
            'source': 'unit://' + f.filename,
            'target': f.url,
            'action': TRANSFER,
            'flags': CREATE_PARENTS
        })

    def _add_input(self, cud: ComputeUnitDescription, f: object) -> None:
        if not isinstance(f, File):
            return
        cud.input_staging.append({
            'source': f.url,
            'target': 'unit://' + f.filename,
            'action': TRANSFER,
            'flags': CREATE_PARENTS
        })

    def _add_staging(self, cud: ComputeUnitDescription, args: List[str], kwargs: Dict[str, object],
                     function_file: Path, result_file: Path) -> None:
        cud.input_staging = []
        cud.output_staging = []

        for f in kwargs.get('inputs', []) + list(args):
            self._add_input(cud, f)

        for f in kwargs.get('outputs', []):
            self._add_output(cud, f)

        for k, v in kwargs.items():
            if k in ['stdout', 'stderr']:
                self._add_output(cud, File(v))
            else:
                self._add_input(cud, v)

        self._add_input(cud, File(str(function_file)))
        self._add_output(cud, File(str(result_file)))

    def submit(self, func, resource_specification, *args, **kwargs):
        """
        """
        self.task_counter += 1
        task_id = self.task_counter
        task_dir = self._create_task_dir(task_id)
        try:
            cud = ComputeUnitDescription()

            # we have a task_id -> Future map, so set this to be able to retrieve the future
            # when we receive notifications about the unit state
            cud.name = str(task_id)
            # must be installed remotely
            cud.executable = self.wrapper_path

            function_file = task_dir / '.function'
            result_file = task_dir / '.result'

            self._add_staging(cud, args, kwargs, function_file, result_file)

            # Create a Future object and have it be mapped from the task ID in the tasks dictionary
            fu = Future()
            with self.tasks_lock:
                self.tasks[str(task_id)] = (task_dir, fu)

            self._serialize_function(function_file, func, args, kwargs)

            self.unit_manager.submit_units(cud)
            return fu
        except Exception:
            self._maybe_remove_task_dir(task_dir, exception=True)
            raise

    def cb(self, obj, state):
        # in a production version of this executor, we may want to deserialize the result in
        # a separate thread, in order to avoid blocking RP

        if state not in states.FINAL:
            return

        task_id = obj.name
        with self.tasks_lock:
            task_dir, fu = self.tasks[task_id]

        if state == states.DONE:
            if obj.exit_code is not None and obj.exit_code != 0:
                fu.set_exception(Exception(obj.stderr + '\n' + obj.stdout))
            else:
                try:
                    result = self._read_result(task_dir)
                    if isinstance(result, RemoteExceptionWrapper):
                        result.reraise()
                    else:
                        fu.set_result(result)
                except Exception as ex:
                    fu.set_exception(Exception('Failed to read results file: %s' % ex))
        if state == states.CANCELED:
            fu.set_exception(Exception('Task cancelled'))
        if state == states.FAILED:
            fu.set_exception(Exception(obj.stderr + '\n' + obj.stdout))

        self._maybe_remove_task_dir(task_dir, exception=(state != states.DONE))

    def _read_result(self, task_dir):
        with open(task_dir / '.result', "rb") as f:
            return pickle.load(f)

    def _serialize_function(self, fn_path, parsl_fn, parsl_fn_args, parsl_fn_kwargs):
        """Takes the function application parsl_fn(*parsl_fn_args, **parsl_fn_kwargs)
        and serializes it to the file fn_path."""

        # Either build a dictionary with the source of the function, or pickle
        # the function directly:
        if self.source:
            function_info = {"source code": inspect.getsource(parsl_fn),
                             "name": parsl_fn.__name__,
                             "args": parsl_fn_args,
                             "kwargs": parsl_fn_kwargs}
        else:
            function_info = {"byte code": pack_apply_message(parsl_fn, parsl_fn_args, parsl_fn_kwargs,
                                                             buffer_threshold=1024 * 1024)}

        with open(fn_path, "wb") as f_out:
            pickle.dump(function_info, f_out)

    def shutdown(self, *args, **kwargs):
        """
        """
        self.unit_manager.close()
        return True

    @property
    def scaling_enabled(self) -> bool:
        return False

    def scale_in(self, blocks: int):
        raise NotImplementedError

    def scale_out(self, blocks: int):
        raise NotImplementedError
