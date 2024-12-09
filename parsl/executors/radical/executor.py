"""RadicalPilotExecutor builds on the RADICAL-Pilot/Parsl
"""
import inspect
import logging
import os
import queue
import sys
import threading as mt
import time
from concurrent.futures import Future
from functools import partial
from pathlib import PosixPath
from typing import Dict, Optional

import requests
import typeguard

import parsl
from parsl.app.errors import BashExitFailure, RemoteExceptionWrapper
from parsl.app.python import timeout
from parsl.data_provider.files import File
from parsl.executors.base import ParslExecutor
from parsl.serialize import deserialize, pack_res_spec_apply_message
from parsl.serialize.errors import DeserializationError, SerializationError
from parsl.utils import RepresentationMixin

from .rpex_resources import CLIENT, MPI, ResourceConfig

try:
    import radical.pilot as rp
    import radical.utils as ru
except ImportError:
    _rp_enabled = False
else:
    _rp_enabled = True


RPEX = 'RPEX'
BASH = 'bash'
PYTHON = 'python'

CWD = os.getcwd()
PWD = os.path.abspath(os.path.dirname(__file__))

PARSL_RP_RESOURCE_MAP = {'cores': 'ranks',
                         'disk': 'lfs_per_rank',
                         'memory': 'mem_per_rank'}

logger = logging.getLogger(__name__)


class RadicalPilotExecutor(ParslExecutor, RepresentationMixin):
    """Executor is designed for executing heterogeneous tasks
       in terms of type/resource.

    The RadicalPilotExecutor system has the following main components:

      1. "start": Create and start the RADICAL-Pilot runtime components ``rp.Session``,
          ``rp.PilotManager`` and ``rp.TaskManager``.
      2. "translate": Unwrap, identify, and parse Parsl ``apps`` into ``rp.TaskDescription``.
      3. "submit": Submit Parsl apps to ``rp.TaskManager``.
      4. "shutdown": Shut down the RADICAL-Pilot runtime and all associated components.

    Here is a diagram

    .. code:: python

      ----------------------------------------------------------------------------
               Parsl Data Flow Kernel        |   Task Translator |  rp.TaskManager
      ---------------------------------------|-------------------|----------------
                                             |                   |
      -> Dep. check ------> Parsl_tasks{} <--+--> Parsl Task     |
       Data management          +dfk.submit  |        |          |
                                             |        v          |
                                             |    RP Task(s) ->  | submit(task)
      ----------------------------------------------------------------------------

    The RadicalPilotExecutor creates a ``rp.Session``, ``rp.TaskManager``,
    and ``rp.PilotManager``. The executor receives the Parsl apps from the
    DFK and translates these apps (in-memory) into ``rp.TaskDescription``
    object to be passed to the ``rp.TaskManager``. This executor has two
    submission mechanisms:

    1. Default_mode: where the executor submits the tasks directly to
       RADICAL-Pilot.

    2. Bulk_mode: where the executor accumulates N tasks (functions and
       executables) and submit them.

    Parameters
    ----------
    rpex_cfg : :class: `~parsl.executors.rpex_resources.ResourceConfig`
        a dataclass specifying resource configuration.
        Default is ResourceConfig instance.

    label : str
        Label for this executor instance.
        Default is "RPEX".

    bulk_mode : bool
        Enable bulk mode submission and execution. Default is False (stream).

    resource : Optional[str]
        The resource name of the targeted HPC machine or cluster.
        Default is local.localhost (user local machine).

    runtime : int
        The maximum runtime for the entire job in minutes.
        Default is 30.

    working_dir : str
        The working dir to be used by the executor.

    rpex_pilot_kwargs: Dict of kwargs that are passed directly to the rp.PilotDescription object.

    For more information: https://radicalpilot.readthedocs.io/en/stable/
    """

    @typeguard.typechecked
    def __init__(self,
                 resource: str,
                 label: str = RPEX,
                 bulk_mode: bool = False,
                 working_dir: Optional[str] = None,
                 rpex_cfg: Optional[ResourceConfig] = None, **rpex_pilot_kwargs):

        super().__init__()
        self.pmgr = None
        self.tmgr = None
        self.run_dir = '.'
        self.label = label
        self.session = None
        self.resource = resource
        self._uid = RPEX.lower()
        self.bulk_mode = bulk_mode
        self._terminate = mt.Event()
        self.working_dir = working_dir
        self.pilot_kwargs = rpex_pilot_kwargs
        self.future_tasks: Dict[str, Future] = {}

        if rpex_cfg:
            self.rpex_cfg = rpex_cfg.get_config()
        elif not rpex_cfg and 'local' in resource:
            self.rpex_cfg = ResourceConfig().get_config()
        else:
            raise ValueError('Resource config must be '
                             'specified for a non-local resources')

    def task_state_cb(self, task, state):
        """
        Update the state of Parsl Future apps
        Based on RP task state callbacks.
        """
        # check the Master/Worker state
        if task.mode in [rp.RAPTOR_MASTER, rp.RAPTOR_WORKER]:
            if state == rp.FAILED:
                exception = RuntimeError(f'{task.uid} failed with internal error: {task.stderr}')
                self._fail_all_tasks(exception)

        # check all other tasks state
        else:
            parsl_task = self.future_tasks[task.uid]

            if state == rp.DONE:
                if task.description['mode'] in [rp.TASK_EXEC,
                                                rp.TASK_PROC,
                                                rp.TASK_EXECUTABLE]:
                    parsl_task.set_result(int(task.exit_code))
                else:
                    # we do not support MPI function output
                    # serialization. TODO: To be fixed soon.
                    if not task.description.get('use_mpi'):
                        result = deserialize(eval(task.return_value))
                        parsl_task.set_result(result)
                    else:
                        parsl_task.set_result(task.return_value)

            elif state == rp.CANCELED:
                parsl_task.cancel()

            elif state == rp.FAILED:
                if task.description['mode'] in [rp.TASK_EXEC,
                                                rp.TASK_EXECUTABLE]:
                    parsl_task.set_exception(BashExitFailure(task.name,
                                                             task.exit_code))
                else:
                    if task.exception:
                        # unpack a serialized exception
                        if not task.description.get('use_mpi') or task.description['mode'] == rp.TASK_PROC:
                            self._unpack_and_set_parsl_exception(parsl_task, task.exception)
                        # we do not serialize mpi function exception
                        else:
                            parsl_task.set_exception(eval(task.exception))
                    else:
                        parsl_task.set_exception('Task failed for an unknown reason')

    def _fail_all_tasks(self, exception):
        """
        Fail all outstanding tasks with the given exception.

        This method iterates through all outstanding tasks in the
        `_future_tasks` dictionary, which have not yet completed,
        and sets the provided exception as their result, indicating
        a failure.

        Parameters:
        - exception: The exception to be set as the result for all
                     outstanding tasks.
        """
        for fut_task in self.future_tasks.values():
            if not fut_task.done():
                fut_task.set_exception(exception)

    def start(self):
        """Create the Pilot component and pass it.
        """
        logger.info("starting RadicalPilotExecutor")
        logger.info('Parsl: {0}'.format(parsl.__version__))
        logger.info('RADICAL pilot: {0}'.format(rp.version))
        self.session = rp.Session(cfg={'base': self.run_dir},
                                  uid=ru.generate_id('rpex.session',
                                                     mode=ru.ID_PRIVATE))
        logger.info("RPEX session is created: {0}".format(self.session.path))

        pd_init = {**self.pilot_kwargs,
                   'exit_on_error': True,
                   'resource': self.resource}

        if not self.resource or 'local' in self.resource:
            os.environ["RADICAL_LOG_LVL"] = "DEBUG"
            logger.info("RPEX will be running in local mode")

        pd = rp.PilotDescription(pd_init)
        pd.verify()

        # start RP's main components TMGR, PMGR and Pilot
        self.tmgr = rp.TaskManager(session=self.session)
        self.pmgr = rp.PilotManager(session=self.session)
        self.pilot = self.pmgr.submit_pilots(pd)

        if not self.pilot.description.get('cores') or not self.pilot.description.get('nodes'):
            logger.warning('no "cores/nodes" per pilot were set, using default resources')

        self.tmgr.add_pilots(self.pilot)
        self.tmgr.register_callback(self.task_state_cb)

        worker_path = '{0}/rpex_worker.py'.format(PWD)

        self.masters = []

        logger.info(f'Starting {self.rpex_cfg.n_masters} masters and {self.rpex_cfg.n_workers} workers for each master')

        # create N masters
        for _ in range(self.rpex_cfg.n_masters):
            md = rp.TaskDescription(self.rpex_cfg.master_descr)
            md.uid = ru.generate_id('rpex.master.%(item_counter)06d', ru.ID_CUSTOM,
                                    ns=self.session.uid)

            # submit the master to the TMGR
            master = self.tmgr.submit_raptors(md)[0]
            self.masters.append(master)

            workers = []
            # create N workers for each master and submit them to the TMGR
            for _ in range(self.rpex_cfg.n_workers):
                wd = rp.TaskDescription(self.rpex_cfg.worker_descr)
                wd.uid = ru.generate_id('rpex.worker.%(item_counter)06d', ru.ID_CUSTOM,
                                        ns=self.session.uid)
                wd.raptor_id = master.uid
                wd.input_staging = self._stage_files([File(worker_path)], mode='in')
                workers.append(wd)

            self.tmgr.submit_workers(workers)

        self.select_master = self._cyclic_master_selector()

        # prepare or use the current env for the agent/pilot side environment
        if self.rpex_cfg.pilot_env_mode != CLIENT:
            logger.info("creating {0} environment for the executor".format(self.rpex_cfg.pilot_env.name))
            self.pilot.prepare_env(env_name=self.rpex_cfg.pilot_env.name,
                                   env_spec=self.rpex_cfg.pilot_env.as_dict())
        else:
            client_env = sys.prefix
            logger.info("reusing ({0}) environment for the executor".format(client_env))

        # create a bulking thread to run the actual task submission
        # to RP in bulks
        if self.bulk_mode:
            self._max_bulk_size = 1024
            self._max_bulk_time = 3        # seconds
            self._min_bulk_time = 0.1      # seconds

            self._bulk_queue = queue.Queue()
            self._bulk_thread = mt.Thread(target=self._bulk_collector)

            self._bulk_thread.daemon = True
            self._bulk_thread.start()

            logger.info('bulk mode is on, submitting tasks in bulks')

        return True

    def _cyclic_master_selector(self):
        """
        Balance tasks submission across N masters and N workers
        """
        current_master = 0
        masters_uids = [m.uid for m in self.masters]

        while True:
            yield masters_uids[current_master]
            current_master = (current_master + 1) % len(self.masters)

    def unwrap(self, func, args):
        """
        Unwrap a Parsl app and its args for further processing.

        Parameters
        ----------
        func : callable
            The function to be unwrapped.

        args : tuple
            The arguments associated with the function.

        Returns
        -------
        tuple
            A tuple containing the unwrapped function, adjusted arguments,
            and task type information.
        """

        task_type = ''

        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__

        try:
            if isinstance(func, partial):
                try:
                    task_type = inspect.getsource(func.args[0]).split('\n')[0]
                    if BASH in task_type:
                        task_type = BASH
                        func = func.args[0]
                    else:
                        task_type = PYTHON

                except Exception:
                    logger.exception('unwrap failed')

                    return func, args, task_type

            else:
                task_type = inspect.getsource(func).split('\n')[0]
                if PYTHON in task_type:
                    task_type = PYTHON
                else:
                    task_type = ''
        except Exception as e:
            raise Exception('failed to obtain task type: {0}'.format(e))

        return func, args, task_type

    def task_translate(self, tid, func, parsl_resource_specification, args, kwargs):
        """
        Convert Parsl function to RADICAL-Pilot rp.TaskDescription
        """

        task = rp.TaskDescription()
        task.name = func.__name__

        if parsl_resource_specification and isinstance(parsl_resource_specification, dict):
            logger.debug('mapping Parsl resource specifications >> RP resource specifications')
            for key, val in parsl_resource_specification.items():
                if key not in task.as_dict():
                    key = PARSL_RP_RESOURCE_MAP.get(key, None)
                    if not key:
                        logger.warning('ignoring "{0}" key from task resource specification as it is not supported by RP'.format(key))
                        continue
                setattr(task, key, val)

        func, args, task_type = self.unwrap(func, args)

        if BASH in task_type:
            if callable(func):
                # if the user specifies the executable mode then we expect the
                # a code in a file that need to be executed in an isolated env.
                if parsl_resource_specification.get('mode') == rp.TASK_EXECUTABLE:
                    # These lines of code are from parsl/app/bash.py
                    try:
                        # Execute the func to get the command
                        bash_app = func(*args, **kwargs)
                        if not isinstance(bash_app, str):
                            raise ValueError("Expected a str for bash_app cmd,"
                                             "got: {0}".format(type(bash_app)))
                    except AttributeError as e:
                        raise Exception("failed to obtain bash app cmd") from e

                    task.executable = bash_app
                    task.mode = rp.TASK_EXECUTABLE

                # This is the default mode where the bash_app will be executed as
                # as a single core process by RP. For cores > 1 the user must use
                # task.mode=rp.TASK_EXECUTABLE (above) or use MPI functions if their
                # code is Python.
                else:
                    task.mode = rp.TASK_PROC
                    task.raptor_id = next(self.select_master)
                    task.executable = self._pack_and_apply_message(func, args, kwargs)

        elif PYTHON in task_type or not task_type:
            task.mode = rp.TASK_FUNCTION
            task.raptor_id = next(self.select_master)
            if kwargs.get('walltime'):
                func = timeout(func, kwargs['walltime'])

            # Check how to serialize the function object
            if MPI in self.rpex_cfg.worker_type.lower():
                task.use_mpi = True
                task.function = rp.PythonTask(func, *args, **kwargs)
            else:
                task.use_mpi = False
                task.function = self._pack_and_apply_message(func, args, kwargs)

        task.input_staging = self._stage_files(kwargs.get("inputs", []),
                                               mode='in')
        task.output_staging = self._stage_files(kwargs.get("outputs", []),
                                                mode='out')

        task.input_staging.extend(self._stage_files(list(args), mode='in'))

        self._set_stdout_stderr(task, kwargs)

        try:
            task.verify()
        except ru.typeddict.TDKeyError as e:
            raise Exception(f'{e}. Please check: https://radicalpilot.readthedocs.io/en/stable/ documentation')

        return task

    def _pack_and_apply_message(self, func, args, kwargs):
        try:
            buffer = pack_res_spec_apply_message(func,
                                                 args,
                                                 kwargs,
                                                 resource_specification={},
                                                 buffer_threshold=1024 * 1024)
            task_func = rp.utils.serialize_bson(buffer)
        except TypeError:
            raise SerializationError(func.__name__)

        return task_func

    def _unpack_and_set_parsl_exception(self, parsl_task, exception):
        try:
            try:
                s = rp.utils.deserialize_bson(exception)
            except Exception:
                s = exception

            if isinstance(s, RemoteExceptionWrapper):
                try:
                    s.reraise()
                except Exception as e:
                    parsl_task.set_exception(e)
            elif isinstance(s, Exception):
                parsl_task.set_exception(s)
            elif isinstance(s, str):
                parsl_task.set_exception(eval(s))
            else:
                raise ValueError("Unknown exception-like type received: {}".format(type(s)))
        except Exception as e:
            parsl_task.set_exception(
                DeserializationError("Received exception, but handling also threw an exception: {}".format(e)))

    def _set_stdout_stderr(self, task, kwargs):
        """
        set the stdout and stderr of a task
        """
        for k in ['stdout', 'stderr']:
            k_val = kwargs.get(k, '')
            if k_val:
                # check the type of the stderr/out
                if isinstance(k_val, File):
                    k_val = k_val.filepath
                elif isinstance(k_val, PosixPath):
                    k_val = k_val.__str__()

                # set the stderr/out to the desired
                # name by the user
                setattr(task, k, k_val)
                task.sandbox = CWD

    def _stage_files(self, files, mode):
        """
        a function to stage list of input/output
        files between two locations.
        """
        to_stage = []
        files = [f for f in files if isinstance(f, File)]
        for file in files:
            if mode == 'in':
                # a workaround RP not supporting
                # staging https file
                if file.scheme == 'https':
                    r = requests.get(file.url)
                    p = CWD + '/' + file.filename
                    with open(p, 'wb') as ff:
                        ff.write(r.content)
                    file = File(p)

                f = {'source': file.url,
                     'action': rp.TRANSFER}
                to_stage.append(f)

            elif mode == 'out':
                # this indicates that the user
                # did not provided a specific
                # output file and RP will stage out
                # the task.stdout from pilot://task_folder
                # to the CWD or file.url
                if '/' not in file.url:
                    f = {'source': file.filename,
                         'target': file.url,
                         'action': rp.TRANSFER}
                    to_stage.append(f)
            else:
                raise ValueError('unknown staging mode')

        return to_stage

    def _bulk_collector(self):

        bulk = list()

        while not self._terminate.is_set():

            now = time.time()  # time of last submission

            # collect tasks for min bulk time
            # NOTE: total collect time could actually be max_time + min_time
            while time.time() - now < self._max_bulk_time:

                try:
                    task = self._bulk_queue.get(block=True,
                                                timeout=self._min_bulk_time)
                except queue.Empty:
                    task = None

                if task:
                    bulk.append(task)

                if len(bulk) >= self._max_bulk_size:
                    break

                if self._terminate.is_set():
                    break

            if bulk:
                logger.debug('submit bulk: %d', len(bulk))
                self.tmgr.submit_tasks(bulk)
                bulk = list()

    def submit(self, func, resource_specification, *args, **kwargs):
        """
        Submits tasks in stream mode or bulks (bulk mode)
        to RADICAL-Pilot rp.TaskManager.
        """
        rp_tid = ru.generate_id('task.%(item_counter)06d', ru.ID_CUSTOM,
                                ns=self.session.uid)
        parsl_tid = int(rp_tid.split('task.')[1])

        logger.debug("got Task {0} from Parsl-dfk".format(parsl_tid))
        task = self.task_translate(parsl_tid, func, resource_specification, args, kwargs)

        # assign task id for rp task
        task.uid = rp_tid

        # set the future with corresponding id
        self.future_tasks[rp_tid] = Future()

        if self.bulk_mode:
            # push task to rp submit thread
            self._bulk_queue.put(task)
        else:
            # submit the task to rp
            logger.debug("put {0} to rp-TMGR".format(rp_tid))
            self.tmgr.submit_tasks(task)

        return self.future_tasks[rp_tid]

    def shutdown(self, hub=True, targets='all', block=False):
        """Shutdown the executor, including all RADICAL-Pilot components."""
        logger.info("RadicalPilotExecutor is terminating...")

        self._terminate.set()

        # ensure we are in the bulk submssion mode
        if self.bulk_mode:
            self._bulk_thread.join()

        self.session.close(download=True)
        logger.info("RadicalPilotExecutor is terminated.")

        return True
