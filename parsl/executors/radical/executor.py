"""RadicalPilotExecutor builds on the RADICAL-Pilot/Parsl
"""
import os
import time
import parsl
import queue
import logging
import inspect
import typeguard
import threading as mt

import radical.pilot as rp
import radical.utils as ru

from functools import partial
from typing import Optional, Dict
from concurrent.futures import Future

from .rpex_resources import ResourceConfig

from radical.pilot import PythonTask
from parsl.app.errors import AppException
from parsl.utils import RepresentationMixin
from parsl.executors.base import ParslExecutor

RPEX = 'RPEX'
BASH = 'bash'
PYTHON = 'python'

os.environ["RADICAL_REPORT"] = "False"
os.environ["RADICAL_LOG_LVL"] = "DEBUG"

logger = logging.getLogger(__name__)


class RadicalPilotExecutor(ParslExecutor, RepresentationMixin):
    """Executor is designed for executing heterogeneous tasks
       in terms of type/resource.

    The RadicalPilotExecutor system has the following components:

      1. "start"    :creating the RADICAL-executor session and pilot.
      2. "translate":unwrap/identify/ out of parsl task and construct RP task.
      3. "submit"   :translating and submiting Parsl tasks to Radical Pilot.
      4. "shut_down":shutting down the RADICAL-executor components.

    Here is a diagram

    .. code:: python

    ---------------------------------------------------------------------------
             Parsl Data Flow Kernel        |   Task Translator |  Task-Manager
    ---------------------------------------|-------------------|---------------
                                           |                   |
    -> Dep. check ------> Parsl_tasks{} <--+--> Parsl Task     |
     Data management          +dfk.submit  |        |          |
                                           |        v          |
                                           |    RP Task(s) ->  | submit(task)
    ---------------------------------------------------------------------------

    The RadicalPilotExecutor creates a ``SESSION OBJECT``, ``TASK_MANAGER``,
    and ``PILOT_MANAGER``. The executor receives the tasks from the DFK and
    translates these tasks (in-memory) into ``RP.TASK_DESCRIPTION`` object to
    be passed to the ``TASK_MANAGER``. This executor has two submission
    mechanisms:

    1. Default_mode: where the executor submits the tasks directly to
       RADICAL-Pilot.

    2. Bulk_mode: where the executor accumulates N tasks (functions and
       executables) and submit them.

    Parameters
    ----------
    rpex_cfg : :class: `~parsl.executors.rpex_resources.ResourceConfig
        a dataclass specifying resource configuration.
        Default is ResourceConfig instance.

    label : str
        Label for this executor instance.
        Default is "RPEX".

    bulk_mode : bool
        Enable bulk mode submssion and execution. Default is False (stream).

    resource : Optional[str]
        The resource name of the targeted HPC machine or cluster.
        Default is local.localhost (user local machine).

    access_schema : Optional[str]
        The key of an access mechanism to use. Default local.

    walltime : int
        The maximum walltime for the entire job in minutes.
        Default is 30.

    cores : int
        The number of CPU cores to allocate per job (pilot). Default is 2.

    gpus : int
        The number of GPUs to allocate per job (pilot). Default is 0.

    partition : Optional[str]
        The resource partition (queue) for the job (pilot). Default is None.

    project : Optional[str]
        The project name for resource allocation. Default is None.

    For more information: https://radicalpilot.readthedocs.io/en/stable/
    """

    @typeguard.typechecked
    def __init__(self,
                 cores: int,
                 walltime: int,
                 resource: str,
                 gpus: int = 0,
                 label: str = RPEX,
                 bulk_mode: bool = False,
                 project: Optional[str] = None,
                 rpex_cfg: Optional[str] = None,
                 partition: Optional[str] = None,
                 access_schema: Optional[str] = None):

        super().__init__()
        self._uid = RPEX.lower()
        self.project = project
        self.bulk_mode = bulk_mode
        self.resource = resource
        self.access_schema = access_schema
        self.partition = partition
        self.walltime = walltime
        self.label = label
        self.future_tasks: Dict[str, Future] = {}
        self.cores = cores
        self.gpus = gpus
        self.run_dir = '.'
        self.session = None
        self.pmgr = None
        self.tmgr = None

        if rpex_cfg:
            self.rpex_cfg = rpex_cfg
        elif not rpex_cfg and 'local' in resource:
            self.rpex_cfg = ResourceConfig.get_cfg_file()
        else:
            raise ValueError('Resource config file must be '
                             'specified for a non-local execution')

        cfg = ru.Config(cfg=ru.read_json(self.rpex_cfg))

        self.master = cfg.master_descr
        self.n_masters = cfg.n_masters
        self.pilot_env = cfg.pilot_env

    def task_state_cb(self, task, state):
        """
        Update the state of Parsl Future tasks
        Based on RP task state callbacks.
        """
        if not task.uid.startswith('master'):
            parsl_task = self.future_tasks[task.uid]

            if state == rp.DONE:
                if task.description['mode'] in [rp.TASK_EXEC,
                                                rp.TASK_EXECUTABLE]:
                    parsl_task.set_result(int(task.exit_code))

                else:
                    parsl_task.set_result(task.return_value)

            elif state == rp.CANCELED:
                parsl_task.set_exception(AppException(rp.CANCELED))

            elif state == rp.FAILED:
                parsl_task.set_exception(AppException(task.stderr))

    def start(self):
        """Create the Pilot component and pass it.
        """
        logger.info("starting RadicalPilotExecutor")
        logger.info('Parsl: {0}'.format(parsl.__version__))
        logger.info('RADICAL pilot: {0}'.format(rp.version))
        self.session = rp.Session(cfg={'base': self.run_dir},
                                  uid=ru.generate_id('rpex.session',
                                                     mode=ru.ID_PRIVATE))
        logger.info(f"RPEX session is created: {0}".format(self.session.path))

        pd_init = {'gpus': self.gpus,
                   'cores': self.cores,
                   'exit_on_error': True,
                   'queue': self.partition,
                   'project': self.project,
                   'runtime': self.walltime,
                   'resource': self.resource,
                   'access_schema': self.access_schema}

        # move the agent sandbox in the workdir mainly for tests purposes
        if not self.resource or 'local' in self.resource:
            pd_init['sandbox'] = self.run_dir
            logger.info("RPEX will be running in the local mode")

        pd = rp.PilotDescription(pd_init)

        tds = list()
        executor_path = os.path.abspath(os.path.dirname(__file__))
        master_path = '{0}/rpex_master.py'.format(executor_path)

        for i in range(self.n_masters):
            td = rp.TaskDescription(self.master)
            td.mode = rp.RAPTOR_MASTER
            td.uid = ru.generate_id('master.%(item_counter)06d', ru.ID_CUSTOM,
                                    ns=self.session.uid)
            td.arguments = [self.rpex_cfg, i]
            td.ranks = 1
            td.cores_per_rank = 1
            td.input_staging = [{'source': master_path,
                                 'target': 'rpex_master.py',
                                 'action': rp.TRANSFER,
                                 'flags': rp.DEFAULT_FLAGS},
                                {'source': self.rpex_cfg,
                                 'target': os.path.basename(self.rpex_cfg),
                                 'action': rp.TRANSFER,
                                 'flags': rp.DEFAULT_FLAGS}]
            tds.append(td)

        self.pmgr = rp.PilotManager(session=self.session)
        self.tmgr = rp.TaskManager(session=self.session)

        # submit pilot(s)
        pilot = self.pmgr.submit_pilots(pd)
        self.tmgr.submit_tasks(tds)

        logger.info("setting up the executor environment")
        pilot.prepare_env(env_name=self.pilot_env.get('name'),
                          env_spec={'type': self.pilot_env.get('type'),
                                    'path': self.pilot_env.get('path'),
                                    'setup': self.pilot_env.get('setup'),
                                    'version': self.pilot_env.get('version'),
                                    'pre_exec': self.pilot_env.get('pre_exec')
                                    })

        self.tmgr.add_pilots(pilot)
        self.tmgr.register_callback(self.task_state_cb)

        # create a bulking thread to run the actual task submittion
        # to RP in bulks
        if self.bulk_mode:
            self._max_bulk_size = 1024
            self._max_bulk_time = 3        # seconds
            self._min_bulk_time = 0.1      # seconds

            self._bulk_queue = queue.Queue()
            self._bulk_thread = mt.Thread(target=self._bulk_collector)

            self._bulk_thread.daemon = True
            self._bulk_thread.start()

        return True

    def unwrap(self, func, args):
        """
        Unwrap a parsl app and its args for further processing.

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

        # ignore the resource dict from Parsl
        new_args = list(args)
        new_args.pop(0)
        args = tuple(new_args)

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
        except Exception:
            logger.exception('failed to obtain task type')

        return func, args, task_type

    def task_translate(self, tid, func, args, kwargs):
        """
        Convert parsl function to RADICAL-Pilot Task-Description
        """

        task = rp.TaskDescription()
        func, args, task_type = self.unwrap(func, args)

        if BASH in task_type:
            if callable(func):
                # These lines of code are from parsl/app/bash.py
                try:
                    # Execute the func to get the command
                    bash_app = func(*args, **kwargs)
                    if not isinstance(bash_app, str):
                        raise ValueError("Expected a str for bash_app cmd,"
                                         "got: {0}".format(type(bash_app)))
                except AttributeError as e:
                    raise Exception("failed to obtain bash app cmd") from e

                task.mode = rp.TASK_EXECUTABLE
                task.executable = '/bin/bash'
                task.arguments = ['-c', bash_app]

                # specifying pre_exec is only for executables
                task.pre_exec = kwargs.get('pre_exec', [])

        elif PYTHON in task_type or not task_type:
            task.mode = rp.TASK_FUNCTION
            task.raptor_id = 'master.%06d' % (tid % self.n_masters)
            task.function = PythonTask(func, *args, **kwargs)

        task.ranks = kwargs.get('ranks', 1)
        task.cores_per_rank = kwargs.get('cores_per_rank', 1)
        task.threading_type = kwargs.get('threading_type', '')
        task.gpus_per_rank = kwargs.get('gpus_per_rank', 0)
        task.gpu_type = kwargs.get('gpu_type', '')
        task.mem_per_rank = kwargs.get('mem_per_rank', 0)
        task.stdout = kwargs.get('stdout', '')
        task.stderr = kwargs.get('stderr', '')
        task.timeout = kwargs.get('walltime', 0.0)

        return task

    def _bulk_collector(self):

        bulk = list()

        while True:

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

            if bulk:
                logger.debug('submit bulk: %d', len(bulk))
                self.tmgr.submit_tasks(bulk)
                bulk = list()

    def submit(self, func, *args, **kwargs):
        """
        Submits tasks in stream mode or bulks (bulk mode)
        to RADICAL-Pilot rp.TASK_MANAGER.
        """
        rp_tid = ru.generate_id('task.%(item_counter)06d', ru.ID_CUSTOM,
                                ns=self.session.uid)
        parsl_tid = int(rp_tid.split('task.')[1])

        logger.debug("got Task {0} from parsl-dfk".format(parsl_tid))
        task = self.task_translate(parsl_tid, func, args, kwargs)

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
        logger.info("RadicalPilotExecutor shutdown")
        self.session.close(download=True)

        return True

    @property
    def scaling_enabled(self) -> bool:
        return False

    def scale_in(self, blocks: int):
        raise NotImplementedError

    def scale_out(self, blocks: int):
        raise NotImplementedError
