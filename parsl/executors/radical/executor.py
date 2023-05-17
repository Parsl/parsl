"""RadicalPilotExecutor builds on the RADICAL-Pilot/Parsl
"""
import os
import sys
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

from radical.pilot import PythonTask
from parsl.app.errors import AppException
from parsl.utils import RepresentationMixin
from parsl.executors.status_handling import NoStatusHandlingExecutor

BASH = 'bash'
PYTHON = 'python'

logger = logging.getLogger(__name__)


class RadicalPilotExecutor(NoStatusHandlingExecutor, RepresentationMixin):
    """Executor designed for: executing heterogeneous tasks in terms of
                              type/resource

    The RadicalPilotExecutor system has the following components:

      1. "start"    :creating the RADICAL-executor session and pilot.
      2. "translate":unwrap/identify/ out of parsl task and construct RP task.
      2. "submit"   :translating and submiting Parsl tasks the RADICAL-executor.
      3. "shut_down":shutting down the RADICAL-executor components.

    RADICAL Executor
    ------------------------------------------------------------------------------------------------
             Parsl DFK/dflow               |      Task Translator      |     RP-Client/Task-Manager
    ---------------------------------------|---------------------------|----------------------------
                                           |                           |
    -> Dep. check ------> Parsl_tasks{} <--+--> Parsl Task/func/arg/kwg| tmgr.submit_Tasks(RP_tasks)
     Data management          +dfk.submit  |             |             |
                                           |             v             |
                                           |     RP Task/Tasks desc. --+->
    ------------------------------------------------------------------------------------------------
    """

    @typeguard.typechecked
    def __init__(self,
                 rpex_cfg,
                 label: str = 'RPEX',
                 bulk_mode: bool = False,
                 resource: Optional[str] = None,
                 login_method: Optional[str] = None,
                 walltime: int = 10,
                 managed: bool = True,
                 cores: int = 1,
                 gpus: int = 0,
                 worker_logdir_root: Optional[str] = ".",
                 partition: Optional[str] = None,
                 project: Optional[str] = None):

        self._uid = 'rpex'
        self.project = project
        self.bulk_mode = bulk_mode
        self.resource = resource
        self.login_method = login_method
        self.partition = partition
        self.walltime = walltime
        self.label = label
        self.future_tasks: Dict[str, Future] = {}
        self.cores = cores
        self.gpus = gpus
        self.managed = managed
        self.run_dir = '.'
        self.worker_logdir_root = worker_logdir_root

        self.session = None
        self.pmgr = None
        self.tmgr = None

        # Raptor specific
        self.rpex_cfg = rpex_cfg
        cfg = ru.Config(cfg=ru.read_json(self.rpex_cfg))

        self.master = cfg.master_descr
        self.worker = cfg.worker_descr
        self.cpn = cfg.cpn         # cores per node
        self.gpn = cfg.gpn         # gpus per node
        self.n_masters = cfg.n_masters   # number of total masters
        self.masters_pn = cfg.masters_pn  # number of masters per node

        self.pilot_env = cfg.pilot_env

    def task_state_cb(self, task, state):
        """
        Update the state of Parsl Future tasks
        Based on RP task state
        """

        # FIXME: user might specify task uid as
        # task.uid = 'master...' this migh create
        # a confusion with the raptpor master
        if not task.uid.startswith('master'):
            parsl_task = self.future_tasks[task.uid]

            if state == rp.DONE:
                if task.description['mode'] in [rp.TASK_EXECUTABLE, rp.TASK_EXEC]:
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
        self.session = rp.Session(uid=ru.generate_id('rpex.session', mode=ru.ID_PRIVATE))

        if self.resource is None:
            logger.error("specify remote or local resource")

        else:
            pd_init = {'resource': self.resource,
                       'runtime': self.walltime,
                       'exit_on_error': True,
                       'project': self.project,
                       'queue': self.partition,
                       'access_schema': self.login_method,
                       'cores': self.cores,
                       'gpus': self.gpus}
        pd = rp.PilotDescription(pd_init)

        tds = list()
        executor_path = os.path.abspath(os.path.dirname(__file__))
        master_path = '{0}/rpex_master.py'.format(executor_path)
        worker_path = '{0}/rpex_worker.py'.format(executor_path)

        for i in range(self.n_masters):
            td = rp.TaskDescription(self.master)
            td.named_env = 'rp'
            td.mode = rp.RAPTOR_MASTER
            td.uid = ru.generate_id('master.%(item_counter)06d', ru.ID_CUSTOM,
                                    ns=self.session.uid)
            td.arguments = [self.rpex_cfg, i]
            td.cpu_threads = 1
            td.cpu_processes = 1
            td.input_staging = [{'source': master_path,
                                 'target': 'rpex_master.py',
                                 'action': rp.TRANSFER,
                                 'flags': rp.DEFAULT_FLAGS},
                                {'source': worker_path,
                                 'target': 'rpex_worker.py',
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

        # FIXME: master tasks are never checked for, should add a task state
        #        callback to do so.

        pilot.stage_in({'source': ru.which('radical-pilot-hello.sh'),
                        'target': 'radical-pilot-hello.sh',
                        'action': rp.TRANSFER})

        python_v = '{0}.{1}'.format(sys.version_info[0], sys.version_info[1])
        pilot.prepare_env(env_name='ve_rpex',
                          env_spec={'type': self.pilot_env.get('type', 'virtualenv'),
                                    'version': self.pilot_env.get('version', python_v),
                                    'path': self.pilot_env.get('path', ''),
                                    'pre_exec': self.pilot_env.get('pre_exec', []),
                                    'setup': self.pilot_env.get('setup', [])})

        self.tmgr.add_pilots(pilot)
        self.tmgr.register_callback(self.task_state_cb)

        # create a bulking thread to run the actual task submittion to RP in
        # bulks
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

        task_type = ''

        # Ignore the resource dict from Parsl
        new_args = list(args)
        new_args.pop(0)
        args = tuple(new_args)

        # remove the remote wrapper from parsl
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__

        # identify the task type
        try:
            # bash and python might be partial wrapped
            if isinstance(func, partial):
                # @bash_app from parsl
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

            # @python_app from parsl
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
        Args:
            - tid   (int)      : Parsl task id
            - func  (callable) : Callable function
            - *args (list)     : List of arbitrary positional arguments.

        Kwargs:
            - **kwargs (dict) : A dictionary of arbitrary keyword args for func.

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
                        raise ValueError("Expected a str for bash_app cmd, got: {0}".format(type(bash_app)))
                except AttributeError as e:
                    raise Exception("failed to obtain bash app cmd") from e

                task.mode = rp.TASK_EXECUTABLE
                task.scheduler = None
                task.executable = '/bin/bash'
                task.arguments = ['-c', bash_app]

                # specifying pre_exec is only for executables
                task.pre_exec = kwargs.get('pre_exec', [])

        elif PYTHON in task_type or not task_type:
            task.mode = rp.TASK_FUNCTION
            task.scheduler = 'master.%06d' % (tid % self.n_masters)
            task.function = PythonTask(func, *args, **kwargs)

        task.stdout = kwargs.get('stdout', '')
        task.stderr = kwargs.get('stderr', '')
        task.timeout = kwargs.get('walltime', 0.0)
        task.cpu_threads = kwargs.get('cpu_threads', 1)
        task.gpu_threads = kwargs.get('gpu_threads', 0)
        task.cpu_processes = kwargs.get('cpu_processes', 1)
        task.gpu_processes = kwargs.get('gpu_processes', 0)
        task.cpu_process_type = kwargs.get('cpu_process_type', '')
        task.gpu_process_type = kwargs.get('gpu_process_type', '')

        return task

    def _bulk_collector(self):

        bulk = list()

        while True:

            now = time.time()  # time of last submission

            # collect tasks for min bulk time
            # NOTE: total collect time could actually be max_time + min_time
            while time.time() - now < self._max_bulk_time:

                try:
                    task = self._bulk_queue.get(block=True, timeout=self._min_bulk_time)
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
        Submits tasks iteratively (stream mode) or tasks in bulks (bulk mode) to
        RADICAL task_manager.

        Args:
            - func (callable) : Callable function
            - *args (list)    : List of arbitrary positional arguments.

        Kwargs:
            - **kwargs (dict) : A dictionary of arbitrary keyword args for func.
        """
        rp_tid = ru.generate_id('task.%(item_counter)06d', ru.ID_CUSTOM,
                                ns=self.session.uid)
        parsl_tid = int(rp_tid.split('task.')[1])

        logger.debug("got {0} from parsl-dfk".format(parsl_tid))
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
