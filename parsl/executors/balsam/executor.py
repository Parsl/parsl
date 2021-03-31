"""BalsamExecutor
"""

import logging
from typing import Optional, List, Callable, Dict, Any, Tuple, Union
from balsam.api import Job, App, BatchJob, Site, site_config
from multiprocessing import Condition

from concurrent.futures import Future
import time
import typeguard

from concurrent.futures import ThreadPoolExecutor
from parsl.executors.errors import UnsupportedFeatureError
from parsl.executors.status_handling import NoStatusHandlingExecutor
from parsl.utils import RepresentationMixin
import os

logger = logging.getLogger(__name__)
os.makedirs('logs', exist_ok=True)
fileh = logging.FileHandler(os.getcwd() + os.path.sep + 'logs' + os.path.sep + 'executor.log', 'a')
logger.addHandler(fileh)
logging.basicConfig(level=logging.INFO)

SITE_ID: int = 0
CLASS_PATH: int = 1
lock = Condition()
result_lock = Condition()


class BalsamJobFailureException(Exception):
    """

    """
    pass


class BalsamFutureException(Exception):
    """

    """
    pass


class BalsamUnsupportedFeatureException(UnsupportedFeatureError):
    """

    """
    pass


class BalsamFuture(Future):
    """

    """
    _job: Job = None
    _timeout: int = 60
    _appname: str = None
    _sleep: int = 2

    def __init__(self, job, appname, sleep=2, timeout=60):
        super(BalsamFuture, self).__init__()
        self._job = job
        self._appname = appname
        self._sleep = sleep
        self._timeout = timeout

    def submit(self):
        pass

    def poll_result(self):
        """
        Poll Balsam2 API about this Job as long is job is either not finished,
        or gets cancelled by calling future.cancel() on the future object
        """
        while self._job.state != "JOB_FINISHED":
            if self._job.state == 'FAILED':
                self.cancel()
                raise BalsamJobFailureException()
            
            result_lock.acquire()
            try:
                self._timeout -= 1

                logger.debug("Checking result in balsam {} {}...{}".format(str(id(self)), id(self._job), self._appname))
                self._job.refresh_from_db()

                if self._timeout <= 0:
                    logger.debug("Cancelling job due to timeout reached.")
                    self.cancel()
                    self._job.state = "FAILED"
                    self._job.save()
                    return
            finally:
                result_lock.release()

            time.sleep(self._sleep)

        if not self.cancelled():
            logger.debug("Result is available[{}]: {} {}".format(self._appname, id(self._job), self._job.data))
            self.set_result(self._job.data["result"])
            logger.debug("Set result on: {} {} ".format(id(self._job), id(self)))
            self.done()

        if self.cancelled():
            logger.debug("Job future was cancelled[{}]: {} {} ".format(self._appname, id(self._job), self._job.data))


class BalsamExecutor(NoStatusHandlingExecutor, RepresentationMixin):
    """

    """
    managed = False
    maxworkers = 3

    @typeguard.typechecked
    def __init__(self,
                 label: str = 'BalsamExecutor',
                 workdir: str = 'parsl',
                 numnodes: int = 1,
                 walltime: int = 30,
                 queue: str = 'local',
                 mode: str = 'mpi',
                 maxworkers: int = 3,
                 project: str = 'local',
                 siteid: int = -1,
                 sleep: int = 1,
                 sitedir: str = None,
                 node_packing_count: int = 1,
                 timeout: int = 60,
                 classpath: str = 'parslapprunner.ParslAppRunner',
                 tags: Dict[str, str] = {}
                 ):
        logger.debug("Initializing BalsamExecutor")

        NoStatusHandlingExecutor.__init__(self)
        self.label = label
        self.workdir = workdir
        self.numnodes = numnodes
        self.maxworkers = maxworkers
        self.walltime = walltime
        self.queue = queue
        self.project = project
        self.mode = mode
        self.tags = tags
        self.siteid = siteid
        self.timeout = timeout
        self.sitedir = sitedir
        self.sleep = sleep
        self.classpath = classpath
        self.node_packing_count = node_packing_count
        self.threadpool = None
        self.batchjob = None
        self.balsam_future = None
        self.workdir = workdir

    def _get_block_and_job_ids(self) -> Tuple[List[str], List[Any]]:
        pass

    @property
    def scaling_enabled(self) -> bool:
        return False

    def start(self) -> Optional[List[str]]:
        """Start the executor.

        Any spin-up operations (for example: starting thread pools) should be performed here.
        """
        self.threadpool = ThreadPoolExecutor(max_workers=self.maxworkers)

    def submit(self, func: Callable, resource_specification: Dict[str, Any], *args: Any, **kwargs: Any) -> Future:
        """Submit
        submit(func,None, site_id, class_path, {})
        """
        try:
            import os
            import sys
            import inspect
            import codecs
            import re
            import pickle

            appname = func.__name__
            site_id = kwargs['siteid'] if 'siteid' in kwargs else self.siteid

            workdir = kwargs['workdir'] if 'workdir' in kwargs else "parsl" + os.path.sep + appname

            logger.debug(os.getcwd(), workdir + os.path.sep + 'executor' + os.path.sep + 'logs')

            logger.debug("Log file is " + workdir + os.path.sep + 'executor' + os.path.sep +
                  'logs' + os.path.sep + 'executor.log')

            class_path = kwargs['classpath'] if 'classpath' in kwargs else self.classpath
            callback = kwargs['callback'] if 'callback' in kwargs else None
            inputs = kwargs['inputs'] if 'inputs' in kwargs else []
            script = kwargs['script'] if 'script' in kwargs else None
            sleep = kwargs['sleep'] if 'sleep' in kwargs else self.sleep
            numnodes = kwargs['numnodes'] if 'numnodes' in kwargs else self.numnodes
            walltime = kwargs['walltime'] if 'walltime' in kwargs else self.walltime
            timeout = kwargs['timeout'] if 'timeout' in kwargs else self.timeout

            node_packing_count = kwargs['node_packing_count'] if 'node_packing_count' in kwargs else self.node_packing_count
            parameters = kwargs['params'] if 'params' in kwargs else {}
            parameters['name'] = appname

            if resource_specification:
                logger.error("Ignoring the resource specification. ")
                raise BalsamUnsupportedFeatureException()

            if script == 'bash':
                shell_command = func(inputs=inputs)
            else:
                lines = inspect.getsource(func)

                pargs = codecs.encode(pickle.dumps(inputs), "base64").decode()
                pargs = re.sub(r'\n', "", pargs).strip()
                shell_command = "python << HEREDOC\n\nimport pickle\nimport codecs\nSITE_ID={}\nCLASS_PATH='{}'\n{}\npargs = '{}'\nargs = pickle.loads(codecs.decode(pargs.encode(), \"base64\"))\nresult = {}(inputs=[*args])\nprint(result)\nHEREDOC".format(
                    site_id, class_path, lines, pargs, appname)
                source = "import pickle\nimport codecs\nSITE_ID={}\nCLASS_PATH='{}'\n{}\npargs = '{}'\nargs = pickle.loads(codecs.decode(pargs.encode(), \"base64\"))\nresult = {}(inputs=[*args])\nprint(result)\n".format(
                    site_id, class_path, lines, pargs, appname)

                logger.debug(sys.executable)
                shell_command = sys.executable + ' app.py'
                source = source.replace('@python_app','#@python_app')

            try:
                app = App.objects.get(site_id=site_id, class_path=class_path)
            except Exception as ex:
                # Create App if it doesn't exist
                app = App.objects.create(site_id=site_id, class_path=class_path)
                app.save()

            logger.debug("Making workdir for job: {}".format(workdir))
            os.makedirs(workdir, exist_ok=True)

            job = Job(
                workdir,
                app.id,
                wall_time_min=walltime,
                num_nodes=numnodes,
                parameters={},
                node_packing_count=node_packing_count,
            )
            job.parameters["command"] = shell_command
            job.save()

            # Write function source to app.py in job workdir for balsam to pick up
            if script != 'bash':
                with open(job.resolve_workdir(site_config.data_path).joinpath("app.py"), "w") as appsource:
                    appsource.write(source)

            self.balsam_future = BalsamFuture(job, appname, sleep=sleep, timeout=timeout)

            if callback:
                self.balsam_future.add_done_callback(callback)

            logger.debug("Starting job thread: ", job)
            self.threadpool.submit(self.balsam_future.poll_result)

            return self.balsam_future
        finally:
            pass

    def scale_out(self, blocks: int) -> List[str]:
        """Scale out method.

        We should have the scale out method simply take resource object
        which will have the scaling methods, scale_out itself should be a coroutine, since
        scaling tasks can be slow.

        :return: A list of block ids corresponding to the blocks that were added.
        """

        raise NotImplementedError

    def scale_in(self, blocks: int) -> List[str]:
        """Scale in method.

        Cause the executor to reduce the number of blocks by count.

        We should have the scale in method simply take resource object
        which will have the scaling methods, scale_in itself should be a coroutine, since
        scaling tasks can be slow.

        :return: A list of block ids corresponding to the blocks that were removed.
        """

        raise NotImplementedError

    def shutdown(self) -> bool:
        """Shutdown the executor.

        This includes all attached resources such as workers and controllers.
        """
        self.balsam_future.cancel()

    @property
    def bad_state_is_set(self):
        return True

    @property
    def executor_exception(self):
        return False

    @property
    def error_management_enabled(self):
        return False
