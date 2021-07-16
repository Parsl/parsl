"""BalsamExecutor
"""

import logging
import time
import os
import typeguard
import threading
from uuid import uuid4
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Condition
from typing import Optional, List, Callable, Dict, Any, Tuple

from balsam.api import Job, App, BatchJob, Site, site_config
from parsl.executors.errors import UnsupportedFeatureError
from parsl.executors.status_handling import NoStatusHandlingExecutor
from parsl.utils import RepresentationMixin

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

lock = Condition()
result_lock = Condition()
futures_lock = Condition()

PARSL_SESSION = uuid4().hex

JOBS = {}


class BalsamBulkPoller:
    """
    Polls the balsam API for jobs associated with this batch
    """
    _thread = None

    def __init__(self, batchjob: BatchJob, futures: Dict, sleep: int):
        _thread = threading.Thread(target=self.bulk_poll, daemon=True, args=(futures, batchjob, sleep))
        logging.debug("BalsamBulkPoller: Starting...")
        _thread.start()

    def bulk_poll(self, futures: Dict, batchjob: BatchJob, sleep: int):
        import time
        logging.debug("bulk_poll: start")
        while True:
            logging.debug("bulk_poller")
            try:
                futures_lock.acquire()

                logging.debug("bulk_poll: Updating jobs for parsl-id %s", PARSL_SESSION)
                jobs = Job.objects.filter(tags={"parsl-id": PARSL_SESSION})
                logging.debug("bulk_poll: Updated jobs for parsl-id %s", PARSL_SESSION)
                logging.debug("bulk_poll: %s", jobs)

                for job in jobs:
                    JOBS[job.id] = job

                logging.debug("JOBS %s", JOBS)
                if batchjob:
                    logging.debug("Refreshing batchjob from db")
                    batchjob.refresh_from_db()

            finally:
                futures_lock.release()

            logging.debug("bulk_poll: Sleeping %s", sleep)
            time.sleep(sleep)


class BalsamExecutorException(Exception):
    """
    Generic balsam exception
    """
    pass


class BalsamJobFailureException(Exception):
    """
    Balsam Job Failure exception
    """
    pass


class BalsamFutureException(Exception):
    """
    Balsam future exception
    """
    pass


class BalsamUnsupportedFeatureException(UnsupportedFeatureError):
    """
    Unsupported feature exception
    """
    pass


class BalsamFuture(Future):
    """
    A future for a balsam job. Will poll for its result
    """
    _job: Job = None
    _timeout: int = 60
    _appname: str = None
    _sleep: int = 2

    def __init__(self, job, appname, sleep=2, timeout=600):
        super(BalsamFuture, self).__init__()
        self._job = job
        self._appname = appname
        self._sleep = sleep
        self._timeout = timeout

    def submit(self):
        pass

    def poll_bulk_result(self):

        logger.debug("Timeout is {}".format(self._timeout))
        while self._job.id in JOBS and JOBS[self._job.id].state != "JOB_FINISHED":
            if JOBS[self._job.id].state == 'FAILED':
                self.cancel()
                raise BalsamJobFailureException()

            self._timeout -= 1

            if self._timeout <= 0:
                logger.debug("Cancelling job due to timeout reached.")
                self.cancel()
                self._job.state = "FAILED"
                self._job.save()
                return

            time.sleep(self._sleep)

        logging.debug("JOB %s is FINISHED!", self._job.id)

        if not self.cancelled():
            import pickle

            logger.debug("Result is available[{}]: {} {}".format(self._appname, id(self._job), self._job.data))

            metadata = JOBS[self._job.id].data
            logger.debug(metadata)
            if metadata['type'] == 'python':
                logging.debug("Opening output file %s", metadata['file'])
                with open(metadata['file'], 'rb') as input:
                    result = pickle.load(input)
                    logger.debug("OUTPUT.PICKLE is " + str(result))
                    self.set_result(result)
            else:
                logger.debug("BASH RESULT is " + self._job.data['result'])
                self.set_result(self._job.data['result'])

            logger.debug("Set result on: {} {} ".format(id(self._job), id(self)))
            self.done()

        if self.cancelled():
            logger.debug("Job future was cancelled[{}]: {} {} ".format(self._appname, id(self._job), self._job.data))

    def poll_result(self):
        """
        Poll Balsam2 API about this Job as long is job is either not finished,
        or gets cancelled by calling future.cancel() on the future object
        """
        logger.debug("Timeout is {}".format(self._timeout))
        while self._job.state != "JOB_FINISHED":
            if self._job.state == 'FAILED':
                self.cancel()
                raise BalsamJobFailureException()

            result_lock.acquire()
            self._timeout -= 1

            logger.debug("Checking result in balsam {} {}...{}".format(str(id(self)), id(self._job), self._appname))
            self._job.refresh_from_db()

            if self._timeout <= 0:
                logger.debug("Cancelling job due to timeout reached.")
                self.cancel()
                self._job.state = "FAILED"
                self._job.save()
                return

            time.sleep(self._sleep)

        if not self.cancelled():
            import pickle

            logger.debug("Result is available[{}]: {} {}".format(self._appname, id(self._job), self._job.data))

            metadata = self._job.data
            logger.debug(metadata)
            if metadata['type'] == 'python':
                with open(metadata['file'], 'rb') as input:
                    result = pickle.load(input)
                    logger.debug("OUTPUT.PICKLE is " + str(result))
                    self.set_result(result)
            else:
                logger.debug("BASH RESULT is " + self._job.data['result'])
                self.set_result(self._job.data['result'])

            logger.debug("Set result on: {} {} ".format(id(self._job), id(self)))
            self.done()

        if self.cancelled():
            logger.debug("Job future was cancelled[{}]: {} {} ".format(self._appname, id(self._job), self._job.data))


class BalsamExecutor(NoStatusHandlingExecutor, RepresentationMixin):
    """
    Parsl executor for balsam2 jobs
    """
    managed = False
    maxworkers = 3
    exception = False

    @typeguard.typechecked
    def __init__(self,
                 label: str = 'BalsamExecutor',
                 workdir: str = 'parsl',
                 datadir: str = 'work',
                 image: str = None,
                 numnodes: int = 1,
                 walltime: int = 30,
                 jobnodes: int = 1,
                 queue: str = 'local',
                 mode: str = 'mpi',
                 maxworkers: int = 3,
                 project: str = 'local',
                 batchjob: bool = False,
                 siteid: int = None,
                 sleep: int = 1,
                 sitedir: str = None,
                 node_packing_count: int = 1,
                 timeout: int = 600,
                 classpath: str = 'parsl.AppRunner',
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
        self.jobnodes = jobnodes
        self.classpath = classpath
        self.node_packing_count = node_packing_count
        self.threadpool = None
        self.balsam_future = None
        self.workdir = workdir
        self.datadir = datadir
        self.image = image

        if sitedir is None and 'BALSAM_SITE_PATH' in os.environ:
            self.sitedir = os.environ['BALSAM_SITE_PATH']

        if siteid is None:
            self.siteid = Site.objects.get(path=self.sitedir).id

        self.batchjob = None

        if batchjob:
            logger.debug("Creating Batchjob")
            self.batchjob = BatchJob(
                num_nodes=self.numnodes,
                wall_time_min=self.walltime,
                job_mode=self.mode,
                queue=self.queue,
                site_id=self.siteid,
                project=self.project,
                filter_tags={"parsl-id": PARSL_SESSION}
            )

            self.batchjob.save()
            logger.debug("Saved Batchjob")

        self.bulkpoller = BalsamBulkPoller(self.batchjob, {}, self.sleep)

        if self.sitedir is None and 'BALSAM_SITE_PATH' not in os.environ:
            self.exception = True

            raise BalsamExecutorException("Environment variable BALSAM_SITE_PATH must be set if sitedir property is not used.")

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
            import inspect
            import codecs
            import re
            import pickle

            appname = func.__name__

            site_id = kwargs['siteid'] if 'siteid' in kwargs else self.siteid
            uuid = uuid4().hex
            workdir = kwargs['workdir'] if 'workdir' in kwargs else "parsl" + os.path.sep + appname + os.path.sep + uuid

            logger.debug("Log file is " + workdir + os.path.sep + 'executor' + os.path.sep +
                         'logs' + os.path.sep + 'executor.log')

            callback = kwargs['callback'] if 'callback' in kwargs else None
            inputs = kwargs['inputs'] if 'inputs' in kwargs else []
            script = kwargs['script'] if 'script' in kwargs else None
            sleep = kwargs['sleep'] if 'sleep' in kwargs else self.sleep
            walltime = kwargs['walltime'] if 'walltime' in kwargs else self.walltime
            timeout = kwargs['timeout'] if 'timeout' in kwargs else self.timeout

            node_packing_count = kwargs['node_packing_count'] if 'node_packing_count' in kwargs else self.node_packing_count
            parameters = kwargs['params'] if 'params' in kwargs else {}
            parameters['name'] = appname

            if resource_specification:
                logger.error("Ignoring the resource specification. ")
                raise BalsamUnsupportedFeatureException()

            logger.debug("WALLTIME: %s", walltime)
            appdir = os.path.abspath(self.sitedir + '/data/' + workdir)
            if script == 'bash':
                class_path = 'parsl.BashRunner'
                shell_command = func(inputs=inputs)

                try:
                    app = App.objects.get(site_id=site_id, class_path=class_path)
                except Exception:
                    # Create App if it doesn't exist
                    app = App.objects.create(site_id=site_id, class_path=class_path)
                    app.save()
                try:
                    logging.debug("Acquiring futures_lock")
                    futures_lock.acquire()
                    logging.debug("Acquired futures_lock")
                    job = Job(
                        workdir,
                        app.id,
                        wall_time_min=0,
                        num_nodes=self.jobnodes,
                        parameters={},
                        node_packing_count=node_packing_count,
                        tags={"parsl-id": PARSL_SESSION}
                    )

                    job.parameters["command"] = shell_command
                    job.save()
                    logging.debug("JOB %s saved.", job.id)
                finally:
                    futures_lock.release()

            elif script == 'container':
                import json
                import os
                import sys

                lines = []

                try:
                    lines = inspect.getsource(func.func)
                except:
                    import traceback
                    print(traceback.format_exc())

                class_path = 'parsl.ContainerRunner'

                logger.debug("{} Inputs: {}".format(appname, json.dumps(inputs)))
                pargs = codecs.encode(pickle.dumps(inputs), "base64").decode()
                pargs = re.sub(r'\n', "", pargs).strip()

                source = "import pickle\n" \
                         "import os\n" \
                         "import json\n" \
                         "import codecs\n" \
                         "SITE_ID={}\n" \
                         "CLASS_PATH='{}'\n" \
                         "{}\n" \
                         "pargs = '{}'\n" \
                         "args = pickle.loads(codecs.decode(pargs.encode(), \"base64\"))\n" \
                         "print(args)\n" \
                         "result = {}(inputs=[*args])\n" \
                         "with open('{}/output.pickle','ab') as output:\n" \
                         "    pickle.dump(result, output)\n".format(
                             site_id,
                             class_path,
                             lines,
                             pargs,
                             appname,
                             appdir) + \
                         "metadata = {\"type\":\"python\",\"file\":\"" + appdir + "/output.pickle\"}\n" \
                         "with open('/app/job.metadata','w') as job:\n" \
                         "    job.write(json.dumps(metadata))\n" \
                         "print(result)\n"

                source = source.replace('@container_app', '#@container_app')

                try:
                    app = App.objects.get(site_id=site_id, class_path=class_path)
                except Exception:
                    # Create App if it doesn't exist
                    app = App.objects.create(site_id=site_id, class_path=class_path)
                    app.save()

                try:
                    logging.debug("Acquiring futures_lock")
                    futures_lock.acquire()
                    logging.debug("Acquired futures_lock")
                    job = Job(
                        workdir,
                        app.id,
                        wall_time_min=0,
                        num_nodes=self.jobnodes,
                        parameters={},
                        node_packing_count=node_packing_count,
                        tags={"parsl-id": PARSL_SESSION}
                    )

                    job.parameters["image"] = self.image
                    job.parameters["datadir"] = self.datadir
                    job.save()
                    logging.debug("JOB %s saved.", job.id)
                    JOBS[job.id] = job
                finally:
                    futures_lock.release()

            else:
                import json
                import os
                import sys

                lines = []
                try:
                    lines = inspect.getsource(func)
                except:
                    import traceback
                    print(traceback.format_exc())

                class_path = 'parsl.AppRunner'

                logger.debug("{} Inputs: {}".format(appname, json.dumps(inputs)))
                pargs = codecs.encode(pickle.dumps(inputs), "base64").decode()
                pargs = re.sub(r'\n', "", pargs).strip()

                source = "import pickle\n" \
                         "import os\n" \
                         "import json\n" \
                         "import codecs\n" \
                         "SITE_ID={}\n" \
                         "CLASS_PATH='{}'\n" \
                         "{}\n" \
                         "pargs = '{}'\n" \
                         "args = pickle.loads(codecs.decode(pargs.encode(), \"base64\"))\n" \
                         "print(args)\n" \
                         "result = {}(inputs=[*args])\n" \
                         "with open('{}/output.pickle','ab') as output:\n" \
                         "    pickle.dump(result, output)\n".format(
                             site_id,
                             class_path,
                             lines,
                             pargs,
                             appname,
                             appdir) + \
                         "metadata = {\"type\":\"python\",\"file\":\"" + appdir + "/output.pickle\"}\n" \
                         "with open('/app/job.metadata','w') as job:\n" \
                         "    job.write(json.dumps(metadata))\n" \
                         "print(result)\n"

                source = source.replace('@python_app', '#@python_app')

                try:
                    app = App.objects.get(site_id=site_id, class_path=class_path)
                except Exception:
                    # Create App if it doesn't exist
                    app = App.objects.create(site_id=site_id, class_path=class_path)
                    app.save()

                try:
                    logging.debug("Acquiring futures_lock")
                    futures_lock.acquire()
                    logging.debug("Acquired futures_lock")
                    job = Job(
                        workdir,
                        app.id,
                        wall_time_min=0,
                        num_nodes=self.jobnodes,
                        parameters={},
                        node_packing_count=node_packing_count,
                        tags={"parsl-id": PARSL_SESSION}
                    )

                    job.parameters["python"] = sys.executable
                    job.save()
                    logging.debug("JOB %s saved.", job.id)
                    JOBS[job.id] = job
                finally:
                    futures_lock.release()

            logger.debug("Making workdir for job: {}".format(workdir))
            os.makedirs(workdir, exist_ok=True)

            os.makedirs(job.resolve_workdir(site_config.data_path), exist_ok=True)
            # Write function source to app.py in job workdir for balsam to pick up
            logger.debug("Script type is {}".format(str(script)))
            if script != 'bash':
                with open(job.resolve_workdir(site_config.data_path).joinpath("app.py"), "w") as appsource:
                    appsource.write(source)
                    logger.debug("Wrote app.py to {}".format(appsource.name))

            self.balsam_future = BalsamFuture(job, appname, sleep=sleep, timeout=timeout)

            if callback:
                self.balsam_future.add_done_callback(callback)

            self.threadpool.submit(self.balsam_future.poll_bulk_result)

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
        return self.batchjob.state.toLower() == "failed"

    @property
    def executor_exception(self):
        return self.batchjob.state.toLower() == "failed"

    @property
    def error_management_enabled(self):
        return False
