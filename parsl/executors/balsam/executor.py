"""BalsamExecutor
"""

import logging
from abc import ABC
from asyncio import Future
from typing import Optional, List, Callable, Dict, Any, Tuple, Union
from balsam.api import Job, App, BatchJob, Site, site_config

import threading
import time
import typeguard

from parsl.executors.errors import UnsupportedFeatureError
from parsl.executors.status_handling import NoStatusHandlingExecutor
from parsl.utils import RepresentationMixin
import asyncio

logger = logging.getLogger(__name__)

SITE_ID: int = 0
CLASS_PATH: int = 1


class BalsamFutureException(Exception):
    """"""
    pass


class BalsamUnsupportedFeatureException(Exception):
    """"""
    pass


class BalsamFuture(Future):
    """

    """
    _job: Job = None
    _timeout: int = 20
    _future: Future = None

    def __init__(self, job, future):
        super(BalsamFuture, self).__init__()
        self._job = job
        self._future = future

    def submit(self):
        pass

    @asyncio.coroutine
    def get_result(self):
        """
        Poll Balsam2 API about this Job as long is job is either not finished,
        or gets cancelled by calling future.cancel() on the future object
        """
        while self._job.state != "JOB_FINISHED":
            self._timeout -= 1

            print("Checking result in balsam...")
            self._job.refresh_from_db()

            if self._timeout <= 0:
                print("Cancelling job due to timeout reached.")
                self.cancel()
                return

            yield time.sleep(2)

        if not self.cancelled():
            print("Result is available: ",self._job.data)
            self.set_result(self._job.data["result"])
            self.done()

        if self.cancelled():
            logger.info("Job future was cancelled")


class BalsamExecutor(NoStatusHandlingExecutor, RepresentationMixin):
    """

    """
    managed = False

    @typeguard.typechecked
    def __init__(self,
                 label: str = 'BalsamExecutor',
                 workdir: str = 'balsam',
                 numnodes: int = 1,
                 walltime: int = 30,
                 queue: str = 'local',
                 mode: str = 'mpi',
                 project: str = 'MyProject',
                 siteid: int = -1,
                 tags: Dict[str, str] = {}
                 ):
        logger.debug("Initializing HighThroughputExecutor")

        NoStatusHandlingExecutor.__init__(self)
        self.label = label
        self.workdir = workdir
        self.numnodes = numnodes
        self.walltime = walltime
        self.queue = queue
        self.project = project
        self.mode = mode
        self.tags = tags
        self.siteid = siteid
        self.timeout = 30
        self.sleep = 1

    def _get_block_and_job_ids(self) -> Tuple[List[str], List[Any]]:
        pass

    @property
    def scaling_enabled(self) -> bool:
        return False

    def start(self) -> Optional[List[str]]:
        """Start the executor.

        Any spin-up operations (for example: starting thread pools) should be performed here.
        """
        import os
        from balsam.api import BatchJob

        os.makedirs(self.workdir+os.path.sep+'executor'+os.path.sep+'logs', exist_ok=True)
        batchjob = BatchJob(
            num_nodes=self.numnodes,
            wall_time_min=self.walltime,
            job_mode=self.mode,
            queue=self.queue,
            site_id=self.siteid,
            project=self.project,
            filter_tags=self.tags
        )
        batchjob.save()

    @asyncio.coroutine
    def poll_balsam_result(self, future):
        while self.job.state != "JOB_FINISHED":

            print("Checking result for {} in balsam...".format(self.appname))
            self.job.refresh_from_db()

            if self.timeout <= 0:
                print("Cancelling job due to timeout reached.")
                future.cancel()
                return

            print("Sleeping {} seconds...timeout in {} seconds.".format(self.sleep, self.timeout))
            yield time.sleep(self.sleep)

        if not self.future.cancelled():
            print("Result is available {}: ".format(self.appname), self.job.data)
            future.set_result(self.job.data["result"])
            future.done()

    def submit(self, func: Callable, resource_specification: Dict[str, Any], *args: Any, **kwargs: Any) -> Future:
        """Submit
        submit(func,None, site_id, class_path, {})
        """
        import os
        import inspect
        site_id = args[SITE_ID]
        class_path = args[CLASS_PATH]

        appname = kwargs['appname']
        sitedir = kwargs['sitedir']
        appdir = sitedir + os.path.sep + "apps"
        #appdeffile = appdir + os.path.sep + appname + ".py"
        #parslrunner = appdir + os.path.sep + "parslapprunner.py"

        workdir = kwargs['workdir'] if 'workdir' in kwargs else 'site'+site_id+os.path.sep+"/"+appname

        thread = kwargs['thread'] if 'thread' in kwargs else True
        callback = kwargs['callback'] if 'callback' in kwargs else None
        inputs = kwargs['inputs'] if 'inputs' in kwargs else []
        script = kwargs['script'] if 'script' in kwargs else 'bash'

        node_packing_count = kwargs['node_packing_count'] if 'node_packing_count' in kwargs else 1
        parameters = kwargs['params'] if 'params' in kwargs else {}
        parameters['name'] = appname

        if resource_specification:
            logger.error("Ignoring the resource specification. ")
            raise BalsamUnsupportedFeatureException()

        if script == 'bash':
            shell_command = func(inputs=inputs)

        elif script == 'python':
            # Inject function into appdef
            lines = inspect.getsource(func)
            shell_command = "python << HEREDOC\n{}\nHEREDOC".format(lines)
        else:
            # Found unknown script type
            raise

        # Use lines to inject into parslapprunner with shell_command as python -c
        try:
            app = App.objects.create(site_id=site_id, class_path=class_path)
        except:
            app = App.objects.get(site_id=site_id, class_path=class_path)

        app.save()
        job = Job(
            workdir,
            app.id,
            parameters={},
            node_packing_count=node_packing_count,
        )
        job.parameters["command"] = shell_command
        job.save()
        loop = asyncio.get_event_loop()

        # Create a new Future object.
        future = loop.create_future()
        #self.task = loop.create_task(self.poll_balsam_result(future))

        if callback:
            future.add_done_callback(callback)

        balsam_future = BalsamFuture(job, future)

        def run():
            import os

            _workdir = self.workdir+os.path.sep+job.workdir.name
            logger.debug("Making workdir for job: {}".format(workdir))
            os.makedirs(_workdir, exist_ok=True)
            logger.debug("Running loop.run_until_complete: ", job)
            #self.task = loop.create_task(balsam_future.get_result())
            loop.run_until_complete(balsam_future.get_result())
            print("Running...")

        if thread:
            logger.debug("Starting job thread: ", job)
            thread = threading.Thread(target=run, args=())
            thread.start()
        else:
            run()

        return balsam_future

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
        # Shutdown site
        pass

    @property
    def bad_state_is_set(self):
        return True

    @property
    def executor_exception(self):
        return True

    @property
    def error_management_enabled(self):
        return True
