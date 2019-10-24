import logging
import os
import pathlib
import uuid

from ipyparallel import Client
from parsl.providers import LocalProvider
from parsl.utils import RepresentationMixin

from parsl.executors.base import ParslExecutor
from parsl.executors.errors import ScalingFailed
from parsl.executors.ipp_controller import Controller
from parsl.utils import wait_for_file

logger = logging.getLogger(__name__)


class IPyParallelExecutor(ParslExecutor, RepresentationMixin):
    """The IPython Parallel executor.

    This executor uses IPythonParallel's pilot execution system to manage multiple processes
    running locally or remotely.

    Parameters
    ----------
    provider : :class:`~parsl.providers.provider_base.ExecutionProvider`
        Provider to access computation resources. Can be one of :class:`~parsl.providers.aws.aws.EC2Provider`,
        :class:`~parsl.providers.cobalt.cobalt.Cobalt`,
        :class:`~parsl.providers.condor.condor.Condor`,
        :class:`~parsl.providers.googlecloud.googlecloud.GoogleCloud`,
        :class:`~parsl.providers.gridEngine.gridEngine.GridEngine`,
        :class:`~parsl.providers.jetstream.jetstream.Jetstream`,
        :class:`~parsl.providers.local.local.Local`,
        :class:`~parsl.providers.sge.sge.GridEngine`,
        :class:`~parsl.providers.slurm.slurm.Slurm`, or
        :class:`~parsl.providers.torque.torque.Torque`.
    label : str
        Label for this executor instance.
    controller : :class:`~parsl.executors.ipp_controller.Controller`
        Which Controller instance to use. Default is `Controller()`.
    workers_per_node : int
        Number of workers to be launched per node. Default=1
    container_image : str
        Launch tasks in a container using this docker image. If set to None, no container is used.
        Default is None.
    engine_dir : str
        Directory where engine logs and configuration files will be stored.
    working_dir : str
        Directory where input data should be staged to.
    storage_access : list of :class:`~parsl.data_provider.staging.Staging`
        Specifications for accessing data this executor remotely.
    managed : bool
        If True, parsl will control dynamic scaling of this executor, and be responsible. Otherwise,
        this is managed by the user.
    engine_debug_level : int | str
        Sets engine logging to specified debug level. Choices: (0, 10, 20, 30, 40, 50, 'DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL')

    .. note::
           Some deficiencies with this executor are:

               1. Ipengines execute one task at a time. This means one engine per core
                  is necessary to exploit the full parallelism of a node.
               2. No notion of remaining walltime.
               3. Lack of throttling means tasks could be queued up on a worker.
    """

    def __init__(self,
                 provider=LocalProvider(),
                 label='ipp',
                 working_dir=None,
                 controller=Controller(),
                 container_image=None,
                 engine_dir=None,
                 storage_access=None,
                 engine_debug_level=None,
                 workers_per_node=1,
                 managed=True):
        self.provider = provider
        self.label = label
        self.working_dir = working_dir
        self.controller = controller
        self.engine_debug_level = engine_debug_level
        self.container_image = container_image
        self.engine_dir = engine_dir
        self.workers_per_node = workers_per_node
        self.storage_access = storage_access
        self.managed = managed

        self.debug_option = ""
        if self.engine_debug_level:
            self.debug_option = "--log-level={}".format(self.engine_debug_level)

    def start(self):
        self.controller.profile = self.label
        self.controller.ipython_dir = self.run_dir
        if self.engine_dir is None:
            parent, child = pathlib.Path(self.run_dir).parts[-2:]
            self.engine_dir = os.path.join(parent, child)
        self.controller.start()

        self.engine_file = self.controller.engine_file

        with wait_for_file(self.controller.client_file, seconds=120):
            logger.debug("Waiting for {0}".format(self.controller.client_file))

        if not os.path.exists(self.controller.client_file):
            raise Exception("Controller client file is missing at {0}".format(self.controller.client_file))

        command_composer = self.compose_launch_cmd

        self.executor = Client(url_file=self.controller.client_file)
        if self.container_image:
            command_composer = self.compose_containerized_launch_cmd
            logger.info("Launching IPP with Docker image: {0}".format(self.container_image))

        self.launch_cmd = command_composer(self.engine_file, self.engine_dir, self.container_image)
        self.engines = []

        self._scaling_enabled = True
        logger.debug("Starting IPyParallelExecutor with provider:\n%s", self.provider)
        if hasattr(self.provider, 'init_blocks'):
            try:
                self.scale_out(blocks=self.provider.init_blocks)
            except Exception as e:
                logger.error("Scaling out failed: %s" % e)
                raise e

        self.lb_view = self.executor.load_balanced_view()
        logger.debug("Starting executor")

    def compose_launch_cmd(self, filepath, engine_dir, container_image):
        """Reads the json contents from filepath and uses that to compose the engine launch command.

        Args:
            filepath: Path to the engine file
            engine_dir: CWD for the engines

        """
        self.engine_file = os.path.expanduser(filepath)
        uid = str(uuid.uuid4())
        engine_json = None
        try:
            with open(self.engine_file, 'r') as f:
                engine_json = f.read()

        except OSError as e:
            logger.error("Could not open engine_json : ", self.engine_file)
            raise e

        return """mkdir -p {0}
cat <<EOF > {0}/ipengine.{uid}.json
{1}
EOF

mkdir -p '{0}/engine_logs'
ipengine --file={0}/ipengine.{uid}.json {debug_option} >> {0}/engine_logs/$JOBNAME.log 2>&1
""".format(engine_dir, engine_json, debug_option=self.debug_option, uid=uid)

    def compose_containerized_launch_cmd(self, filepath, engine_dir, container_image):
        """Reads the json contents from filepath and uses that to compose the engine launch command.

        Notes: Add this to the ipengine launch for debug logs :
                          --log-to-file --debug
        Args:
            filepath (str): Path to the engine file
            engine_dir (str): CWD for the engines .
            container_image (str): The container to be used to launch workers
        """
        self.engine_file = os.path.expanduser(filepath)
        uid = str(uuid.uuid4())
        engine_json = None
        try:
            with open(self.engine_file, 'r') as f:
                engine_json = f.read()

        except OSError as e:
            logger.error("Could not open engine_json : ", self.engine_file)
            raise e

        return """mkdir -p {0}
cd {0}
cat <<EOF > ipengine.{uid}.json
{1}
EOF

DOCKER_ID=$(docker create --network host {2} ipengine --file=/tmp/ipengine.{uid}.json) {debug_option}
docker cp ipengine.{uid}.json $DOCKER_ID:/tmp/ipengine.{uid}.json

# Copy current dir to the working directory
DOCKER_CWD=$(docker image inspect --format='{{{{.Config.WorkingDir}}}}' {2})
docker cp -a . $DOCKER_ID:$DOCKER_CWD
docker start $DOCKER_ID

at_exit() {{
  echo "Caught SIGTERM/SIGINT signal!"
  docker stop $DOCKER_ID
}}

trap at_exit SIGTERM SIGINT
sleep infinity
""".format(engine_dir, engine_json, container_image, debug_option=self.debug_option, uid=uid)

    @property
    def outstanding(self):
        return len(self.executor.outstanding)

    @property
    def connected_workers(self):
        return len(self.executor.ids)

    @property
    def scaling_enabled(self):
        return self._scaling_enabled

    def submit(self, *args, **kwargs):
        """Submits work to the thread pool.

        This method is simply pass through and behaves like a submit call as described
        here `Python docs: <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_

        Returns:
              Future
        """
        return self.lb_view.apply_async(*args, **kwargs)

    def scale_out(self, blocks=1):
        """Scales out the number of active workers by 1.

        This method is notImplemented for threads and will raise the error if called.

        Parameters:
            blocks : int
               Number of blocks to be provisioned.
        """
        r = []
        for i in range(blocks):
            if self.provider:
                block = self.provider.submit(self.launch_cmd, self.workers_per_node)
                logger.debug("Launched block {}:{}".format(i, block))
                if not block:
                    raise(ScalingFailed(self.provider.label,
                                        "Attempts to provision nodes via provider has failed"))
                self.engines.extend([block])
                r.extend([block])
        else:
            logger.error("No execution provider available")
            r = None

        return r

    def scale_in(self, blocks):
        """Scale in the number of active blocks by the specified number.

        """
        status = dict(zip(self.engines, self.provider.status(self.engines)))

        # This works for blocks=0
        to_kill = [engine for engine in status if status[engine] == "RUNNING"][:blocks]

        if self.provider:
            r = self.provider.cancel(to_kill)
        else:
            logger.error("No execution provider available")
            r = None

        return r

    def status(self):
        """Returns the status of the executor via probing the execution providers."""
        if self.provider:
            status = self.provider.status(self.engines)

        else:
            status = []

        return status

    def shutdown(self, hub=True, targets='all', block=False):
        """Shutdown the executor, including all workers and controllers.

        The interface documentation for IPP is `here <http://ipyparallel.readthedocs.io/en/latest/api/ipyparallel.html#ipyparallel.Client.shutdown>`_

        Kwargs:
            - hub (Bool): Whether the hub should be shutdown, Default:True,
            - targets (list of ints| 'all'): List of engine id's to kill, Default:'all'
            - block (Bool): To block for confirmations or not

        Raises:
             NotImplementedError
        """
        if self.controller:
            logger.debug("IPP:Shutdown sequence: Attempting controller kill")
            self.controller.close()

        # We do not actually do executor.shutdown because
        # this blocks even when requested to not block, killing the
        # controller is more effective although impolite.
        # x = self.executor.shutdown(targets=targets,
        #                           hub=hub,
        #                           block=block)

        logger.debug("Done with executor shutdown")
        return True


if __name__ == "__main__":

    pool1_config = {"poolname": "pool1",
                    "queue": "foo"}
