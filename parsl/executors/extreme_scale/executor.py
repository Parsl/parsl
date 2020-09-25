"""HighThroughputExecutor builds on the Swift/T EMEWS architecture to use MPI for fast task distribution
"""

import logging

try:
    import mpi4py
except ImportError:
    _mpi_enabled = False
else:
    _mpi_enabled = True

from parsl.errors import OptionalModuleMissing
from parsl.executors.high_throughput.executor import HighThroughputExecutor

from parsl.utils import RepresentationMixin
from parsl.providers import LocalProvider

logger = logging.getLogger(__name__)


class ExtremeScaleExecutor(HighThroughputExecutor, RepresentationMixin):
    """Executor designed for leadership class supercomputer scale

    The ExtremeScaleExecutor extends the Executor interface to enable task execution on
    supercomputing systems (>1K Nodes). When functions and their arguments are submitted
    to the interface, a future is returned that tracks the execution of the function on
    a distributed compute environment.

    The ExtremeScaleExecutor system has the following components:
      1. The ExtremeScaleExecutor instance which is run as part of the Parsl script
      2. The Interchange which is acts as a load-balancing proxy between workers and Parsl
      3. The MPI based mpi_worker_pool which coordinates task execution over several nodes
         With MPI communication between workers, we can exploit low latency networking on
         HPC systems.
      4. ZeroMQ pipes that connect the ExtremeScaleExecutor, Interchange and the mpi_worker_pool

    Our design assumes that there is a single MPI application (mpi_worker_pool)
    running over a ``block`` and that there might be several such instances.

    Here is a diagram

    .. code:: python


                        |  Data   |  Executor   |  Interchange  | External Process(es)
                        |  Flow   |             |               |
                   Task | Kernel  |             |               |
                 +----->|-------->|------------>|->outgoing_q---|-> mpi_worker_pool
                 |      |         |             | batching      |    |         |
           Parsl<---Fut-|         |             | load-balancing|  result   exception
                     ^  |         |             | watchdogs     |    |         |
                     |  |         |   Q_mngmnt  |               |    V         V
                     |  |         |    Thread<--|-incoming_q<---|--- +---------+
                     |  |         |      |      |               |
                     |  |         |      |      |               |
                     +----update_fut-----+


    Parameters
    ----------

    provider : :class:`~parsl.providers.provider_base.ExecutionProvider`
       Provider to access computation resources. Can be any providers in ``parsl.providers``:
        :class:`~parsl.providers.cobalt.cobalt.Cobalt`,
        :class:`~parsl.providers.condor.condor.Condor`,
        :class:`~parsl.providers.googlecloud.googlecloud.GoogleCloud`,
        :class:`~parsl.providers.gridEngine.gridEngine.GridEngine`,
        :class:`~parsl.providers.local.local.Local`,
        :class:`~parsl.providers.sge.sge.GridEngine`,
        :class:`~parsl.providers.slurm.slurm.Slurm`, or
        :class:`~parsl.providers.torque.torque.Torque`.

    label : str
        Label for this executor instance.

    launch_cmd : str
        Command line string to launch the mpi_worker_pool from the provider.
        The command line string will be formatted with appropriate values for the following values (debug, task_url, result_url,
        ranks_per_node, nodes_per_block, heartbeat_period ,heartbeat_threshold, logdir). For example:
        launch_cmd="mpiexec -np {ranks_per_node} mpi_worker_pool.py {debug} --task_url={task_url} --result_url={result_url}"

    address : string
        An address to connect to the main Parsl process which is reachable from the network in which
        workers will be running. This can be either a hostname as returned by ``hostname`` or an
        IP address. Most login nodes on clusters have several network interfaces available, only
        some of which can be reached from the compute nodes.  Some trial and error might be
        necessary to identify what addresses are reachable from compute nodes.

    worker_ports : (int, int)
        Specify the ports to be used by workers to connect to Parsl. If this option is specified,
        worker_port_range will not be honored.

    worker_port_range : (int, int)
        Worker ports will be chosen between the two integers provided.

    interchange_port_range : (int, int)
        Port range used by Parsl to communicate with the Interchange.

    working_dir : str
        Working dir to be used by the executor.

    worker_debug : Bool
        Enables engine debug logging.

    managed : Bool
        If this executor is managed by the DFK or externally handled.

    ranks_per_node : int
        Specify the ranks to be launched per node.

    heartbeat_threshold : int
        Seconds since the last message from the counterpart in the communication pair:
        (interchange, manager) after which the counterpart is assumed to be un-available. Default:120s

    heartbeat_period : int
        Number of seconds after which a heartbeat message indicating liveness is sent to the
        counterpart (interchange, manager). Default:30s

    """

    def __init__(self,
                 label='ExtremeScaleExecutor',
                 provider=LocalProvider(),
                 launch_cmd=None,
                 address="127.0.0.1",
                 worker_ports=None,
                 worker_port_range=(54000, 55000),
                 interchange_port_range=(55000, 56000),
                 storage_access=None,
                 working_dir=None,
                 worker_debug=False,
                 ranks_per_node=1,
                 heartbeat_threshold=120,
                 heartbeat_period=30,
                 managed=True):

        super().__init__(label=label,
                         provider=provider,
                         launch_cmd=launch_cmd,
                         address=address,
                         worker_ports=worker_ports,
                         worker_port_range=worker_port_range,
                         interchange_port_range=interchange_port_range,
                         storage_access=storage_access,
                         working_dir=working_dir,
                         worker_debug=worker_debug,
                         heartbeat_threshold=heartbeat_threshold,
                         heartbeat_period=heartbeat_period,
                         managed=managed)

        self.ranks_per_node = ranks_per_node

        logger.debug("Initializing ExtremeScaleExecutor")

        if not launch_cmd:
            self.launch_cmd = ("mpiexec -np {ranks_per_node} mpi_worker_pool.py "
                               "{debug} "
                               "--task_url={task_url} "
                               "--result_url={result_url} "
                               "--logdir={logdir} "
                               "--hb_period={heartbeat_period} "
                               "--hb_threshold={heartbeat_threshold} ")
        self.worker_debug = worker_debug

    def start(self):
        if not _mpi_enabled:
            raise OptionalModuleMissing("mpi4py", "Cannot initialize ExtremeScaleExecutor without mpi4py")
        else:
            # This is only to stop flake8 from complaining
            logger.debug("MPI version :{}".format(mpi4py.__version__))

        super().start()

    def initialize_scaling(self):

        debug_opts = "--debug" if self.worker_debug else ""
        l_cmd = self.launch_cmd.format(debug=debug_opts,
                                       task_url="tcp://{}:{}".format(self.address,
                                                                     self.worker_task_port),
                                       result_url="tcp://{}:{}".format(self.address,
                                                                       self.worker_result_port),
                                       cores_per_worker=self.cores_per_worker,
                                       # This is here only to support the exex mpiexec call
                                       ranks_per_node=self.ranks_per_node,
                                       nodes_per_block=self.provider.nodes_per_block,
                                       heartbeat_period=self.heartbeat_period,
                                       heartbeat_threshold=self.heartbeat_threshold,
                                       logdir="{}/{}".format(self.run_dir, self.label))
        self.launch_cmd = l_cmd
        logger.debug("Launch command: {}".format(self.launch_cmd))

        self._scaling_enabled = True
        logger.debug("Starting ExtremeScaleExecutor with provider:\n%s", self.provider)
        if hasattr(self.provider, 'init_blocks'):
            try:
                self.scale_out(blocks=self.provider.init_blocks)

            except Exception as e:
                logger.error("Scaling out failed: {}".format(e))
                raise e
