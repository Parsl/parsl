"""HighThroughputExecutor builds on the Swift/T EMEWS architecture to use MPI for fast task distribution
"""

import logging

try:
    import mpi4py
except ImportError:
    _mpi_enabled = False
else:
    _mpi_enabled = True

from parsl.errors import *
from parsl.executors.errors import *
from parsl.executors.high_throughput.executor import HighThroughputExecutor

from parsl.utils import RepresentationMixin
from parsl.providers import Local

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
    running over a `block` and that there might be several such instances.

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
       Provider to access computation resources. Can be any providers in `parsl.providers`:
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

    launch_cmd : str
        Command line string to launch the mpi_worker_pool from the provider.

    public_ip : string
        Set the public ip of the machine on which Parsl is executing.

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
    """

    def __init__(self,
                 label='ExtremeScaleExecutor',
                 provider=Local(),
                 launch_cmd=None,
                 public_ip="127.0.0.1",
                 worker_ports=None,
                 worker_port_range=(54000, 55000),
                 interchange_port_range=(55000, 56000),
                 storage_access=None,
                 working_dir=None,
                 worker_debug=False,
                 managed=True):

        super().__init__(label=label,
                         provider=provider,
                         launch_cmd=launch_cmd,
                         public_ip=public_ip,
                         worker_ports=worker_ports,
                         worker_port_range=worker_port_range,
                         interchange_port_range=interchange_port_range,
                         storage_access=storage_access,
                         working_dir=working_dir,
                         worker_debug=worker_debug,
                         managed=managed)

        if not _mpi_enabled:
            raise OptionalModuleMissing("mpi4py", "Cannot initialize ExtremeScaleExecutor without mpi4py")
        else:
            # This is only to stop flake8 from complaining
            logger.debug("MPI version :{}".format(mpi4py.__version__))

        logger.debug("Initializing ExtremeScaleExecutor")

        if not launch_cmd:
            self.launch_cmd = """mpiexec -np {tasks_per_node} mpi_worker_pool.py {debug} --task_url={task_url} --result_url={result_url}"""
