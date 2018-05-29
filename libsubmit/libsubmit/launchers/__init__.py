from libsubmit.launchers.launchers import single_node_launcher
from libsubmit.launchers.launchers import srun_launcher
from libsubmit.launchers.launchers import srun_mpi_launcher
from libsubmit.launchers.launchers import aprun_launcher

launchers = {
    "single_node": single_node_launcher,
    "srun": srun_launcher,
    "aprun": aprun_launcher,
    "srun_mpi": srun_mpi_launcher
}
