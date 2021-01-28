from abc import ABCMeta, abstractmethod
import logging

from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class Launcher(RepresentationMixin, metaclass=ABCMeta):
    """Launchers are basically wrappers for user submitted scripts as they
    are submitted to a specific execution resource.
    """
    def __init__(self, debug: bool = True):
        self.debug = debug

    @abstractmethod
    def __call__(self, command, tasks_per_node, nodes_per_block):
        """ Wraps the command with the Launcher calls.
        """
        pass


class SimpleLauncher(Launcher):
    """ Does no wrapping. Just returns the command as-is
    """
    def __init_(self, debug: bool = True):
        super().__init__(debug=debug)

    def __call__(self, command, tasks_per_node, nodes_per_block):
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

        """
        return command


class WrappedLauncher(Launcher):
    """Wraps the command by prepending commands before a user's command

    As an example, the wrapped launcher can be used to launch a command
    inside a docker container by prepending the proper docker invocation"""

    def __init__(self, prepend: str, debug: bool = True):
        """
        Args:
             prepend (str): Command to use before the launcher (e.g., ``time``)
        """
        super().__init__(debug=debug)
        self.prepend = prepend

    def __call__(self, command, tasks_per_node, nodes_per_block, debug=True):
        if tasks_per_node > 1:
            logger.warning('WrappedLauncher ignores the number of tasks per node. '
                           'You may be getting fewer workers than expected')
        if nodes_per_block > 1:
            logger.warning('WrappedLauncher ignores the number of nodes per block. '
                           'You may be getting fewer workers than expected')
        return "{0} {1}".format(self.prepend, command)


class SingleNodeLauncher(Launcher):
    """ Worker launcher that wraps the user's command with the framework to
    launch multiple command invocations in parallel. This wrapper sets the
    bash env variable CORES to the number of cores on the machine. By setting
    task_blocks to an integer or to a bash expression the number of invocations
    of the command to be launched can be controlled.
    """
    def __init__(self, debug: bool = True, fail_on_any: bool = False):
        super().__init__(debug=debug)
        self.fail_on_any = fail_on_any

    def __call__(self, command, tasks_per_node, nodes_per_block):
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.
        - fail_on_any: If True, return a nonzero exit code if any worker failed, otherwise zero;
                       if False, return a nonzero exit code if all workers failed, otherwise zero.

        """
        task_blocks = tasks_per_node * nodes_per_block
        fail_on_any_num = int(self.fail_on_any)
        debug_num = int(self.debug)

        x = '''set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "{debug}" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT={task_blocks}
FAILONANY={fail_on_any}
PIDS=""

CMD() {{
{command}
}}
for COUNT in $(seq 1 1 $WORKERCOUNT); do
    [[ "{debug}" == "1" ]] && echo "Launching worker: $COUNT"
    CMD $COUNT &
    PIDS="$PIDS $!"
done

ALLFAILED=1
ANYFAILED=0
for PID in $PIDS ; do
    wait $PID
    if [ "$?" != "0" ]; then
        ANYFAILED=1
    else
        ALLFAILED=0
    fi
done

[[ "{debug}" == "1" ]] && echo "All workers done"
if [ "$FAILONANY" == "1" ]; then
    exit $ANYFAILED
else
    exit $ALLFAILED
fi
'''.format(command=command,
           task_blocks=task_blocks,
           debug=debug_num,
           fail_on_any=fail_on_any_num)
        return x


class GnuParallelLauncher(Launcher):
    """ Worker launcher that wraps the user's command with the framework to
    launch multiple command invocations via GNU parallel sshlogin.

    This wrapper sets the bash env variable CORES to the number of cores on the
    machine.

    This launcher makes the following assumptions:

    - GNU parallel is installed and can be located in $PATH
    - Paswordless SSH login is configured between the controller node and the
      target nodes.
    - The provider makes available the $PBS_NODEFILE environment variable
    """
    def __init__(self, debug: bool = True):
        super().__init__(debug=debug)

    def __call__(self, command, tasks_per_node, nodes_per_block):
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

        """
        task_blocks = tasks_per_node * nodes_per_block
        debug_num = int(self.debug)

        x = '''set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "{debug}" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT={task_blocks}

# Deduplicate the nodefile
SSHLOGINFILE="$JOBNAME.nodes"
if [ -z "$PBS_NODEFILE" ]; then
    echo "localhost" > $SSHLOGINFILE
else
    sort -u $PBS_NODEFILE > $SSHLOGINFILE
fi

cat << PARALLEL_CMD_EOF > cmd_$JOBNAME.sh
{command}
PARALLEL_CMD_EOF
chmod u+x cmd_$JOBNAME.sh

#file to contain the commands to parallel
PFILE=cmd_${{JOBNAME}}.sh.parallel

# Truncate the file
cp /dev/null $PFILE

for COUNT in $(seq 1 1 $WORKERCOUNT)
do
    echo "sh cmd_$JOBNAME.sh" >> $PFILE
done

parallel --env _ --joblog "$JOBNAME.sh.parallel.log" \
    --sshloginfile $SSHLOGINFILE --jobs {tasks_per_node} < $PFILE

[[ "{debug}" == "1" ]] && echo "All workers done"
'''.format(command=command,
           tasks_per_node=tasks_per_node,
           task_blocks=task_blocks,
           debug=debug_num)
        return x


class MpiExecLauncher(Launcher):
    """ Worker launcher that wraps the user's command with the framework to
    launch multiple command invocations via mpiexec.

    This wrapper sets the bash env variable CORES to the number of cores on the
    machine.

    This launcher makes the following assumptions:
    - mpiexec is installed and can be located in $PATH
    - The provider makes available the $PBS_NODEFILE environment variable
    """
    def __init__(self, debug: bool = True):
        super().__init__(debug=debug)

    def __call__(self, command, tasks_per_node, nodes_per_block):
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

        """
        task_blocks = tasks_per_node * nodes_per_block
        debug_num = int(self.debug)

        x = '''set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "{debug}" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT={task_blocks}

# Deduplicate the nodefile
HOSTFILE="$JOBNAME.nodes"
if [ -z "$PBS_NODEFILE" ]; then
    echo "localhost" > $HOSTFILE
else
    sort -u $PBS_NODEFILE > $HOSTFILE
fi

cat << MPIEXEC_EOF > cmd_$JOBNAME.sh
{command}
MPIEXEC_EOF
chmod u+x cmd_$JOBNAME.sh

mpiexec --bind-to none -n $WORKERCOUNT --hostfile $HOSTFILE /usr/bin/sh cmd_$JOBNAME.sh

[[ "{debug}" == "1" ]] && echo "All workers done"
'''.format(command=command,
           task_blocks=task_blocks,
           debug=debug_num)
        return x


class MpiRunLauncher(Launcher):
    """ Worker launcher that wraps the user's command with the framework to
    launch multiple command invocations via mpirun.

    This wrapper sets the bash env variable CORES to the number of cores on the
    machine.

    This launcher makes the following assumptions:
    - mpirun is installed and can be located in $PATH
    - The provider makes available the $PBS_NODEFILE environment variable
    """
    def __init__(self, debug: bool = True, bash_location: str = '/bin/bash'):
        super().__init__(debug=debug)
        self.bash_location = bash_location

    def __call__(self, command, tasks_per_node, nodes_per_block):
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

        """
        task_blocks = tasks_per_node * nodes_per_block
        debug_num = int(self.debug)

        x = '''set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "{debug}" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT={task_blocks}

cat << MPIRUN_EOF > cmd_$JOBNAME.sh
{command}
MPIRUN_EOF
chmod u+x cmd_$JOBNAME.sh

mpirun -np $WORKERCOUNT {bash_location} cmd_$JOBNAME.sh

[[ "{debug}" == "1" ]] && echo "All workers done"
'''.format(command=command,
           task_blocks=task_blocks,
           bash_location=self.bash_location,
           debug=debug_num)
        return x


class SrunLauncher(Launcher):
    """ Worker launcher that wraps the user's command with the SRUN launch framework
    to launch multiple cmd invocations in parallel on a single job allocation.
    """

    def __init__(self, debug: bool = True, overrides: str = ''):
        """
        Parameters
        ----------

        overrides: str
             This string will be passed to the srun launcher. Default: ''
        """

        super().__init__(debug=debug)
        self.overrides = overrides

    def __call__(self, command, tasks_per_node, nodes_per_block):
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

        """
        task_blocks = tasks_per_node * nodes_per_block
        debug_num = int(self.debug)

        x = '''set -e
export CORES=$SLURM_CPUS_ON_NODE
export NODES=$SLURM_JOB_NUM_NODES

[[ "{debug}" == "1" ]] && echo "Found cores : $CORES"
[[ "{debug}" == "1" ]] && echo "Found nodes : $NODES"
WORKERCOUNT={task_blocks}

cat << SLURM_EOF > cmd_$SLURM_JOB_NAME.sh
{command}
SLURM_EOF
chmod a+x cmd_$SLURM_JOB_NAME.sh

srun --ntasks {task_blocks} -l {overrides} bash cmd_$SLURM_JOB_NAME.sh

[[ "{debug}" == "1" ]] && echo "Done"
'''.format(command=command,
           task_blocks=task_blocks,
           overrides=self.overrides,
           debug=debug_num)
        return x


class SrunMPILauncher(Launcher):
    """Launches as many workers as MPI tasks to be executed concurrently within a block.

    Use this launcher instead of SrunLauncher if each block will execute multiple MPI applications
    at the same time. Workers should be launched with independent Srun calls so as to setup the
    environment for MPI application launch.
    """
    def __init__(self, debug: bool = True, overrides: str = ''):
        """
        Parameters
        ----------

        overrides: str
             This string will be passed to the launcher. Default: ''
        """

        super().__init__(debug=debug)
        self.overrides = overrides

    def __call__(self, command, tasks_per_node, nodes_per_block):
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

        """
        task_blocks = tasks_per_node * nodes_per_block
        debug_num = int(self.debug)

        x = '''set -e
export CORES=$SLURM_CPUS_ON_NODE
export NODES=$SLURM_JOB_NUM_NODES

[[ "{debug}" == "1" ]] && echo "Found cores : $CORES"
[[ "{debug}" == "1" ]] && echo "Found nodes : $NODES"
WORKERCOUNT={task_blocks}

cat << SLURM_EOF > cmd_$SLURM_JOB_NAME.sh
{command}
SLURM_EOF
chmod a+x cmd_$SLURM_JOB_NAME.sh

TASKBLOCKS={task_blocks}

# If there are more taskblocks to be launched than nodes use
if (( "$TASKBLOCKS" > "$NODES" ))
then
    [[ "{debug}" == "1" ]] && echo "TaskBlocks:$TASKBLOCKS > Nodes:$NODES"
    CORES_PER_BLOCK=$(($NODES * $CORES / $TASKBLOCKS))
    for blk in $(seq 1 1 $TASKBLOCKS):
    do
        srun --ntasks $CORES_PER_BLOCK -l {overrides} bash cmd_$SLURM_JOB_NAME.sh &
    done
    wait
else
    # A Task block could be integer multiples of Nodes
    [[ "{debug}" == "1" ]] && echo "TaskBlocks:$TASKBLOCKS <= Nodes:$NODES"
    NODES_PER_BLOCK=$(( $NODES / $TASKBLOCKS ))
    for blk in $(seq 1 1 $TASKBLOCKS):
    do
        srun --exclusive --nodes $NODES_PER_BLOCK -l {overrides} bash cmd_$SLURM_JOB_NAME.sh &
    done
    wait

fi


[[ "{debug}" == "1" ]] && echo "Done"
'''.format(command=command,
           task_blocks=task_blocks,
           overrides=self.overrides,
           debug=debug_num)
        return x


class AprunLauncher(Launcher):
    """  Worker launcher that wraps the user's command with the Aprun launch framework
    to launch multiple cmd invocations in parallel on a single job allocation

    """
    def __init__(self, debug: bool = True, overrides: str = ''):
        """
        Parameters
        ----------

        overrides: str
             This string will be passed to the aprun launcher. Default: ''
        """
        super().__init__(debug=debug)
        self.overrides = overrides

    def __call__(self, command, tasks_per_node, nodes_per_block):
        """
        Args:
        - command (string): The command string to be launched
        - tasks_per_node (int) : Workers to launch per node
        - nodes_per_block (int) : Number of nodes in a block

        """

        tasks_per_block = tasks_per_node * nodes_per_block
        debug_num = int(self.debug)

        x = '''set -e
WORKERCOUNT={tasks_per_block}

cat << APRUN_EOF > cmd_$JOBNAME.sh
{command}
APRUN_EOF
chmod a+x cmd_$JOBNAME.sh

aprun -n {tasks_per_block} -N {tasks_per_node} {overrides} /bin/bash cmd_$JOBNAME.sh &
wait

[[ "{debug}" == "1" ]] && echo "Done"
'''.format(command=command,
           tasks_per_block=tasks_per_block,
           tasks_per_node=tasks_per_node,
           overrides=self.overrides,
           debug=debug_num)
        return x


class JsrunLauncher(Launcher):
    """  Worker launcher that wraps the user's command with the Jsrun launch framework
    to launch multiple cmd invocations in parallel on a single job allocation

    """
    def __init__(self, debug: bool = True, overrides: str = ''):
        """
        Parameters
        ----------

        overrides: str
             This string will be passed to the JSrun launcher. Default: ''
        """
        super().__init__(debug=debug)
        self.overrides = overrides

    def __call__(self, command, tasks_per_node, nodes_per_block):
        """
        Args:
        - command (string): The command string to be launched
        - tasks_per_node (int) : Workers to launch per node
        - nodes_per_block (int) : Number of nodes in a block

        """

        tasks_per_block = tasks_per_node * nodes_per_block
        debug_num = int(self.debug)

        x = '''set -e
WORKERCOUNT={tasks_per_block}

cat << JSRUN_EOF > cmd_$JOBNAME.sh
{command}
JSRUN_EOF
chmod a+x cmd_$JOBNAME.sh

jsrun -n {tasks_per_block} -r {tasks_per_node} {overrides} /bin/bash cmd_$JOBNAME.sh &
wait

[[ "{debug}" == "1" ]] && echo "Done"
'''.format(command=command,
           tasks_per_block=tasks_per_block,
           tasks_per_node=tasks_per_node,
           overrides=self.overrides,
           debug=debug_num)
        return x
