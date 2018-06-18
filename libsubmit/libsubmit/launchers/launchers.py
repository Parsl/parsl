def single_node_launcher(command, task_blocks, walltime=None):
    ''' Worker launcher that wraps the user's command with the framework to
    launch multiple command invocations in parallel. This wrapper sets the
    bash env variable CORES to the number of cores on the machine. By setting
    task_blocks to an integer or to a bash expression the number of invocations
    of the command to be launched can be controlled.

    Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

    KWargs:
        - walltime (int) : This is not used by this launcher.
    '''

    x = '''export CORES=$(getconf _NPROCESSORS_ONLN)
echo "Found cores : $CORES"
WORKERCOUNT={1}

CMD ( ) {{
{0}
}}

for COUNT in $(seq 1 1 $WORKERCOUNT)
do
    echo "Launching worker: $COUNT"
    CMD &
done
wait
echo "All workers done"
'''.format(command, task_blocks)
    return x


def srun_launcher(command, task_blocks, walltime=None):
    ''' Worker launcher that wraps the user's command with the SRUN launch framework
    to launch multiple cmd invocations in parallel on a single job allocation.

    Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

    KWargs:
        - walltime (int) : This is not used by this launcher.
    '''

    x = '''export CORES=$SLURM_CPUS_ON_NODE
export NODES=$SLURM_JOB_NUM_NODES

echo "Found cores : $CORES"
echo "Found nodes : $NODES"
WORKERCOUNT={1}

cat << SLURM_EOF > cmd_$SLURM_JOB_NAME.sh
{0}
SLURM_EOF
chmod a+x cmd_$SLURM_JOB_NAME.sh

TASKBLOCKS={1}

srun --ntasks $TASKBLOCKS -l bash cmd_$SLURM_JOB_NAME.sh

echo "Done"
'''.format(command, task_blocks)
    return x


def srun_mpi_launcher(command, task_blocks, walltime=None):
    ''' Worker launcher that wraps the user's command with the SRUN launch framework
    to launch multiple cmd invocations in parallel on a single job allocation.

    Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

    KWargs:
        - walltime (int) : This is not used by this launcher.
    '''

    x = '''export CORES=$SLURM_CPUS_ON_NODE
export NODES=$SLURM_JOB_NUM_NODES

echo "Found cores : $CORES"
echo "Found nodes : $NODES"
WORKERCOUNT={1}

cat << SLURM_EOF > cmd_$SLURM_JOB_NAME.sh
{0}
SLURM_EOF
chmod a+x cmd_$SLURM_JOB_NAME.sh

TASKBLOCKS={1}

# If there are more taskblocks to be launched than nodes use
if (( "$TASKBLOCKS" > "$NODES" ))
then
    echo "TaskBlocks:$TASKBLOCKS > Nodes:$NODES"
    CORES_PER_BLOCK=$(($NODES * $CORES / $TASKBLOCKS))
    for blk in $(seq 1 1 $TASKBLOCKS):
    do
        srun --ntasks $CORES_PER_BLOCK -l bash cmd_$SLURM_JOB_NAME.sh &
    done
    wait
else
    # A Task block could be integer multiples of Nodes
    echo "TaskBlocks:$TASKBLOCKS <= Nodes:$NODES"
    NODES_PER_BLOCK=$(( $NODES / $TASKBLOCKS ))
    for blk in $(seq 1 1 $TASKBLOCKS):
    do
        srun --exclusive --nodes $NODES_PER_BLOCK -l bash cmd_$SLURM_JOB_NAME.sh &
    done
    wait

fi


echo "Done"
'''.format(command, task_blocks)
    return x


def aprun_launcher(command, task_blocks, walltime=None):
    ''' Worker launcher that wraps the user's command with the Aprun launch framework
    to launch multiple cmd invocations in parallel on a single job allocation.

    Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

    KWargs:
        - walltime (int) : This is not used by this launcher.
    '''

    x = '''
WORKERCOUNT={1}

cat << APRUN_EOF > cmd_$JOBNAME.sh
{0}
APRUN_EOF
chmod a+x cmd_$JOBNAME.sh

TASKBLOCKS={1}

for i in $(seq 1 1 $TASKBLOCKS):
do
    aprun /bin/bash cmd_$JOBNAME.sh &
done
wait

echo "Done"
'''.format(command, task_blocks)
    return x
