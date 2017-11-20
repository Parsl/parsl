
def singleNodeLauncher (cmd_string, taskBlocks, walltime=None):
    ''' Worker launcher that wraps the user's cmd_string with the framework to
    launch multiple cmd_string invocations in parallel. This wrapper sets the
    bash env variable CORES to the number of cores on the machine. By setting
    taskBlocks to an integer or to a bash expression the number of invocations
    of the cmd_string to be launched can be controlled.

    Args:
        - cmd_string (string): The command string to be launched
        - taskBlock (string) : bash evaluated string.

    KWargs:
        - walltime (int) : This is not used by this launcher.
    '''

    x = '''export CORES=$(grep -c ^processor /proc/cpuinfo)
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
'''.format(cmd_string, taskBlocks)
    return x

