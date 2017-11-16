
def singleNodeLauncher (cmd_string, taskBlocks, walltime=None):
    ''' Worker launcher that wraps the job with the framework to launch
    multiple ipengines in parallel
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

