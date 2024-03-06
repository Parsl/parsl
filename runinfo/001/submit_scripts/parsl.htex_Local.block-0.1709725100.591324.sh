
export JOBNAME=$parsl.htex_Local.block-0.1709725100.591324
set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "1" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT=1
FAILONANY=0
PIDS=""

CMD() {
process_worker_pool.py --debug  -a 127.0.0.1 -p 0 -c 1 -m None --poll 100 --task_port=54996 --result_port=54967 --cert_dir /home/work/Open-Source/parsl-fork/runinfo/001/htex_Local/certificates --logdir=/home/work/Open-Source/parsl-fork/runinfo/001/htex_Local --block_id=0 --hb_period=2  --hb_threshold=5 --cpu-affinity none  --mpi-launcher=mpiexec --available-accelerators 
}
for COUNT in $(seq 1 1 $WORKERCOUNT); do
    [[ "1" == "1" ]] && echo "Launching worker: $COUNT"
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

[[ "1" == "1" ]] && echo "All workers done"
if [ "$FAILONANY" == "1" ]; then
    exit $ANYFAILED
else
    exit $ALLFAILED
fi
