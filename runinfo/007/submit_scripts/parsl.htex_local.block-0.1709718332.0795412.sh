
export JOBNAME=$parsl.htex_local.block-0.1709718332.0795412
set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "1" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT=1
FAILONANY=0
PIDS=""

CMD() {
process_worker_pool.py  --max_workers_per_node=5 -a 182.191.141.188,172.17.11.0,127.0.0.1,127.0.1.1 -p 0 -c 1 -m None --poll 10 --task_port=54306 --result_port=54890 --cert_dir /home/zakia/parsl/runinfo/007/htex_local/certificates --logdir=/home/zakia/parsl/runinfo/007/htex_local --block_id=0 --hb_period=30  --hb_threshold=120 --cpu-affinity none  --mpi-launcher=mpiexec --available-accelerators 
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
