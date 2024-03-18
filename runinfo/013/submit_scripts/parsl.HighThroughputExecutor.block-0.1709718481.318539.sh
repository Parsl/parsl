
export JOBNAME=$parsl.HighThroughputExecutor.block-0.1709718481.318539
set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "1" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT=1
FAILONANY=0
PIDS=""

CMD() {
process_worker_pool.py   -a 172.17.11.0,127.0.0.1,127.0.1.1,182.191.128.92 -p 0 -c 1.0 -m None --poll 10 --task_port=54719 --result_port=54704 --cert_dir /home/zakia/parsl/.pytest/parsltest-current/test_htex_start_encrypted_True-True_-v9c3o8gb/provided/certificates --logdir=/home/zakia/parsl/.pytest/parsltest-current/test_htex_start_encrypted_True-True_-v9c3o8gb/HighThroughputExecutor --block_id=0 --hb_period=30  --hb_threshold=120 --cpu-affinity none  --mpi-launcher=mpiexec --available-accelerators 
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
