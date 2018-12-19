
export CORES=$(getconf _NPROCESSORS_ONLN)
echo "Found cores : $CORES"
WORKERCOUNT=1

CMD ( ) {
process_worker_pool.py --debug -c 1 --task_url=tcp://127.0.0.1:54787 --result_url=tcp://127.0.0.1:54954 --logdir=/tmp/parsl/parsl/tests/manual_tests/runinfo/007/htex_local --hb_period=30 --hb_threshold=120 
}
for COUNT in $(seq 1 1 $WORKERCOUNT)
do
    echo "Launching worker: $COUNT"
    CMD &
done
wait
echo "All workers done"
