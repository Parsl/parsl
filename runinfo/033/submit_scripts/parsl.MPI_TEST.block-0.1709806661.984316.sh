
export JOBNAME=$parsl.MPI_TEST.block-0.1709806661.984316
process_worker_pool.py --debug --max_workers_per_node=1 -a 182.191.134.121,172.17.11.0,127.0.0.1,127.0.1.1 -p 0 -c 1 -m None --poll 10 --task_port=54218 --result_port=54592 --cert_dir /home/zakia/parsl/runinfo/033/MPI_TEST/certificates --logdir=/home/zakia/parsl/runinfo/033/MPI_TEST --block_id=0 --hb_period=30  --hb_threshold=120 --cpu-affinity none  --mpi-launcher=mpiexec --available-accelerators 