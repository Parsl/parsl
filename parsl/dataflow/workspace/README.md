# FOR CORRECT SCALEING UP/DOWN
  `max_blocks` should be more than 1 otherwise user cannot get merties from new strategies

# TODO 
* Debug: Feb. 25/ Takuya. Local application suspends long-time without time.sleep <br />
  ==> should fid ASAP <br />
  This occurs somtimes in this time, but now ??

* Debug: Feb. 26/ Takuya. FIXME:/parsl/dataflow/strategy.py, def _htex_strategy_totaltime  <br />
   Error line; `tasks_per_node = connected_workers[0]['worker_count']` <br />
   Error Msg;  TypeError: tuple indices must be integers or slices, not str <br />
   Return object from `connected_workers` ==>  `[('df15cd1e9f13', 0, True)]` 

* Debug: Feb. 26/ Takuya. Slurm/Local job does not finish automatically. Why??!  <br />

* Debug: Feb. 28/ Hai. Strategy Debug 90% Done.  <br />

* TODO: Feb. 28/ Theo & Tak. Think metrics and Dev. application for benchmark .  <br />

# List of Programs

1. test_cio_parsl.py  <br />
  Test script to check whether strategy works normal. Write "Hello World" on your termianl.

2. cpu_stress_midway.py  <br />
  Test script for midway

3. cpu_stress.py  <br />
  Ver.0 test script. Vec-computation for loner loop with time.sleep. <br />
  With plotting lines for CPU Util and Memmory 

4. test_strategy.py <br />
  TBD...

5. job.bash <br />
  JOB  Script for slurm batch job on cluster.

6. cpu_stress_midway_slurm.py  <br />
  Test script for midway on slurm 

7. app_dag_zhuozhao.py <br />
  Dataflow python application from Zhuzhao
 
8. app_dev_dag_zhuozhao.py <br />
  Developed Dataflow python application based on app_dag_zhuozhao.py

9. app_slurm_dag_zhuozhao.py
  Dataflow script on slurm job

10. job_slurm_app_parallel.bash
  Dataflow script(app_slurm_dag.py ) on slurm job
