What is this test
=================

This simple test measures the time taken to launch single tasks and receive results from an executor.

We will call get the result back after launch as ``latency`` and the combined measure of time to launch
and the time to get the result back as ``roundtrip time``.


How do we measure
=================

1. We start the executor
2. Run 100 tasks and wait for them to prime the executors and the ensure that workers are online
3. Run the actual measurements:
     1) Time to launch the app
     2) Time to receive result.

4. Take the min, max, and mean of ``latency`` and ``roundtrip``.


Preliminary results
==================

Results from running on Midway with IPP executor.

Latency   |   Min:0.005968570709228516 Max:0.011006593704223633 Average:0.0065019774436950685
Roundtrip |   Min:0.00716400146484375  Max:0.012288331985473633 Average:0.007741005420684815
