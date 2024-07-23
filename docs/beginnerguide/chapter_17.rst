17. Measuring Performance with parsl-perf
=========================================

Introduction to parsl-perf
--------------------------

`parsl-perf` is a tool that helps you measure how fast your Parsl setup can run. It does this by running a lot of simple tasks and seeing how many it can finish in a certain amount of time. This can help you find ways to make your Parsl programs run faster.

Setting Up and Using parsl-perf
-------------------------------

**Install parsl-perf**: If you haven't already, install parsl-perf using pip:

.. code-block:: bash

   pip install parsl-perf

**Create a configuration file**: This file tells `parsl-perf` how to set up Parsl. You can use the same configuration file that you use for your Parsl scripts.

**Run parsl-perf**: Open your terminal and run the following command:

.. code-block:: bash

   parsl-perf --config your_config_file.py

Replace `your_config_file.py` with the name of your configuration file.

`parsl-perf` will then run a series of tests, gradually increasing the number of tasks until it reaches a point where the tasks take a certain amount of time to complete (by default, 120 seconds).

Analyzing Performance Metrics
-----------------------------

`parsl-perf` will show you how many tasks it was able to complete per second. This number can give you an idea of how fast your Parsl setup is. If the number is low, it might mean that there are some bottlenecks in your configuration that you can try to fix.

Practical Example: Performance Tuning with parsl-perf
-----------------------------------------------------

Let's say you're running Parsl on a cluster, and you want to see if you can improve its performance. You can use `parsl-perf` to measure the current performance and then experiment with different configuration settings to see if you can get a higher number of tasks per second.

For example, you could try:

- Increasing the number of workers per node.
- Changing the type of executor you're using.
- Adjusting resource requirements for tasks.

By using `parsl-perf` to measure the impact of these changes, you can find the optimal configuration for your specific workload.
