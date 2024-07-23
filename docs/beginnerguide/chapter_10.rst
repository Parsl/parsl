10. Memoization and Checkpointing
=================================

Parsl offers two ways to make your work faster and more reliable: remembering past results (app caching) and saving progress (checkpointing).

App Caching
-----------

App caching means that Parsl remembers the results of tasks it has already done. If you ask Parsl to do the same task again with the same inputs, it will simply give you the old answer instead of redoing the work. This can save a lot of time, especially for tasks that take a long time to finish. Parsl figures out if tasks are the same by comparing the task itself and the inputs you give it. If you change the task's code or the inputs, Parsl will know it's a different task and will run it again. You can also tell Parsl to ignore certain inputs when deciding if tasks are the same. This is helpful for things that don't change the result, like log file names or timestamps.

.. note:: 
   App caching works best for tasks that always give the same result for the same inputs. If your task has random elements, caching might not be a good idea.

Checkpointing
-------------

Checkpointing is like saving your progress in a video game. If something goes wrong, you can start again from where you left off instead of starting from the beginning. Parsl can automatically save checkpoints or you can tell it when to save them. To use checkpoints, you need to tell Parsl where to find the checkpoint files. Parsl will then load the saved results and use them for any tasks that have been done before with the same inputs.

Practical Tutorial: Using Memoization and Checkpointing
-------------------------------------------------------

.. code-block:: python

   import parsl
   from parsl import python_app, Config
   from parsl.executors import ThreadPoolExecutor

   config = Config(executors=[ThreadPoolExecutor(max_threads=4)])
   parsl.load(config)

   @python_app(cache=True)  # Tell Parsl to remember results
   def long_task(x):
       # Pretend this task takes a long time
       import time
       time.sleep(5)
       return x**2

   # First run: tasks are done and results are remembered
   results = []
   for i in range(5):
       results.append(long_task(i))
   print([r.result() for r in results])

   # Second run: Parsl remembers the results and gives them back right away
   results = []
   for i in range(5):
       results.append(long_task(i))
   print([r.result() for r in results])

In this example, the `long_task` function pretends to be a task that takes a long time. The first time you run it, it will take about 25 seconds (5 seconds per task). But the second time, Parsl will remember the results and give them back right away.
