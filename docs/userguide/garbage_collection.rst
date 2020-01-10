.. _garbage_collection:

Garbage Collection
------------------

Production workflows often run for days processing millions of tasks, and as tasks accumulate keeping
task information in memory can get expensive. To minimize the memory footprint Parsl enables Python's
garbage collection routines to remove tasks that have completed and have no further dependent tasks.
This functionality is always on and if the user wishes to keep tasks in memory, they can do so by
storing `AppFutures` in an active context. For eq:

.. code-block:: python

   def double(x):
        return x * 2

   # Since the AppFuture returned is not stored in a variable
   # the task will be garbage collected.
   print(double(5).result)

   y = double(6)
   print(y.result())
   # Task info will not be garbage collected as long as the
   # AppFuture reference stored in y is live


Interactions with Checkpointing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Garbage collection relies removing unnecessary references to task information so that Python's garbage
collection will free memory. However, task references must be kept available for checkpointing to work.
As a result of these conflicting requirements, any checkpointing mode that does not trigger at
`task_exit` such as `checkpoint_mode='periodic' | 'dfk_exit' | 'manual'` will result in deferred or no effective garbage collection.
