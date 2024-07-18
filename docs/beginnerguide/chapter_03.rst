3. Core Concepts
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Parsl and Concurrency
----------------------

Parsl is designed to make parallel programming easier in Python. It allows you to break down your code into smaller tasks that can run concurrently, meaning they can be executed at the same time or in an overlapping manner. This is different from traditional Python code, which runs one line at a time, in sequence.

When you call a Parsl app (a function decorated with ``@python_app`` or ``@bash_app``), Parsl creates a new task that runs independently of your main program. This means your main program can continue running while the task is being executed, potentially on a different processor or computer.

Introduction to Futures
------------------------

To manage these concurrent tasks, Parsl uses futures. A future is a placeholder for the result of a task that hasn't finished yet. You can think of it like a meal ticket in a restaurant. You get the ticket immediately, but you have to wait for the meal to be prepared. Similarly, when you call a Parsl app, you get a future right away, but you have to wait for the task to complete before you can access the result.

Understanding AppFutures and DataFutures
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- AppFutures: Represent the execution of a Parsl app. You can use an AppFuture to check the status of a task, wait for it to finish, and get the result or any exceptions that occurred.
- DataFutures: Represent files produced by a Parsl app. They allow you to track the creation of output files and ensure that they are ready before being used as inputs to other tasks.

Parsl and Execution
-------------------

Execution providers, executors, and launchers make Parsl's ability to run tasks on different resources possible.

Execution Providers, Executors, and Launchers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Execution Providers: These components connect Parsl to the computing resources you want to use, whether it's your local machine, a cluster, or a cloud platform. They handle the details of submitting jobs, checking their status, and canceling them if needed.
- Executors: These components manage the execution of tasks on the resources provided by the execution provider. They decide when and where to run each task, taking into account factors like dependencies between tasks and the availability of resources.
- Launchers: These components are responsible for starting the worker processes that actually execute the tasks. They work with the execution provider to ensure that the workers are launched on the correct resources and with the correct environment.

Blocks and Elasticity
-----------------------

Parsl uses blocks to represent groups of resources. A block can be a single computer, a group of nodes in a cluster, or a set of virtual machines in the cloud. Parsl can dynamically adjust the number of blocks it uses based on the workload, a feature called elasticity. This allows Parsl to use resources efficiently, scaling up when there are many tasks to run and scaling down when the workload is lighter.

Parsl and Communication
-------------------------

Parsl tasks often need to exchange data to accomplish their work. Parsl provides two main mechanisms for communication between tasks:

Parameter Passing
^^^^^^^^^^^^^^^^^^^^

You can pass data directly between tasks as function arguments and return values. Parsl handles the serialization and deserialization of data, so you can pass complex objects like lists and dictionaries.

File Passing
^^^^^^^^^^^^^^^

You can also pass data between tasks using files. Parsl provides a ``File`` class that abstracts the location of files, making it easy to work with files stored on different systems.

Interactive Tutorial: Running Your First Parallel Task
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Parsl documentation includes an interactive tutorial that guides you through writing and running a parallel task.

Here are your options for completing the tutorial:

- Binder: For an online interactive experience without any installations, you can use Binder to run the tutorial in a Jupyter Notebook environment. Start the tutorial on Binder `here <https://mybinder.org/v2/gh/Parsl/parsl/master?filepath=tutorial%2Fparsl_tutorial.ipynb>`__.
- Online Notebooks: If you'd rather try Parsl in a different online notebook setup, you can access it `here <https://colab.research.google.com/github/Parsl/parsl/blob/master/tutorial/parsl_tutorial.ipynb>`__.

Here are links to Parsl documentation that will help guide you through the tutorial:

- Parsl Tutorial: This provides a comprehensive guide on using Parsl with examples and explanations. You can access it `here <https://parsl.readthedocs.io/en/latest/tutorial.html>`__.
- Quickstart Guide: This provides a quick introduction to Parsl and how to start the tutorial. You can access it `here <https://parsl.readthedocs.io/en/latest/getting_started.html>`__.

Visual Guide: How Parsl Manages Concurrency
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. raw:: html

    <iframe src="https://github.com/Kanegraffiti/parsl/blob/beginner-user-guide/notebooks/Parsl_and_Concurrency.ipynb" width="700" height="400"></iframe>
