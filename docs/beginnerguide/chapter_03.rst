3. Core Concepts
================

Parsl and Concurrency
---------------------

Parsl is designed to make parallel programming easier in Python. It allows you to break down your code into smaller tasks that can run concurrently, meaning they can be executed at the same time or in an overlapping manner. This is different from traditional Python code, which runs one line at a time, in sequence.

When you call a Parsl app (a function decorated with ``@python_app`` or ``@bash_app``), Parsl creates a new task that runs independently of your main program. This means your main program can continue running while the task is being executed, potentially on a different processor or computer.

Introduction to Futures
-----------------------

To manage these concurrent tasks, Parsl uses futures. A future is a placeholder for the result of a task that hasn't finished yet. You can think of it like a meal ticket in a restaurant. You get the ticket immediately, but you have to wait for the meal to be prepared. Similarly, when you call a Parsl app, you get a future right away, but you have to wait for the task to complete before you can access the result.

Understanding AppFutures and DataFutures
----------------------------------------

- **AppFutures**: Represent the execution of a Parsl app. You can use an AppFuture to check the status of a task, wait for it to finish, and get the result or any exceptions that occurred.
- **DataFutures**: Represent files produced by a Parsl app. They allow you to track the creation of output files and ensure that they are ready before being used as inputs to other tasks.

Parsl and Execution
-------------------

Execution providers, executors, and launchers make Parsl's ability to run tasks on different resources possible.

Execution Providers, Executors, and Launchers
---------------------------------------------

- **Execution Providers**: These components connect Parsl to the computing resources you want to use, whether it's your local machine, a cluster, or a cloud platform. They handle the details of submitting jobs, checking their status, and canceling them if needed.
- **Executors**: These components manage the execution of tasks on the resources provided by the execution provider. They decide when and where to run each task, taking into account factors like dependencies between tasks and the availability of resources.
- **Launchers**: These components are responsible for starting the worker processes that actually execute the tasks. They work with the execution provider to ensure that the workers are launched on the correct resources and with the correct environment.

Blocks and Elasticity
---------------------

Parsl uses blocks to represent groups of resources. A block can be a single computer, a group of nodes in a cluster, or a set of virtual machines in the cloud. Parsl can dynamically adjust the number of blocks it uses based on the workload, a feature called elasticity. This allows Parsl to use resources efficiently, scaling up when there are many tasks to run and scaling down when the workload is lighter.

Parsl and Communication
-----------------------

Parsl tasks often need to exchange data to accomplish their work. Parsl provides two main mechanisms for communication between tasks:

Parameter Passing
-----------------

You can pass data directly between tasks as function arguments and return values. Parsl handles the serialization and deserialization of data, so you can pass complex objects like lists and dictionaries.

File Passing
------------

You can also pass data between tasks using files. Parsl provides a ``File`` class that abstracts the location of files, making it easy to work with files stored on different systems.

Interactive Tutorial: Running Your First Parallel Task
------------------------------------------------------

The Parsl documentation includes an interactive tutorial that guides you through writing and running a parallel task.

Here are your options for completing the tutorial:

- **Binder**: For an online interactive experience without any installations, you can use Binder to run the tutorial in a Jupyter Notebook environment. Start the tutorial on Binder `here <https://mybinder.org/v2/gh/Parsl/parsl/master?filepath=tutorial%2Fparsl_tutorial.ipynb>`__.
- **Online Notebooks**: If you'd rather try Parsl in a different online notebook setup, you can access it `here <https://colab.research.google.com/github/Parsl/parsl/blob/master/tutorial/parsl_tutorial.ipynb>`__.

Here are links to Parsl documentation that will help guide you through the tutorial:

- **Parsl Tutorial**: This provides a comprehensive guide on using Parsl with examples and explanations. You can access it `here <https://parsl.readthedocs.io/en/latest/tutorial.html>`__.
- **Quickstart Guide**: This provides a quick introduction to Parsl and how to start the tutorial. You can access it `here <https://parsl.readthedocs.io/en/latest/getting_started.html>`__.

Practical Guide: How Parsl Manages Concurrency
==============================================

This `code <https://github.com/Kanegraffiti/parsl/blob/beginner-user-guide/docs/beginnerguide/notebooks/Parsl_and_Concurrency.ipynb>`_ is a simple demonstration of how to use Parsl to execute functions concurrently on multiple cores or nodes. Let's break down what each part of the code does:

1. Importing Libraries and Configuring Parsl
============================================

Import Statements:
------------------

.. code-block:: python

   import parsl
   from parsl import python_app, HighThroughputExecutor
   from parsl.config import Config

- ``import parsl``: This imports the Parsl library into your Python script.
- ``from parsl import python_app, HighThroughputExecutor``: This imports specific tools from Parsl:
  - ``python_app``: A decorator that converts a regular Python function into a Parsl app, allowing it to run asynchronously.
  - ``HighThroughputExecutor``: An executor designed to manage and distribute a high volume of tasks across many compute nodes efficiently.
- ``from parsl.config import Config``: Imports the ``Config`` class, which is used to configure Parsl's behavior and settings.

Configuration Setup:
--------------------

.. code-block:: python

   config = Config(
       executors=[HighThroughputExecutor(
           label="htex_Local",
           worker_debug=True,
           cores_per_worker=1,
           max_workers=4  # Adjust based on your local machine's resources
       )]
   )
   parsl.load(config)

- ``Config``: Sets up the configuration for Parsl, specifying what executors to use.
- ``HighThroughputExecutor``: Specifies the executor type. In this case, it's set up for high throughput with options like ``worker_debug`` for debugging, ``cores_per_worker`` defining how many CPU cores each worker can use, and ``max_workers`` which limits the number of workers to prevent overloading your system.
- ``parsl.load(config)``: Loads and applies the configuration you've defined so that Parsl is ready to run with these settings.

2. Defining Parsl Python Apps
=============================

.. code-block:: python

   @python_app
   def multiply(x, y):
       return x * y

   @python_app
   def add(x, y):
       return x + y

- **Decorators** (``@python_app``): These lines transform the ``multiply`` and ``add`` functions into Parsl apps. Once a function is decorated with ``@python_app``, it can be executed asynchronously. That means it can run in the background, allowing other code to execute without waiting for it to finish.
- **Function Definitions**: Simple mathematical operations are defined here. ``multiply`` multiplies two numbers, and ``add`` adds two numbers.

3. Executing Apps Asynchronously
================================

.. code-block:: python

   result1 = multiply(5, 10)
   result2 = add(20, 30)

- These lines execute the ``multiply`` and ``add`` functions asynchronously. Instead of waiting for the results immediately, Parsl manages these tasks in the background and allows your program to continue running.

4. Retrieving and Printing Results
==================================

.. code-block:: python

   print("Result of multiplication:", result1.result())
   print("Result of addition:", result2.result())

- ``result()`` Method: This is called on the future objects (``result1`` and ``result2``) to get the outcome of the asynchronous operations. Calling ``result()`` will block the execution of further code until the computation it refers to is completed and the result is available.
- ``print`` Statements: These lines display the results of the operations. They show how to access the results from asynchronous operations and print them out.

This example demonstrates the basic concepts of nonsimultaneous execution using Parsl, which is particularly useful for parallel processing tasks that can be distributed across multiple cores or nodes to improve computational efficiency.
