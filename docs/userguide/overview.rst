Overview of Parsl
=================

Parsl is a Python library that simplifies the execution of parallel and distributed tasks by converting standard Python functions into "apps" that run asynchronously. These apps can be combined to form workflows, enabling complex data processing pipelines. Parsl automates task scheduling, dependency management, and resource allocation, allowing developers to focus on the core logic of their applications rather than the complexities of parallel execution. Whether you're working on a local machine or a massive supercomputing cluster, Parsl scales to meet your computational needs, supporting various applications, including those that use GPUs, external code, or multiple threads.

Key Concepts
------------

Parsl introduces several important concepts that differentiate it from traditional Python programming:

- **Parallel Computing:** Executing multiple processes simultaneously, essential for efficiently handling large datasets or complex simulations.
- **Tasks and Workflows:** A task in Parsl is an individual unit of work, such as a function call. A workflow is a series of tasks connected by dependencies, defining the order in which tasks are executed based on the availability of their inputs.
- **Execution Environments:** Parsl can execute tasks in various environments, including local machines, clusters, and cloud platforms. The execution environment determines how tasks are distributed across available resources.
- **Scheduling and Resource Management:** Parsl automates the allocation of computational resources, ensuring tasks are executed in an optimal order without exceeding resource limits.

Developing a Parsl Program
--------------------------

Creating a Parsl program involves two main steps:

1. **Defining Parsl Apps:** A Parsl app is a Python function decorated with ``@python_app`` (for Python functions) or ``@bash_app`` (for command-line applications). These decorators indicate that the function can be executed asynchronously, allowing it to run in parallel with other tasks.

2. **Invoking Parsl Apps:** Once apps are defined, they can be invoked like regular Python functions. Instead of returning immediate results, these invocations return futures—objects that represent the eventual outcome of the function, allowing the main program to continue executing while the app runs in the background.

Example:

.. code-block:: python

    from parsl import python_app

    @python_app
    def add(x, y):
        return x + y

    result = add(3, 4)
    print(result.result())  # Expected outcome: 7

In this example, the ``add`` function is executed asynchronously. The main program continues executing without waiting for the result, which is retrieved later using ``result.result()``.

.. note::
	The behavior of a Parsl program can vary in minor respects depending on the
	Executor used (see :ref:`label-execution`). We focus here on the behavior seen when
	using the recommended `parsl.executors.HighThroughputExecutor` (HTEX).

Parsl and Concurrency
---------------------

In Parsl, any call to an app creates a new task that executes concurrently with the main program and other currently executing tasks. These tasks may run on the same or different nodes, and on the same or different computers.

**Python Execution Models:**

- In a typical Python program, tasks are executed sequentially, one after the other, in the order they appear. When a function is called, control passes from the main program to the function and resumes only after the function returns.
- Parsl's execution model is inherently concurrent. When a program calls an app, it creates a separate thread of execution, allowing the main program to continue without pausing. For instance, calling the ``double`` app twice creates two new tasks that can run concurrently.

.. image:: ../images/overview/python-concurrency.png
   :scale: 70
   :align: center

**Concurrency vs. Parallelism:**

- **Concurrency** allows multiple tasks to start, run, and complete in overlapping periods, but not necessarily simultaneously.
- **Parallelism** implies that multiple tasks actually run simultaneously, which is possible if there are enough processors available.

.. image:: ../images/overview/parsl-concurrency.png


.. note::
	Note: We talk here about concurrency rather than parallelism for a reason.
	Two activities are concurrent if they can execute at the same time. Two
	activities occur in parallel if they do run at the same time. If a Parsl
	program creates more tasks that there are available processors, not all
	concurrent activities may run in parallel.

Parsl and Execution
-------------------

Parsl allows tasks to be executed across various computational resources using different executors. Executors manage a queue of tasks and execute them on local or remote resources.

**Executors Overview:**

1. **HighThroughputExecutor (HTEX):**
   - HTEX implements a pilot job model that facilitates fine-grain task execution across one or more provisioned nodes. It can be used on a single node, such as a laptop, and leverages multiple processes for concurrent execution.
   - HTEX uses Parsl's provider abstraction to communicate with resource managers (e.g., batch schedulers or cloud APIs) to provision nodes. A lightweight worker agent deployed on these nodes connects back to the main Parsl process, distributing tasks and collecting results.

2. **ThreadPoolExecutor:**
   - This executor allows tasks to be executed on a pool of locally accessible threads. Execution occurs on the same computer, and tasks share memory with one another.
   - ``ThreadPoolExecutor`` is ideal for applications requiring high concurrency on a single machine, as tasks share the same memory and resources.

.. image:: ../images/overview/htex-model.png

.. Note:
	Note: when deploying HTEX, or any pilot job model such as the
	WorkQueueExecutor, it is important that the worker nodes be able to connect
	back to the main Parsl process. Thus, you should verify that there is network
  connectivity between the workers and the Parsl process and ensure that the
	correct network address is used by the workers. Parsl provides a helper
	function to automatically detect network addresses 
	(`parsl.addresses.address_by_query`).

The `parsl.executors.ThreadPoolExecutor` allows tasks to be executed on a pool of locally 
accessible threads. As execution occurs on the same computer, on a pool of 
threads forked from the main program, the tasks share memory with one another 
(this is discussed further in the following sections).


Parsl and Communication
-----------------------

Parsl tasks often need to communicate with each other to perform useful work. Parsl provides two primary forms of communication: parameter passing and file passing.

**Parameter Passing:**

Tasks exchange values directly, allowing the results of one task to be used as inputs for another. This method supports both simple primitive types and complex objects like Numpy Arrays or Pandas DataFrames.

**File Passing:**

File passing is useful for exchanging large datasets or when data cannot be easily serialized into Python objects. Parsl supports file-based communication in both Bash and Python apps. For tasks running on remote nodes without shared file systems, Parsl uses a ``parsl.data_provider.files.File`` construct for location-independent file references.

**Futures for Synchronization:**

Parameter and file passing also serve as synchronization mechanisms. When a Parsl app is called, it returns a "future" object that holds the app's result. The ``result()`` function blocks the main program until the future has a value, ensuring that dependent tasks do not proceed until their inputs are ready.

The Parsl Environment
---------------------

Parsl enhances Python by modifying the environment in which code executes. The environment refers to the variables and modules (*memory environment*), file systems (*file system environment*), and services (*service environment*) accessible to a function.

**Memory Environment:**

In regular Python, functions have access to both local and global variables. In Parsl, tasks executed with the ``parsl.executors.HighThroughputExecutor`` (HTEX) operate in a distinct environment, accessing only local variables associated with the function. This contrasts with the ``parsl.executors.ThreadPoolExecutor``, where tasks share the memory environment with the main program.

**File System Environment:**

The file system environment determines which files are accessible to a task. In a regular Python program, tasks have access to the entire file system. In Parsl, the file system environment depends on where the task is executed. Tasks on nodes with shared file systems can share files, while tasks on different nodes may need to transfer files explicitly.

.. image:: ../images/overview/filesystem.png
   :scale: 70
   :align: center 

**Service Environment:**

The service environment includes network services accessible to Parsl tasks, such as Redis servers or Globus data management services. These services are available to all tasks within the environment.

**Environment Summary:**

Tasks executed with the ``parsl.executors.ThreadPoolExecutor`` share the memory and file system environment with the main program. In contrast, tasks executed with other Parsl executors may have isolated environments, depending on the executor and node configuration.

+--------------------+--------------------+--------------------+---------------------------+------------------+
|                    | Share memory       | Share file system  | Share file system         | Share service    |
|                    | environment with   | environment with   | environment with other    | environment      |
|                    | parent/other tasks | parent             | tasks                     | with other tasks | 
+====================+====================+====================+===========================+==================+
+--------------------+--------------------+--------------------+---------------------------+------------------+
| Python             | Yes                | Yes                | N/A                       |     N/A          |
| without            |                    |                    |                           |                  |
| Parsl              |                    |                    |                           |                  |
+--------------------+--------------------+--------------------+---------------------------+------------------+
| Parsl              | Yes                | Yes                | Yes                       |     N/A          |
| ThreadPoolExecutor |                    |                    |                           |                  |
|                    |                    |                    |                           |                  |
+--------------------+--------------------+--------------------+---------------------------+------------------+
| Other Parsl        | No                 | If executed on the | If tasks are executed on  |     N/A          |
| executors          |                    | same node with     | the same node or with     |                  |
|                    |                    | file system access | access to the same file   |                  |
|                    |                    |                    | system                    |                  |
+--------------------+--------------------+--------------------+---------------------------+------------------+

