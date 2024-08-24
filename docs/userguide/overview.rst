Overview
========

What is Parsl and Its Applications?
-----------------------------------

Parsl is a Python library designed to enable straightforward parallelism and the orchestration of asynchronous tasks into dataflow-based workflows. It simplifies the execution of tasks across multiple computing resources, from laptops to supercomputers, by managing the concurrent execution of these tasks. Parsl schedules each task only when its dependencies, such as input data, are met. This allows Python code to run concurrently on various processors or computers, significantly speeding up data processing and computational tasks. Parsl is particularly beneficial for applications where problems can be broken down into smaller, independent tasks, making it versatile for a wide range of uses—from simple Python functions to complex workflows.

Developing a Parsl Program
--------------------------

Developing a Parsl program is a two-step process:

1. **Define Parsl Apps**: Annotate Python functions to indicate that they can be executed concurrently.
2. **Invoke Parsl Apps**: Use standard Python code to invoke these apps, creating asynchronous tasks and managing the dependencies defined between them.

This approach allows you to create a mental model of how Parsl programs behave. You can compare and contrast the behavior of Python programs that use Parsl constructs with those of conventional Python programs.

Common Applications
-------------------

1. **Data Processing and Analysis**: Parsl accelerates the processing of large datasets by performing tasks like data cleaning, transformation, and analysis in parallel. For instance, in genomic data analysis, tasks such as sequence alignment and variant calling can be executed concurrently across multiple computing nodes, significantly speeding up the process.

2. **Scientific Simulations**: Parsl supports the parallel execution of complex simulations in fields such as physics, chemistry, and biology. A practical example includes simulating CO2 molecule clusters in chemistry, where running simulations in parallel improves computational efficiency and statistical sampling.

3. **Machine Learning**: Parsl can parallelize the training of machine learning models. For example, it can train multiple deep learning models with different hyperparameters simultaneously across various GPUs, reducing the time required to find the best-performing model.

4. **Parameter Studies**: Parsl allows the execution of multiple program instances with varying parameters in parallel. This is particularly useful in materials science, where researchers can study material properties under different conditions by running simulations in parallel.

Script vs. Workflow
-------------------

In Parsl, the distinction between a script and a workflow is essential:

- **Script**: A Python file containing Parsl code that defines the tasks and their dependencies.
- **Workflow**: The actual execution of the Parsl script, which coordinates multiple tasks across different resources, ensuring tasks are executed in the correct order based on their dependencies.

.. image:: ../images/ScriptvsWorkflow.png
   :alt: Script vs. Workflow
   :align: center
   :scale: 70%

Understanding this distinction is like differentiating between a recipe and the cooking process. The script is the recipe, while the workflow is the cooking process, orchestrating the execution of tasks.

Key Features and Benefits
-------------------------

- **Python-Based**: Parsl is written in Python, making it accessible to users familiar with the language.
- **Cross-Platform Compatibility**: Parsl runs on various platforms, from laptops to supercomputers, allowing for easy scaling.
- **Flexibility**: It supports different tasks, including Python functions, Bash scripts, and MPI applications, enabling the integration of existing code.
- **Data Handling**: Parsl manages data movement between tasks automatically, simplifying the development of data-intensive workflows.
- **Performance**: Parsl efficiently handles thousands of tasks per second, making it suitable for high-performance computing applications.

Understanding Concurrency and Parallelism
=========================================

Concurrency vs. Parallelism
----------------------------

- **Concurrency**: The ability of a program to handle multiple tasks simultaneously. These tasks may not execute at the same time but can overlap or be interleaved.
- **Parallelism**: The simultaneous execution of multiple tasks on different processors or computers, requiring hardware support for parallel processing.

Parsl facilitates both concurrency and parallelism, allowing tasks to be executed concurrently and leveraging parallel hardware to run them simultaneously on different processors or computers.

.. image:: ../images/ParslManagesConcurrency.jpg
   :alt: How Parsl Manages Concurrency
   :align: center
   :scale: 70%

How Parsl Facilitates Parallel Computing
----------------------------------------

Parsl simplifies parallel computing by abstracting away the complexities of managing threads, processes, or inter-node communication. It allows you to focus on application logic by handling:

- **Task Definition**: Defining tasks as Python functions or Bash scripts that can run in parallel.
- **Dependency Management**: Specifying dependencies between tasks to ensure correct execution order.
- **Resource Allocation**: Automatically allocating resources based on task dependencies and availability.
- **Task Execution**: Running tasks in parallel by utilizing available resources.
- **Data Management**: Managing data movement between tasks to ensure they have the necessary input data.
- **Result Collection**: Collecting task results for further processing in your Python code.

Parsl and Concurrency
=====================

Any call to a Parsl app creates a new task that executes concurrently with the main program and other tasks. This behavior contrasts with the sequential nature of standard Python programs, where statements are executed one at a time in the order they appear.

In a traditional Python program, calling a function transfers control from the main program to the function, and execution of the main program resumes only after the function returns. However, in Parsl, whenever a program calls an app, a separate thread of execution is created, and the main program continues without pausing. The calling program will only block (wait) when explicitly told to do so, typically by calling the ``result()`` method.

Execution Model: Python vs. Parsl
---------------------------------

The execution model of Parsl is inherently concurrent, which is a key difference from Python's native sequential execution model. The figures below illustrate this difference:

1. **Python Sequential Execution**:
   - In traditional Python, the main program pauses when a function is called and resumes only after the function returns.

2. **Parsl Concurrent Execution**:
   - In Parsl, when an app is called, a separate task is created, and the main program continues executing. The main program only waits if the ``result()`` method is called.

This concurrent execution model enables Parsl to efficiently manage and execute multiple tasks in parallel, even across different nodes or computers.

Parsl and Execution
===================

Parsl tasks are executed concurrently alongside the main Python program and other Parsl tasks. Depending on the computing environment, Parsl allows tasks to be executed using different executors, which are responsible for managing the execution of tasks on local or remote resources.

Common Executors in Parsl
--------------------------

1. **HighThroughputExecutor (HTEX)**:
   The ``HighThroughputExecutor`` (HTEX) implements a pilot job model that enables fine-grain task execution across one or more provisioned nodes. HTEX can be used on a single node (e.g., a laptop) or across multiple nodes in a cluster. It communicates with a resource manager (e.g., a batch scheduler or cloud API) to provision nodes for the duration of execution. HTEX deploys lightweight worker agents on the nodes, which connect back to the main Parsl process. This model avoids long job scheduler queue delays and allows for efficient scheduling of many tasks on individual nodes.

   .. image:: ../images/overview/htex-model.png
      :alt: HTEX Model

   **Note**: When deploying HTEX or any pilot job model, it is important to ensure that the worker nodes can connect back to the main Parsl process. Parsl provides a helper function, ``parsl.addresses.address_by_query``, to automatically detect network addresses.

2. **ThreadPoolExecutor**:
   The ``ThreadPoolExecutor`` allows tasks to be executed on a pool of locally accessible threads. As execution occurs on the same computer, the tasks share memory with one another. This executor is ideal for running tasks on a single machine where tasks need to share data or resources.

Parsl and Communication
=======================

Parsl tasks typically need to communicate to perform useful work. Parsl supports two primary forms of communication: parameter passing and file passing.

Parameter Passing
-----------------

In Parsl, parameters can be passed directly between tasks. When a task is created, it receives its input parameters and, upon completion, returns the output to the main program or another task. While simple primitive types like integers are commonly passed, more complex objects such as Numpy arrays, Pandas DataFrames, or custom objects can also be passed to and from tasks.

File Passing
------------

Parsl also supports communication via files, which is especially useful when dealing with large datasets or when tasks are designed to work with files. Parsl uses the ``parsl.data_provider.files.File`` construct for location-independent reference to files. This allows tasks to be executed on remote nodes without shared file systems. Parsl can transfer files in, out, and between tasks using methods such as FTP, HTTP(S), Globus, and rsync. The asynchronous nature of file transfer is managed by Parsl, which adds these transfers as dependencies in the execution graph.

Synchronization with Futures
-----------------------------

Futures in Parsl serve as placeholders for the results of tasks. When a task is created, it returns a future that initially remains in an unassigned state until the task completes. The ``result()`` method is used to retrieve the result, and this method blocks the main program until the future is resolved. This synchronization mechanism ensures that dependent tasks execute in the correct order.

.. image:: ../images/overview/communication.png
   :alt: Communication and Synchronization

The Parsl Environment
=====================

Parsl enhances Python by altering the environment in which code executes, including the memory environment, file system environment, and service environment.

Memory Environment
------------------

In Python, a function has access to both local variables (defined within the function) and global variables (defined outside the function). However, in Parsl, except when using the ``ThreadPoolExecutor``, each task runs in a distinct environment with access only to local variables associated with the app function.

For example, consider the following code:

.. code-block:: python

    answer = 42

    def print_answer():
        print('the answer is', answer)

    print_answer()

In regular Python, the ``print_answer`` function would output "the answer is 42" because it accesses the global variable ``answer``. However, in Parsl (except when using the ``ThreadPoolExecutor``), if this program is executed, the function would print "the answer is 0" because the print statement in ``print_answer`` would not have access to the global variable that has been assigned the value 42. This behavior is due to the fact that Parsl apps run in a distinct environment with no access to global variables.

To ensure compatibility with all Parsl executors, you should pass any required variables as arguments to the function, and import any necessary modules within the function itself, as shown below:

.. code-block:: python

    import random
    factor = 5

    @python_app
    def good_double(factor, x):
        import random
        return x * random.random() * factor

    print(good_double(factor, 42).result())

This ensures that the function runs correctly regardless of the executor used.

File System Environment
========================

In a regular Python program, the environment accessible to the code includes the file system of the computer on which it is running. For example:

.. code-block:: python

    def print_answer_file():
        with open('answer.txt', 'r') as f:
            print('the answer is', f.read())

    with open('answer.txt', 'w') as f:
        f.write('42')

    print_answer_file()

The above code writes the value "42" to a file named ``answer.txt`` and then reads it back to print "the answer is 42". In Parsl, the file system environment accessible to a task depends on where the task executes. If two tasks run on nodes that share a file system, they can share the file system environment, allowing one task to read a file written by another task. However, if the tasks are executed on nodes without a shared file system, they will not share the file environment, and attempts to read the file would fail.

The diagram below illustrates this scenario:

.. image:: ../images/overview/filesystem.png
   :alt: File System Environment

Service Environment
====================

The service environment refers to network services accessible to a Parsl program, such as a Redis server or Globus data management service. These services are typically accessible to any task, regardless of the executor used or the location of the task.

Environment Summary
====================

The table below summarizes the differences in environment sharing between tasks executed with the ``ThreadPoolExecutor`` and other Parsl executors:

.. list-table::
   :header-rows: 1

   * - 
     - Share Memory Environment with Parent/Other Tasks
     - Share File System Environment with Parent
     - Share File System Environment with Other Tasks
     - Share Service Environment with Other Tasks
   * - Python without Parsl
     - Yes
     - Yes
     - N/A
     - N/A
   * - Parsl ``ThreadPoolExecutor``
     - Yes
     - Yes
     - Yes
     - N/A
   * - Other Parsl Executors
     - No
     - If executed on the same node with file system access
     - If tasks are executed on the same node or with access to the same file system
     - N/A

Getting Started with Parsl
===========================

Installation and Setup
-----------------------

System Requirements and Dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Parsl is compatible with Python 3.8 or newer and has been tested on Linux. Ensure Python and pip are installed on your system before proceeding.

Installation on Different Platforms
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Windows**: Parsl is not officially supported on Windows, but you can use the Windows Subsystem for Linux (WSL) to run it.
- **Docker**: Parsl can be run in a Docker container, providing a portable environment.
- **macOS**: Install Parsl using pip or conda. On Macs with M1 chips, use a Rosetta terminal for compatibility.
- **Linux**: Parsl is well-supported on Linux and can be installed using pip or conda.
- **Android**: While Parsl is not designed for Android, you can use platforms like Google Colab to run Parsl scripts in a browser.

Installing Parsl
~~~~~~~~~~~~~~~~

To install Parsl using pip, run:

.. code-block:: bash

    python3 -m pip install parsl

Verify the installation with:

.. code-block:: bash

    parsl --version

Upgrade to the latest version with:

.. code-block:: bash

    python3 -m pip install -U parsl

Basic Configuration
---------------------

Parsl separates your code from how it's executed through a configuration file that defines how Parsl uses computing resources. A simple configuration for running Parsl on your local machine might look like this:

.. code-block:: python

    from parsl.config import Config
    from parsl.executors import ThreadPoolExecutor

    config = Config(
        executors=[ThreadPoolExecutor(max_threads=4)]
    )

This configuration uses up to four threads in parallel on your local machine.

Writing and Running a Parsl Script
-----------------------------------

A Parsl script defines tasks and their dependencies. Here’s a simple example:

.. code-block:: python

    import parsl
    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor
    from parsl import python_app

    config = Config(executors=[HighThroughputExecutor(max_workers=4)])
    parsl.load(config)

    @python_app
    def my_task(x):
        return x * 2

    results = [my_task(i) for i in range(10)]

    for result in results:
        print(result.result())

This script defines a task that doubles a number, runs 10 instances of this task in parallel, and prints the results.

.. image:: ../images/BasicParslScriptFlow.jpg
   :alt: Diagram: Basic Parsl Script Flow
   :align: center
   :scale: 70%

Practical Tutorial: Hello World with Parsl
------------------------------------------

A basic "Hello World" script in Parsl:

.. code-block:: python

    import parsl
    from parsl import python_app

    @python_app
    def hello(name):
        return f'Hello, {name}!'

    parsl.load()
    result = hello("World").result()
    print(result)  # Output: Hello, World!

This script demonstrates the core components of a Parsl program, including importing Parsl, loading a configuration, defining an app, calling the app, and retrieving results.

Setting Up Your First Parsl Workflow
-------------------------------------

To set up your first Parsl workflow, you'll need to:

1. **Install Parsl**: Follow the installation instructions in the "Installation and Setup" section to install Parsl on your system.
2. **Choose a Configuration**: Select a configuration that matches your computing environment. Parsl provides several example configurations for different platforms, such as laptops, clusters, and clouds. You can also create custom settings.
3. **Write a Parsl Script**: Define the tasks you want to run in parallel and their dependencies.
4. **Load the Configuration**: Use the ``parsl.load()`` function to load your chosen configuration.
5. **Run Your Script**: Execute a Parsl script like any other Python script. Parsl will then take care of executing your tasks in parallel, managing dependencies, and moving data as needed.

Advanced Concepts: Parsl Environment and Execution
===================================================

Parsl and Concurrency
----------------------

As previously mentioned, any call to a Parsl app creates a new task that executes concurrently with the main program and other tasks. This behavior contrasts with the sequential nature of standard Python programs, where statements are executed one at a time in the order they appear.

Execution Model: Python vs. Parsl
----------------------------------

The execution model of Parsl is inherently concurrent, which is a key difference from Python's native sequential execution model. This enables Parsl to efficiently manage and execute multiple tasks in parallel, even across different nodes or computers.

Task Communication
-------------------

Parsl supports communication between tasks via parameter and file passing. Parameters can be passed directly between tasks, while files can be used when dealing with large datasets or when tasks are designed to work with files. Parsl manages file transfers as asynchronous tasks, ensuring that dependencies are met before execution.

Synchronization with Futures
-----------------------------

Futures in Parsl serve as placeholders for the results of tasks. When a task is created, it returns a future that remains in an unassigned state until the task completes. The ``result()`` method is used to retrieve the result, and this method blocks the main program until the future is resolved. This synchronization mechanism ensures that dependent tasks execute in the correct order.

Environment Considerations
---------------------------

Parsl enhances Python by altering the environment in which code executes, including the memory environment, file system environment, and service environment. Understanding these environments is crucial for developing efficient and reliable Parsl applications.

