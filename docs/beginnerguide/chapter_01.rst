Introduction to Parsl
=====================

Overview
--------

What is Parsl and What Can It Be Used For?
------------------------------------------

Parsl is a Python library designed to simplify the process of splitting a problem into smaller tasks that can be executed at the same time – in parallel – using multiple computing resources (parallel programming). It allows you to write Python code that can run across multiple computers or processors at the same time, potentially speeding up your calculations and data processing tasks significantly. Parsl is particularly well-suited for problems that can be broken down into smaller, independent steps. It can handle a wide range of applications, from simple Python functions to complex workflows involving multiple steps and different types of tasks.

Common Applications
-------------------

* Data processing and analysis: Parsl can process large datasets in parallel, speeding up tasks like data cleaning, transformation, and analysis. Researchers can use Parsl to parallelize genomic data analysis, where large datasets of DNA sequences are cleaned, aligned, and analyzed to identify genetic variations. This process can be significantly accelerated by executing tasks like sequence alignment and variant calling in parallel across multiple computing nodes.

* Scientific simulations: Parsl can run complex simulations in parallel, such as those used in physics, chemistry, and biology. Parsl has been used in chemistry to parallelize dynamic simulations of small CO2 molecule clusters. This involves using a flexible monomer two-body carbon dioxide potential function to understand the vibrational structure of these clusters at high levels of quantum mechanical theory. By running these simulations in parallel, computational efficiency is increased, and statistical sampling is improved.

* Machine learning: Parsl can train and deploy machine learning models in parallel, improving performance and scalability. Parsl can be employed to parallelize the training of models. An example is training multiple deep learning models with different hyperparameters simultaneously to find the best-performing model. This parallel training can be done across various GPUs, reducing the time required to train and evaluate the models.

* Parameter studies: Parsl can run multiple program instances with different parameters in parallel, exploring a wider range of possibilities. Parsl is helpful for parameter studies, such as in materials science, where researchers might run simulations to study the properties of new materials under different conditions. By using Parsl, they can execute multiple simulation instances with varying parameters, like temperature or pressure, in parallel. This allows for a comprehensive exploration of the material’s behavior across various conditions.

Script vs. Workflow
-------------------

In Parsl, there's a distinction between a script and a workflow.

* **Script**: A single Python file containing Parsl code. It defines the tasks to be executed and their dependencies.

* **Workflow**: The actual execution of a Parsl script. It involves coordinating multiple tasks across different resources, such as processors or computers.

Understanding the role of a Parsl script and a workflow is like understanding the difference between a recipe and the cooking process. A Parsl script can be thought of as a recipe, where the ingredients are the tasks and the steps are the dependencies. The workflow, on the other hand, is the process of cooking that recipe. It orchestrates the actual cooking process, ensuring that the steps are followed correctly and the ingredients are combined at the right time. This analogy helps in understanding the concept better and applying it effectively.

.. image:: images/script_vs_workflow.png
   :width: 800px
   :align: center
   :alt: Infographic - Script vs Workflow

Key Features and Benefits
-------------------------

Parsl offers several features and benefits that make it a powerful tool for parallel programming.

* **Python-based**: Parsl is written in this popular and easy-to-learn programming language. This makes it accessible to a wide range of users, including those without extensive experience in parallel programming.
* **Works everywhere**: Parsl can run on various platforms, from your laptop to large-scale clusters and supercomputers. This flexibility allows you to develop and test your code locally and then easily scale it up to larger systems.
* **Flexible**: Parsl supports different tasks, including Python functions, Bash scripts, and MPI applications. This allows you to leverage existing code and tools in your parallel workflows.
* **Handles data**: Parsl can automatically manage the data movement between tasks, even if they are running on different computers. This simplifies the development of data-intensive workflows.
* **Fast**: Parsl is designed to be efficient and handle thousands of tasks per second, making it suitable for high-performance computing applications.

Understanding Concurrency and Parallelism
-----------------------------------------

Concurrency vs. Parallelism
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Concurrency is a program's ability to handle multiple tasks at once. These tasks may not necessarily be executed simultaneously but can be interleaved or overlapped. Parallelism is the simultaneous execution of multiple tasks on different processors or computers. This requires hardware support for parallel processing, such as multiple cores or nodes.

Concurrency is a more general concept that can be achieved even on a single processor through techniques such as time-sharing or multithreading. Whereas parallelism requires multiple processors and involves the actual simultaneous execution of tasks.

Parsl enables both concurrency and parallelism. It allows you to define tasks that can be executed concurrently and leverages parallel hardware to run those tasks simultaneously on different processors or computers.

Parsl Facilitates Parallel Computing
------------------------------------

Here's how:

* **Task definition**: You define tasks as Python functions or Bash scripts, using decorators to indicate that they can be run in parallel.
* **Dependency management**: You specify dependencies between tasks, indicating which tasks must be completed before others can start.
* **Resource allocation**: Parsl automatically allocates resources (processors or computers) to tasks based on their dependencies and the available resources.
* **Task execution**: Parsl executes tasks in parallel by utilizing available resources.
* **Data management**: Parsl automatically manages the data movement between tasks, ensuring each task has the necessary input data when it starts.
* **Result collection**: Parsl collects the results of tasks as they are completed, allowing you to access them in your Python code.

Glossary
--------

* **App**: A Python function decorated with @python_app or @bash_app that tells Parsl it can be run in parallel.
* **AppFuture**: A future that represents the execution of a Parsl app.
* **Block**: A group of resources used by Parsl.
* **Concurrency**: The ability of a program to handle multiple tasks at once.
* **DataFlowKernel (DFK)**: The part of Parsl that manages the execution of your apps and the flow of data between them.
* **DataFuture**: A future that represents a file produced by a Parsl app.
* **Elasticity**: The ability of Parsl to dynamically adjust how many blocks it uses.
* **Execution Provider**: A component that connects Parsl to computing resources.
* **Executor**: The part of Parsl that runs your apps on different computers or processors.
* **Future**: A placeholder for the result of a task that hasn't finished yet. You can use the .result() method to get the actual result when it's ready.
* **Launcher**: A component that starts worker processes to execute tasks.
* **Thread**: A lightweight process that allows for concurrent execution of tasks within a single program.