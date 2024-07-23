19. FAQ
=======

Parsl vs. Globus Compute Workflows
----------------------------------

Parsl and Globus Compute are both tools for running tasks in parallel, but they have different strengths and are suited for different use cases.

**Parsl**: Parsl is a Python library that makes it easy to write parallel programs in Python. It's designed to be flexible and can run on a wide range of computing environments, from laptops to supercomputers. Parsl is particularly well-suited for tasks that can be broken down into smaller, independent steps, and it can handle complex workflows with dependencies between tasks.

**Globus Compute**: Globus Compute is a platform for running tasks on remote high-performance computing (HPC) systems. It provides a web interface and APIs for submitting and managing jobs, and it can automate the transfer of data between your computer and the HPC system. Globus Compute is a good choice if you need to run large-scale jobs on HPC systems and you want to avoid the complexities of manually managing job submissions and data transfers.

Parsl vs Dask
-------------

Parsl is similar to Dask in several ways. Both are parallel computing libraries for Python that facilitate the execution of tasks in parallel, but they have different focuses and implementation details. Here are the key similarities and differences:

**Similarities**:

1. Parallel and Distributed Computing:
   - Both Parsl and Dask enable parallel and distributed computing, allowing you to execute tasks concurrently on a single machine or across multiple machines.

2. Task Scheduling:
   - Both libraries provide task scheduling capabilities, enabling the definition of task dependencies and the management of task execution.

3. Dynamic Task Graphs:
   - Both Parsl and Dask use dynamic task graphs to represent the workflow of tasks and their dependencies. This allows for flexible execution and the ability to handle complex workflows.

4. Python Integration:
   - Both libraries are designed to work seamlessly with Python, allowing users to write parallelized code in a familiar language.

5. Scalability:
   - Both libraries are designed to scale from single-node to multi-node environments, making them suitable for a wide range of applications from small-scale to large-scale data processing.

**Differences**:

1. Primary Focus:
   - **Parsl**: Primarily focuses on scientific workflows and high-performance computing (HPC) applications. It is designed to integrate with existing HPC systems and provides features that are particularly useful for scientific research and large-scale simulations.
   - **Dask**: More general-purpose and focuses on parallel computing for data analysis. It is commonly used in data science and machine learning workflows and integrates well with the PyData ecosystem, including libraries like NumPy, pandas, and scikit-learn.

2. Ease of Use:
   - **Dask**: Often considered easier to use for data analysis tasks due to its high-level API that mimics the pandas and NumPy APIs, making it straightforward to parallelize existing code.
   - **Parsl**: May require more effort to set up, especially for users unfamiliar with HPC environments. However, it provides more control and customization for complex workflows.

3. API and Abstractions:
   - **Dask**: Provides high-level collections like Dask Arrays, Dask DataFrames, and Dask Bags, which are designed to parallelize operations on large datasets.
   - **Parsl**: Provides a more flexible API for defining tasks and workflows, allowing users to specify the execution environment, resource requirements, and other parameters for each task.

4. Integration with HPC Systems:
   - **Parsl**: Has strong integration with HPC systems and job schedulers like SLURM, PBS, and HTCondor, making it well-suited for running large-scale scientific workflows on supercomputers.
   - **Dask**: While it can be used on HPC systems, it is more commonly used in cloud environments or on local clusters for data-intensive tasks.

5. Fault Tolerance:
   - **Dask**: Includes built-in fault tolerance features that allow for the recovery of computations in case of worker failures.
   - **Parsl**: Also provides fault tolerance mechanisms, but they are often tailored towards the needs of scientific workflows and HPC environments.

Common User Questions and Solutions
-----------------------------------

**How do I install Parsl?**

Parsl can be installed using pip::

    pip install parsl

If you're using the Anaconda distribution of Python, you can also install Parsl using conda::

    conda install -c conda-forge parsl

**How do I configure Parsl to run on my system?**

Parsl requires a configuration file that specifies the resources (e.g., local threads, clusters, clouds) that it will use to execute tasks. You can find example configurations for different environments in the Parsl documentation.

**How do I write a Parsl app?**

A Parsl app is a Python function that you decorate with `@python_app` or `@bash_app`. The function can take any Python object as input and return any Python object as output, as long as the objects can be serialized using pickle or dill.

**How do I run a Parsl script?**

Once you have written a Parsl script and created a configuration file, you can run the script like any other Python script. Parsl will automatically execute the tasks in your script in parallel, according to the configuration you have specified.

**How do I handle errors in Parsl?**

Parsl provides several mechanisms for handling errors, including retries and lazy fail. You can also implement custom error handlers to customize the behavior of Parsl in case of errors.

**How can I debug a Parsl script?**

Parsl interfaces with the Python logger and automatically logs Parsl-related messages to a runinfo directory. The runinfo directory will be created in the folder from which you run the Parsl script and it will contain a series of subfolders for each time you run the code. Your latest run will be the largest number.

Alternatively, you can configure the file logger to write to an output file::

    import parsl
    # Emit log lines to the screen
    parsl.set_stream_logger()
    # Write log to file, specify level of detail for logs
    parsl.set_file_logger(FILENAME, level=logging.DEBUG)

.. note::
   Parsl's logging will not capture STDOUT/STDERR from the apps themselves. Follow instructions below for application logs.

**How can I view outputs and errors from apps?**

Parsl apps include keyword arguments for capturing stderr and stdout in files::

    @bash_app
    def hello(msg, stdout=None):
        return 'echo {}'.format(msg)

    # When hello() runs the STDOUT will be written to 'hello.txt'
    hello('Hello world', stdout='hello.txt')

**How do I specify where apps should be run?**

Parsl's multi-executor support allows you to define the executor (including local threads) on which an App should be executed. For example::

    @python_app(executors=['SuperComputer1'])
    def BigSimulation(...):
        ...

    @python_app(executors=['GPUMachine'])
    def Visualize (...):
        ...

**Workers do not connect back to Parsl**

If you are running via ssh to a remote system from your local machine, or from the login node of a cluster/supercomputer, it is necessary to have a public IP to which the workers can connect back. While our remote execution systems can identify the IP address automatically in certain cases, it is safer to specify the address explicitly. Parsl provides a few heuristic based address resolution methods that could be useful, however with complex networks some trial and error might be necessary to find the right address or network interface to use.

For `parsl.executors.HighThroughputExecutor` the address is specified in the Config as shown below::

    # THIS IS A CONFIG FRAGMENT FOR ILLUSTRATION
    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor
    from parsl.addresses import address_by_route, address_by_query, address_by_hostname

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='ALCF_theta_local',
                address='<AA.BB.CC.DD>'  # specify public ip here
                # address=address_by_route() # Alternatively you can try this
                # address=address_by_query() # Alternatively you can try this
                # address=address_by_hostname() # Alternatively you can try this
            )
        ],
    )

.. note::
   Another possibility that can cause workers not to connect back to Parsl is an incompatibility between the system and the pre-compiled bindings used for pyzmq. As a last resort, you can try: `pip install --upgrade --no-binary pyzmq pyzmq`, which forces re-compilation.

**Where can I get help with Parsl?**

If you need help with Parsl, you can consult the Parsl documentation, ask questions on the Parsl forum, or join the Parsl Slack workspace.
