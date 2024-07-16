Getting Started with Parsl
==========================

Installation and Setup
----------------------

System Requirements and Dependencies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Parsl is compatible with Python 3.8 or newer. It has been tested on Linux. To install Parsl, you must have Python and pip (Python's package installer) on your system.

Jump to instructions for:

* `Windows OS`_
* `Docker`_
* `MacOS`_
* `Linux`_
* `Android`_

Windows OS
^^^^^^^^^^
While Parsl is not officially supported on Windows, you can install and run it using the Windows Subsystem for Linux (WSL), which allows you to run a Linux environment directly on Windows. If you want to lead the development of Windows OS support for Parsl, please participate in this issue: `Parsl Issue 1878 <https://github.com/Parsl/parsl/issues/1878>`_.

To install Parsl in WSL, follow these steps:

1. **Enable WSL**: Open PowerShell as Administrator and run:
   ```
   dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
   ```
2. **Install a Linux distribution**: Download a Linux distribution (e.g., Ubuntu).
3. **Open the Linux terminal**: Once installed, search for "WSL" in the Start menu and select your Linux distribution.
4. **Update and upgrade packages**: In the terminal, run:
   ```
   sudo apt update && sudo apt upgrade
   ```
5. **Install Python and pip**: Run:
   ```
   sudo apt install python3 python3-pip
   ```
6. **Install Parsl**: Run:
   ```
   python3 -m pip install parsl
   ```

Docker
^^^^^^
Docker is a platform for developing, shipping, and running container applications. You can use Docker to create a portable Parsl environment that can run on any system with Docker installed.

To install Parsl in Docker, follow these steps:

1. **Install Docker**: Download and install Docker Desktop for your operating system.
2. **Pull the Parsl image**: In a terminal or command prompt, run:
   ```
   docker pull parsl/parsl
   ```
3. **Run a Parsl container**: Run:
   ```
   docker run -it parsl/parsl bash
   ```
   This will start a Bash shell inside the Parsl container.
4. **Use Parsl**: You can now run Parsl commands inside the container.

MacOS
^^^^^
Parsl can be installed on macOS using pip or conda. If you use a Mac with an M1 chip, you may need to install Parsl in a Rosetta terminal to ensure compatibility with the required libraries.

1. **Open a Rosetta terminal**: Go to /Applications/Utilities/Terminal in Finder, right-click on Terminal, and select "Duplicate."
2. **Right-click on the duplicated Terminal and select "Get Info."**
3. **In the Info window, check the "Open using Rosetta" checkbox.**

Linux
^^^^^
Parsl is well-supported on Linux and can be installed using pip or conda. You can access the terminal by searching for "terminal" in your applications menu or by pressing Ctrl+Alt+T.

Android
^^^^^^^
Parsl is not designed to run on Android devices directly. However, there are workarounds, such as using online platforms like Google Colaboratory to run Parsl scripts in a web browser environment. These platforms provide a Jupyter Notebook interface where you can write and execute Parsl code.

Installing Parsl
^^^^^^^^^^^^^^^^
You can easily install Parsl using pip:

.. code-block:: bash

   python3 -m pip install parsl

To check if it is installed correctly, run the following command in your terminal or command prompt:

.. code-block:: bash

   parsl --version

If Parsl is installed, this command will print the version number. If you get an error, double-check that Python and pip are installed correctly.

To upgrade Parsl to the latest version, use:

.. code-block:: bash

   python3 -m pip install -U parsl

If you are using the conda package manager, you can install Parsl from the conda-forge channel:

.. code-block:: bash

   conda config --add channels conda-forge
   conda install parsl

Common Errors
^^^^^^^^^^^^^
Here are some common errors you might encounter during installation and how to fix them:

* **Dependency errors**: Parsl has several dependencies, such as pyzmq, dill, and globus-sdk. If you encounter errors related to these dependencies, try installing them manually using pip. For example:
  .. code-block:: bash

     python3 -m pip install pyzmq dill globus-sdk

* **ERROR: Could not find a version that satisfies the requirement parsl**: This means that pip cannot find a compatible version of Parsl for your Python version. Make sure you are using Python 3.8 or newer.
* **ModuleNotFoundError: No module named 'parsl'**: This means that Parsl is not installed. Make sure you have followed the installation instructions correctly.
* **Permission errors**: If you get permission errors during installation, try running the pip command with sudo (Linux/macOS) or as an administrator (Windows).

If you encounter other errors, please consult the Parsl documentation or seek help from the Parsl community in the #parsl-help channel in Slack.

Basic Configuration
-------------------

Parsl separates your code (the tasks you want to run) from how it's executed (where and how those tasks run). This is done through a configuration file that tells Parsl how to use your computing resources.

A simple configuration for running Parsl on your local machine might look like this:

.. code-block:: python

   from parsl.config import Config
   from parsl.executors import ThreadPoolExecutor

   config = Config(
       executors=[ThreadPoolExecutor(max_threads=4)]
   )

This configuration tells Parsl to use your local machine's resources and run tasks using up to 4 threads in parallel.

First Steps
-----------

Writing a Parsl Script
^^^^^^^^^^^^^^^^^^^^^^
A Parsl script is a Python script that defines the tasks you want to run in parallel and how they depend on each other.

Here's a simple example:

.. code-block:: python

   !pip install parsl
   import parsl
   from parsl.config import Config
   from parsl.executors import HighThroughputExecutor

   # Configure Parsl (Local Threads)
   config = Config(executors=[HighThroughputExecutor(max_workers=4)]) # Use 4 threads 
   parsl.load(config)

.. code-block:: python

   import parsl
   from parsl import python_app

   @python_app
   def my_task(x):
       return x * 2

   results = []
   for i in range(10):
       results.append(my_task(i))

   for result in results:
       print(result.result())

These scripts define a task called `my_task` that doubles a number. Run the first and then the second (top to bottom). It then creates 10 instances of this task, each with a different input, and runs them in parallel. Finally, it prints the results as they become available. To check if this script worked, you should see the numbers 0 through 18 printed to your console, although not necessarily in order.

Parsl Script Basic Workflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Import Parsl**: The script begins by importing the Parsl library, which provides the necessary tools and functions for parallel execution.
* **Load Configuration**: A configuration object is loaded, specifying the resources (e.g., local threads, clusters, clouds) that Parsl will use to execute tasks. This step is crucial as it tailors Parsl's behavior to the specific computing environment.
* **Define Apps**: Python functions are decorated with special tags (@python_app or @bash_app) to indicate that they can be run in parallel as independent tasks.
* **Call Apps**: The decorated functions (apps) are invoked, creating futures. Futures are placeholders for the results of these parallel tasks, allowing the script to continue without waiting for each task to finish.
* **DataFlowKernel (DFK)**: The DFK, the core of Parsl, takes over. It manages the execution of tasks, ensuring they run when their dependencies (e.g., input data) are ready and resources are available.
* **Task Execution**: The DFK sends tasks to executors, which are responsible for running the tasks on the specified resources (e.g., different cores or nodes).
* **Get Results**: Once tasks are completed, the .result() method is used to retrieve the results from the futures. The script can then use these results for further processing or analysis.
* **End**: The script concludes after all tasks have been executed and their results have been retrieved.

To run a Parsl script, you first need to load the configuration:

.. code-block:: python

   parsl.load(config)

This tells Parsl how to execute the tasks in your script. Once the configuration is loaded, you can run your script like any other Python script.

Practical Tutorial: Hello World with Parsl
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's look at a simplified example:

.. code-block:: python

   import parsl
   from parsl import python_app

   @python_app
   def hello():
       return "Hello Frodo!"

   result = hello().result()
   print(result)  # Output: Hello, Frodo!

This script defines a Parsl app called `hello` that takes a name and returns a greeting. It then runs the app with the input "Frodo" and prints the result. If this script worked, you should see "Hello, Frodo!" printed to your console.

Getting Started Tutorial
^^^^^^^^^^^^^^^^^^^^^^^^
The best way to learn Parsl is by doing. Let's revisit the "Hello World" example from above in-depth:

.. code-block:: python

   import parsl
   from parsl import python_app

   # Start Parsl
   parsl.load(config)

   # Define a Parsl app (a function that can run in parallel)
   @python_app
   def hello(name):
       return f'Hello, {name}!'

   # Run the app and get the result
   result = hello("World").result()
   print(result)  # Output: Hello, World!

This script demonstrates the core components of a Parsl program:

* **Importing Parsl**: The `import parsl` line brings in the Parsl library, giving you access to its functions and classes.
* **Loading Configuration**: The `parsl.load(config)` line initializes Parsl with your chosen configuration (this is addressed in chapter 6). This configuration specifies how Parsl will use your computing resources. In this example, we're using a simple configuration for running Parsl on your local machine.
* **Defining an App**: The `@python_app` decorator tells Parsl that the `hello` function is a Parsl app, meaning it can be run in parallel.
* **Calling the App**: The `hello("World")` line calls the app with the argument "World". This doesn't run the function immediately; instead, it returns a future, a placeholder for the result that will be available later.
* **Getting the Result**: The `.result()` method waits for the app to finish and then returns the result, which is the string "Hello, World!".
* **Printing the Result**: The last line prints the result to the console.

.. image:: images/graphic.png
   :width: 800px
   :align: center
   :alt: Graphic Style- Description of Graphic

Practical Example: Setting Up Your First Parsl Workflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To set up your first Parsl workflow, you'll need to:

1. **Install Parsl**: Follow the instructions in the "Installation and Setup" section to install Parsl on your system.
2. **Choose a configuration**: Select a configuration that matches your computing environment. Parsl provides several example configurations for different platforms, such as laptops, clusters, and clouds. You can also create custom settings.
3. **Write a Parsl script**: Define the tasks you want to run in parallel and their dependencies.
4. **Load the configuration**: Use the `parsl.load()` function to load your chosen configuration.
5. **Run your script**: Execute a Parsl script like any other Python script. Parsl will then take care of executing your tasks in parallel, managing dependencies, and moving data as needed.